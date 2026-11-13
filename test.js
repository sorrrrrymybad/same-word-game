import { spawn } from 'node:child_process';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { mkdtempSync, rmSync, readFileSync, writeFileSync, copyFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { once } from 'node:events';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { WebSocket } from 'ws';
import { createHash } from 'node:crypto';
import { isIP } from 'node:net';

// 随机端口：上一次断言失败的测试不会杀子进程，固定端口会被孤儿进程占着跑旧代码
const PORT = 3100 + Math.floor(Math.random() * 500);
const statsDirectory = mkdtempSync(join(tmpdir(), 'same-word-stats-test-'));
const serverEnv = { ...process.env, PORT, STATS_FILE: join(statsDirectory, 'stats.json'),
  DATABASE_FILE: join(statsDirectory, 'game.sqlite'), TRUST_PROXY: '0' };
let srv = spawn(process.execPath, ['server.js'], { env: serverEnv, stdio: 'ignore' });
let testDb;
let started = false;
srv.on('exit', (c) => { if (!started) { console.error(`服务端启动失败，退出码 ${c}`); process.exit(1); } });
process.on('exit', () => {
  try { srv.kill(); } catch {}
  if (testDb?.isOpen) testDb.close();
  rmSync(statsDirectory, { recursive: true, force: true });
});

for (let i = 0; i < 60 && !started; i++) {
  try { await fetch(`http://127.0.0.1:${PORT}/`); started = true; }
  catch { await new Promise((r) => setTimeout(r, 50)); }
}
if (!started) { console.error('服务端 3 秒内没有就绪'); process.exit(1); }
testDb = new DatabaseSync(serverEnv.DATABASE_FILE);
const metric = (name) => testDb.prepare('SELECT * FROM metrics WHERE name = ?').get(name);
const game = (id) => testDb.prepare('SELECT * FROM games WHERE id = ?').get(id);

/* ---------- 访问只计 HTML GET，各项更新时间独立 ---------- */
const initialVisits = metric('site_visits').value;
const initialGameUpdate = metric('started_games').updated_at;
for (const path of ['/api/stats', '/api/version', '/favicon.ico', '/missing']) {
  await fetch(`http://127.0.0.1:${PORT}${path}`);
}
await fetch(`http://127.0.0.1:${PORT}/`, { method: 'HEAD' });
assert.equal(metric('site_visits').value, initialVisits, '接口、资源、404 和 HEAD 不增加访问量');
for (const path of ['/', '/r/ABCD']) await fetch(`http://127.0.0.1:${PORT}${path}`);
assert.equal(metric('site_visits').value, initialVisits + 2);
assert.equal(metric('started_games').updated_at, initialGameUpdate, '访问不改变开局更新时间');
assert.ok(Number.isFinite(Date.parse(metric('site_visits').updated_at)));
for (const row of testDb.prepare('SELECT * FROM metrics').all()) {
  assert.match(row.since, /\+08:00$/);
  assert.match(row.updated_at, /\+08:00$/);
  assert.ok(Math.abs(Date.now() - Date.parse(row.updated_at)) < 5000, '时区转换保留真实时刻');
}
assert.equal(testDb.prepare('PRAGMA journal_mode').get().journal_mode, 'delete');
const snapshotFile = join(statsDirectory, 'snapshot.sqlite');
copyFileSync(serverEnv.DATABASE_FILE, snapshotFile);
const initialSnapshot = new DatabaseSync(snapshotFile, { readOnly: true });
assert.equal(initialSnapshot.prepare('SELECT count(*) AS count FROM metrics').get().count, 2,
  '运行中主文件本身包含表及已提交统计，不依赖 WAL');
assert.equal(initialSnapshot.prepare("SELECT value FROM metrics WHERE name = 'site_visits'").get().value, initialVisits + 2);
initialSnapshot.close();

async function getStats() {
  const response = await fetch(`http://127.0.0.1:${PORT}/api/stats`);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  return response.json();
}
assert.equal((await fetch(`http://127.0.0.1:${PORT}/stats`)).status, 404, '独立统计页已移除');
assert.deepEqual(Object.keys(await getStats()).sort(), ['online', 'startedGames'], '公开接口只返回汇总数字');
assert.equal((await getStats()).online, 0);
assert.equal((await getStats()).startedGames, 0);

function client(headers = {}) {
  const ws = new WebSocket(`ws://127.0.0.1:${PORT}`, { headers });
  const c = { ws, view: null, errors: [], waiters: [], statsPushes: 0 };
  c.left = new Promise((resolve) => { c.resolveLeft = resolve; });
  ws.on('message', (raw) => {
    const m = JSON.parse(raw);
    if (m.t === 'joined') c.id = m.playerId;
    if (m.t === 'error') c.errors.push(m.msg);
    if (m.t === 'left') c.resolveLeft(m);
    if (m.t === 'state') { c.view = m.view; c.waiters = c.waiters.filter((w) => !w(c.view)); }
    if (m.t === 'stats') c.statsPushes++;
    if (m.t === 'react') (c.reacts ||= []).push(m);
  });
  c.send = (o) => ws.send(JSON.stringify(o));
  c.ready = new Promise((r) => ws.on('open', r));
  c.until = (pred, ms = 2500) => new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error('超时，当前 phase=' + c.view?.phase + ' round=' + c.view?.round)), ms);
    const check = (v) => { if (pred(v)) { clearTimeout(t); res(v); return true; } return false; };
    if (c.view && check(c.view)) return;
    c.waiters.push(check);
  });
  c.untilError = (ms = 1500) => new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error('没有收到错误')), ms);
    const i = setInterval(() => { if (c.errors.length) { clearInterval(i); clearTimeout(t); res(c.errors.pop()); } }, 20);
  });
  return c;
}

let statsPingId = 0;
function heartbeatStats(c) {
  const id = ++statsPingId;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { c.ws.off('message', receive); reject(new Error('统计心跳超时')); }, 1500);
    function receive(raw) {
      const m = JSON.parse(raw);
      if (m.t !== 'pong' || m.id !== id) return;
      clearTimeout(timer);
      c.ws.off('message', receive);
      resolve(m.stats);
    }
    c.ws.on('message', receive);
    c.send({ t: 'ping', id });
  });
}

const A = client({ 'X-Forwarded-For': '198.51.100.9' }), B = client();
await A.ready; await B.ready;
assert.equal((await getStats()).online, 2, '首页访客也算在线');
const visitor = client();
await visitor.ready;
assert.equal((await heartbeatStats(A)).online, 3, '下一次心跳返回新增在线连接');
assert.equal((await getStats()).online, 3);
visitor.ws.close();
await once(visitor.ws, 'close');
assert.equal((await heartbeatStats(A)).online, 2, '下一次心跳返回断开后的在线数');
assert.equal(A.statsPushes, 0, '统计不主动推送');
assert.equal((await getStats()).online, 2, '关闭网页后在线数下降');

/* ---------- 心跳无需加入房间，原样返回测量标识 ---------- */
const pong = new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error('心跳响应超时')), 1500);
  A.ws.once('message', (raw) => { clearTimeout(timer); resolve(JSON.parse(raw)); });
});
A.send({ t: 'ping', id: 123.5 });
assert.deepEqual(await pong, { t: 'pong', id: 123.5, stats: { online: 2, startedGames: 0 } });

/* ---------- 建房与加入 ---------- */
A.send({ t: 'create', token: 'tokA', name: '甲', config: { max: 2, timeout: 0 } });
await A.until((v) => v.phase === 'lobby');
const code = A.view.code;
const firstGameId = A.view.gameId;
assert.equal(code.length, 4);
assert.equal(typeof firstGameId, 'string');
assert.equal(A.view.completedAt, null);
assert.equal(A.view.recordable, false);
assert.equal(A.view.hostId, A.id);
assert.equal((await getStats()).startedGames, 0, '创建房间不算开局');

B.send({ t: 'join', room: code, token: 'tokB', name: '乙' });
await B.until((v) => v.players.length === 2);

/* ---------- 砸番茄 / 点赞：只转发，校验目标和频率 ---------- */
A.send({ t: 'react', to: B.id, kind: 'tomato' });
A.send({ t: 'react', to: B.id, kind: 'like' });        // 一秒内第二次，丢掉
B.send({ t: 'react', to: B.id, kind: 'like' });        // 不能给自己
B.send({ t: 'react', to: A.id, kind: 'poop' });        // 只有番茄和点赞
B.send({ t: 'react', to: 'nobody', kind: 'like' });
await new Promise((r) => setTimeout(r, 150));
for (const c of [A, B]) assert.deepEqual(c.reacts, [{ t: 'react', from: A.id, to: B.id, kind: 'tomato' }], '全房间收到同一条反馈');
B.send({ t: 'react', to: A.id, kind: 'like' });        // 非法消息不占用频率额度
await new Promise((r) => setTimeout(r, 150));
assert.deepEqual(A.reacts.at(-1), { t: 'react', from: B.id, to: A.id, kind: 'like' });
assert.ok(!JSON.stringify(A.view).includes('tomato'), '反馈不进入房间状态');

const C = client();
await C.ready;
C.send({ t: 'join', room: code, token: 'tokC', name: '丙' });
assert.match(await C.untilError(), /房间已满/);
C.send({ t: 'leave' }); // 加入失败的连接不能退出或重置别人的房间

/* ---------- 第一局：不一致 -> 进入下一轮，投票认定 ---------- */
A.send({ t: 'start' });
await A.until((v) => v.phase === 'play');
assert.equal(A.view.round, 1);
assert.deepEqual(A.view.history, [], '开局时不应有历史');
assert.equal(A.view.endedBy, null);
assert.equal((await getStats()).startedGames, 1, '首次开局计一次');
assert.equal(metric('started_games').value, 1);
assert.equal(testDb.prepare('SELECT count(*) AS count FROM metrics').get().count, 2, '仅记录开局和访问两项');
assert.equal(game(firstGameId).player_count, 2);
assert.equal(game(firstGameId).status, 'play');
assert.deepEqual(JSON.parse(game(firstGameId).players_json).map((p) => p.ip), ['127.0.0.1', '127.0.0.1'],
  '默认不信任客户端提供的转发 IP');
assert.ok(!JSON.stringify(A.view).includes('127.0.0.1'), 'IP 不下发客户端');
assert.ok(!game(firstGameId).players_json.includes('tokA'), '私密重连令牌不存档');
assert.equal((await heartbeatStats(C)).startedGames, 1, '首页访客的心跳也返回最新开局数');

// 本轮提交的词绝不能提前泄露给对方
A.send({ t: 'submit', word: '猫' });
await new Promise((r) => setTimeout(r, 150));
assert.ok(!JSON.stringify(A.view).includes('猫'), '本轮已提交的词不能出现在下发的状态里');
assert.equal(A.view.players.find((p) => p.id === A.id).submitted, true);
assert.equal(A.view.players.find((p) => p.id === B.id).submitted, false);
assert.equal(JSON.parse(game(firstGameId).current_round_json).items[0].word, '猫', '未结算答案也持久化');

// 第二个词一到就立刻结算并进入下一轮，不一致不会自动结束本局
B.send({ t: 'submit', word: '狗' });
await A.until((v) => v.round === 2);
assert.equal(A.view.phase, 'play', '不一致应进入下一轮，而不是结束本局');
assert.equal(A.view.endedBy, null);
assert.equal(A.view.history.length, 1);
assert.equal(A.view.history[0].matched, false, '服务器判定不一致');
assert.deepEqual(A.view.history[0].items.map((i) => i.word), ['猫', '狗']);
assert.equal((await getStats()).startedGames, 1, '进入下一轮不重复计数');

// 跨轮禁词（含全角空格归一化）
A.send({ t: 'submit', word: '　猫' });
assert.match(await A.untilError(), /出现过了/);

// 投票是 toggle，且必须全员同意
A.send({ t: 'vote' });
await A.until((v) => v.players.find((p) => p.id === A.id).vote === true);
assert.equal(A.view.phase, 'play', '只有一个人投票时本局不能结束');
A.send({ t: 'vote' });
await A.until((v) => v.players.find((p) => p.id === A.id).vote === false);
A.send({ t: 'vote' });
await A.until((v) => v.players.find((p) => p.id === A.id).vote === true);
B.send({ t: 'vote' });
await A.until((v) => v.phase === 'done');
assert.equal(A.view.endedBy, 'accepted', '投票认定的结束方式');
assert.equal(A.view.history.length, 1, '认定时本局共 1 轮');
assert.equal(A.view.gameId, firstGameId);
assert.ok(Number.isFinite(A.view.completedAt), '成功认定应记录完成时间');
const firstCompletedAt = A.view.completedAt;
assert.equal(game(firstGameId).status, 'done');
assert.equal(game(firstGameId).ended_by, 'accepted');
assert.deepEqual(JSON.parse(game(firstGameId).history_json), A.view.history);
assert.ok(Number.isFinite(Date.parse(game(firstGameId).ended_at)));
for (const column of ['started_at', 'updated_at', 'ended_at']) {
  assert.match(game(firstGameId)[column], /\+08:00$/);
}

/* ---------- 第二局：完全一致 -> 服务器直接判成功 ---------- */
A.send({ t: 'restart' });
await A.until((v) => v.phase === 'done' && v.players.find((p) => p.id === A.id).ready);
assert.equal(A.view.history.length, 1, '一人准备不能清空上一局');
assert.equal(A.view.completedAt, firstCompletedAt, '准备更新不能改变对局时间');
A.send({ t: 'restart' });
await A.until((v) => !v.players.find((p) => p.id === A.id).ready);
B.send({ t: 'restart' });
await A.until((v) => v.players.find((p) => p.id === B.id).ready);
assert.equal((await getStats()).startedGames, 1, '准备未齐不计数');
A.send({ t: 'restart' });
await A.until((v) => v.phase === 'play' && v.round === 1);
assert.deepEqual(A.view.history, []);
assert.equal(A.view.endedBy, null);
assert.ok(A.view.players.every((p) => !p.ready), '新局应清空所有准备状态');
assert.notEqual(A.view.gameId, firstGameId, '每局应有独立标识，支持本地去重');
assert.equal(A.view.completedAt, null);
assert.equal(A.view.recordable, false);
assert.equal((await getStats()).startedGames, 2, '再来一局成功开启时计一次');
assert.equal((await heartbeatStats(C)).startedGames, 2, '再来一局后随心跳更新统计');
assert.equal(game(firstGameId).status, 'done', '再来一局不覆盖旧局');
const secondGameId = A.view.gameId;

// 大小写和写法不同不算一致
A.send({ t: 'submit', word: '苹果' });
B.send({ t: 'submit', word: 'Apple' });
await A.until((v) => v.round === 2);
assert.equal(A.view.history[0].matched, false, '写法不同不应当算一致');

// 第二轮写出完全相同的词
A.send({ t: 'submit', word: '月亮' });
B.send({ t: 'submit', word: '月亮' });
await A.until((v) => v.phase === 'done');
assert.equal(A.view.endedBy, 'match', '完全一致应由服务器直接判定成功');
assert.equal(A.view.round, 2, '结束后不应再开新一轮');
assert.equal(A.view.history.length, 2);
assert.equal(A.view.history[1].matched, true);
assert.ok(Number.isFinite(A.view.completedAt));
assert.equal(A.view.recordable, true);
assert.equal(game(secondGameId).ended_by, 'match');
assert.deepEqual(JSON.parse(game(secondGameId).history_json), A.view.history);
copyFileSync(serverEnv.DATABASE_FILE, snapshotFile);
const completedSnapshot = new DatabaseSync(snapshotFile, { readOnly: true });
assert.deepEqual(JSON.parse(completedSnapshot.prepare('SELECT history_json FROM games WHERE id = ?').get(secondGameId).history_json),
  A.view.history, '已完成对局直接写入主文件');
completedSnapshot.close();

// 结束后不能再提交
A.send({ t: 'submit', word: '太阳' });
A.send({ t: 'vote' });
await new Promise((r) => setTimeout(r, 150));
assert.equal(A.view.history.length, 2, '结束后不应产生新轮次或新票');

/* ---------- 主动退出与解散 ---------- */
A.send({ t: 'restart' });
await A.until((v) => v.players.find((p) => p.id === A.id).ready);
B.send({ t: 'leave' });
assert.equal((await B.left).dissolved, undefined, '普通玩家退出不解散房间');
await A.until((v) => v.phase === 'lobby' && v.players.length === 1);
assert.equal(A.view.hostId, A.id);
assert.equal(A.view.players[0].ready, false);
assert.equal(A.view.players[0].submitted, false);
assert.deepEqual(A.view.history, []);
assert.equal(A.view.round, 0);
assert.equal(A.view.endedBy, null);
A.send({ t: 'start' });
assert.match(await A.untilError(), /至少需要两个人/);
C.send({ t: 'join', room: code, token: 'tokC', name: '丙' });
await A.until((v) => v.players.length === 2);
assert.equal(A.view.players[1].id, C.id, '退出后的空位应可以重新加入');
A.send({ t: 'leave' });
assert.equal((await A.left).dissolved, true);
assert.equal((await C.left).dissolved, true, '房主解散应通知其他玩家返回主页');
C.send({ t: 'restart' });
C.send({ t: 'join', room: code, token: 'tokC' });
assert.match(await C.untilError(), /房间不存在/);

/* ---------- 仅在线玩家参与准备，断线取消准备且可重连 ---------- */
const D = client(), E = client(), F = client();
await Promise.all([D.ready, E.ready, F.ready]);
D.send({ t: 'create', token: 'tokD', config: { max: 3, timeout: 30 } });
await D.until((v) => v.phase === 'lobby');
const anotherCode = D.view.code;
E.send({ t: 'join', room: anotherCode, token: 'tokE' });
F.send({ t: 'join', room: anotherCode, token: 'tokF' });
await D.until((v) => v.players.length === 3);
D.send({ t: 'start' });
await D.until((v) => v.phase === 'play');
for (const c of [D, E, F]) c.send({ t: 'submit', word: '星星' });
await D.until((v) => v.phase === 'done');
D.send({ t: 'restart' });
E.send({ t: 'restart' });
await D.until((v) => v.players.filter((p) => p.ready).length === 2);
assert.equal(D.view.phase, 'done', '还有在线玩家未准备时不能开局');
F.ws.close();
await D.until((v) => v.phase === 'play' && v.round === 1);
for (const c of [D, E]) c.send({ t: 'submit', word: '云朵' });
await D.until((v) => v.phase === 'done' && v.history[0].items[0].word === '云朵');
assert.equal(D.view.history[0].items.length, 2, '离线玩家不阻塞在线玩家的结算');
assert.equal(D.view.recordable, true, '开局前已离线且未参与本局的玩家不影响保存');
const completedBeforeDisconnect = D.view.completedAt;
E.send({ t: 'restart' });
await D.until((v) => v.players.find((p) => p.id === E.id).ready);
E.ws.close();
await D.until((v) => !v.players.find((p) => p.id === E.id).connected);
assert.equal(D.view.players.find((p) => p.id === E.id).ready, false);
assert.equal(D.view.recordable, true, '成功之后断线不应作废已完成的战绩');
assert.equal(D.view.completedAt, completedBeforeDisconnect);
D.send({ t: 'restart' });
await D.until((v) => v.players.find((p) => p.id === D.id).ready);
assert.equal(D.view.phase, 'done', '只剩一人时不能自动开局');
const E2 = client();
await E2.ready;
E2.send({ t: 'join', room: anotherCode, token: 'tokE' });
await E2.until((v) => v.players.find((p) => p.id === E.id).connected);
assert.equal(E2.view.players.find((p) => p.id === E.id).ready, false);
E2.send({ t: 'restart' });
await D.until((v) => v.phase === 'play' && v.history.length === 0);
E2.ws.close();
await D.until((v) => !v.players.find((p) => p.id === E.id).connected);
assert.equal(D.view.recordable, false, '未结算前不能保存');
const E3 = client();
await E3.ready;
E3.send({ t: 'join', room: anotherCode, token: 'tokE', resume: true });
await E3.until((v) => v.players.find((p) => p.id === E.id).connected);
for (const c of [D, E3]) c.send({ t: 'submit', word: '太阳' });
await D.until((v) => v.phase === 'done');
assert.equal(D.view.endedBy, 'match');
assert.equal(D.view.recordable, true, '重连后全员完成可以保存战绩');
D.send({ t: 'leave' });
assert.equal((await E3.left).dissolved, true, '有计时器的房间也应能正常解散');

/* ---------- 切换浏览器后重连：全员已提交须立即补结算 ---------- */
for (const timeout of [0, 30]) {
  for (const matched of [true, false]) {
    for (const bothOffline of [false, true]) {
      const host = client(), guest = client();
      await Promise.all([host.ready, guest.ready]);
      host.send({ t: 'create', token: 'switch-host', config: { max: 2, timeout } });
      await host.until((v) => v.phase === 'lobby');
      const room = host.view.code;
      guest.send({ t: 'join', room, token: 'switch-guest' });
      await host.until((v) => v.players.length === 2);
      host.send({ t: 'start' });
      await guest.until((v) => v.phase === 'play');
      host.send({ t: 'submit', word: '星星' });
      await guest.until((v) => v.players[0].submitted);
      host.ws.close();
      await guest.until((v) => !v.players[0].connected);
      guest.send({ t: 'submit', word: matched ? '星星' : '月亮' });
      await guest.until((v) => v.players.every((p) => p.submitted));
      assert.equal(guest.view.history.length, 0, '仅一人在线时保留答案，等待重连');
      if (bothOffline) {
        guest.ws.close();
        await once(guest.ws, 'close');
      }
      const returnedHost = client();
      await returnedHost.ready;
      returnedHost.send({ t: 'join', room, token: 'switch-host', resume: true });
      let returnedGuest = guest;
      if (bothOffline) {
        await returnedHost.until((v) => v.players.filter((p) => p.connected).length === 1);
        assert.equal(returnedHost.view.history.length, 0, '首位重连时不能单人结算');
        returnedGuest = client();
        await returnedGuest.ready;
        returnedGuest.send({ t: 'join', room, token: 'switch-guest', resume: true });
      }
      for (const c of [returnedHost, returnedGuest]) {
        await c.until((v) => v.history.length === 1);
        assert.equal(c.view.phase, matched ? 'done' : 'play');
        assert.equal(c.view.round, matched ? 1 : 2);
        assert.equal(c.view.history[0].matched, matched);
        assert.deepEqual(c.view.history[0].items.map((p) => p.word), ['星星', matched ? '星星' : '月亮']);
      }
      assert.equal(returnedHost.view.recordable, matched, '全员重连后成功仍可保存战绩');
      returnedHost.send({ t: 'join', room, token: 'switch-host', resume: true });
      await heartbeatStats(returnedHost);
      assert.equal(returnedHost.view.history.length, 1, '重复同步不能重复结算');
      returnedHost.send({ t: 'leave' });
      await returnedGuest.left;
      for (const c of [returnedHost, returnedGuest]) c.ws.close();
    }
  }
}

/* ---------- 默认热梗昵称：满房不重名、尊重自定义、重连保持 ---------- */
const nicknameHost = client();
await nicknameHost.ready;
nicknameHost.send({ t: 'create', token: 'nickname-host', name: '爱你老己', config: { max: 8 } });
await nicknameHost.until((v) => v.phase === 'lobby');
const nicknameCode = nicknameHost.view.code;
const guests = Array.from({ length: 7 }, () => client());
await Promise.all(guests.map((c) => c.ready));
guests.forEach((c, i) => c.send({ t: 'join', room: nicknameCode, token: `nickname-${i}`, name: i === 0 ? '   ' : '' }));
await nicknameHost.until((v) => v.players.length === 8);
const nicknames = nicknameHost.view.players.map((p) => p.name);
assert.equal(nicknames[0], '爱你老己', '自定义昵称应保留');
assert.equal(new Set(nicknames).size, 8, '默认昵称应避开房间内已有昵称，包括自定义昵称');
assert.ok(nicknames.every((name) => name.trim() && name.length <= 12 && !/^玩家\d+$/.test(name)));
const assignedName = nicknameHost.view.players.find((p) => p.id === guests[0].id).name;
guests[0].ws.close();
await nicknameHost.until((v) => !v.players.find((p) => p.id === guests[0].id).connected);
const reconnectGuest = client();
await reconnectGuest.ready;
reconnectGuest.send({ t: 'join', room: nicknameCode, token: 'nickname-0' });
await reconnectGuest.until((v) => v.players.find((p) => p.id === guests[0].id).connected);
assert.equal(reconnectGuest.view.players.find((p) => p.id === guests[0].id).name, assignedName);
nicknameHost.send({ t: 'leave' });
await reconnectGuest.left;

/* ---------- 关闭网页后恢复房主与玩家，失效连接不能自动成为新玩家 ---------- */
const absentHost = client(), absentGuest = client();
await Promise.all([absentHost.ready, absentGuest.ready]);
absentHost.send({ t: 'create', token: 'absent-host', name: '原房主', config: { max: 2 } });
await absentHost.until((v) => v.phase === 'lobby');
const savedCode = absentHost.view.code, savedGameId = absentHost.view.gameId;
absentGuest.send({ t: 'join', room: savedCode, token: 'absent-guest', name: '原玩家' });
await absentHost.until((v) => v.players.length === 2);
await Promise.all([absentHost, absentGuest].map((c) => new Promise((resolve) => { c.ws.once('close', resolve); c.ws.close(); })));
const restoredHost = client(), restoredGuest = client();
await Promise.all([restoredHost.ready, restoredGuest.ready]);
restoredHost.send({ t: 'join', room: savedCode, token: 'absent-host', resume: true });
restoredGuest.send({ t: 'join', room: savedCode, token: 'absent-guest', resume: true });
await restoredHost.until((v) => v.players.every((p) => p.connected));
assert.equal(restoredHost.view.gameId, savedGameId);
assert.equal(restoredHost.view.hostId, absentHost.id);
assert.deepEqual(restoredHost.view.players.map((p) => p.name), ['原房主', '原玩家']);
restoredGuest.send({ t: 'leave' });
await restoredGuest.left;
restoredGuest.send({ t: 'join', room: savedCode, token: 'absent-guest', resume: true });
assert.match(await restoredGuest.untilError(), /连接已失效/);
restoredHost.send({ t: 'leave' });
await restoredHost.left;
restoredGuest.send({ t: 'join', room: savedCode, token: 'absent-guest', resume: true });
assert.match(await restoredGuest.untilError(), /房间不存在/);

/* ---------- 游戏中退出：玩家离开让所有其他人回等待页，房主离开解散 ---------- */
const exitHost = client(), exitGuest = client(), remainingGuest = client();
await Promise.all([exitHost.ready, exitGuest.ready, remainingGuest.ready]);
exitHost.send({ t: 'create', token: 'exit-host', config: { max: 3 } });
await exitHost.until((v) => v.phase === 'lobby');
const exitCode = exitHost.view.code;
exitGuest.send({ t: 'join', room: exitCode, token: 'exit-guest' });
remainingGuest.send({ t: 'join', room: exitCode, token: 'remaining-guest' });
await exitHost.until((v) => v.players.length === 3);
exitHost.send({ t: 'start' });
await exitHost.until((v) => v.phase === 'play');
exitHost.send({ t: 'submit', word: '月亮' });
await exitHost.until((v) => v.players[0].submitted);
exitGuest.send({ t: 'leave' });
await exitGuest.left;
await Promise.all([exitHost, remainingGuest].map((c) => c.until((v) => v.phase === 'lobby' && v.players.length === 2)));
assert.equal(remainingGuest.view.round, 0);
assert.ok(remainingGuest.view.players.every((p) => !p.submitted && !p.ready));
exitHost.send({ t: 'start' });
await remainingGuest.until((v) => v.phase === 'play');
exitHost.send({ t: 'leave' });
assert.equal((await exitHost.left).dissolved, true);
assert.equal((await remainingGuest.left).dissolved, true);

/* ---------- 多人局：重连后全员完成可保存，缺人完成不能事后补存 ---------- */
const reconnectCompletedGames = [], missingPlayerGames = [];
for (const endedBy of ['match', 'accepted']) {
  for (const reconnectBeforeResult of [false, true]) {
    const host = client(), guest = client(), third = client();
    await Promise.all([host.ready, guest.ready, third.ready]);
    host.send({ t: 'create', token: 'record-host', config: { max: 3 } });
    await host.until((v) => v.phase === 'lobby');
    const room = host.view.code;
    guest.send({ t: 'join', room, token: 'record-guest' });
    third.send({ t: 'join', room, token: 'record-third' });
    await host.until((v) => v.players.length === 3);
    host.send({ t: 'start' });
    await third.until((v) => v.phase === 'play');
    if (endedBy === 'accepted') {
      [host, guest, third].forEach((c, i) => c.send({ t: 'submit', word: ['土豆', '马铃薯', '洋芋'][i] }));
      await host.until((v) => v.round === 2);
    }
    third.ws.close();
    await host.until((v) => v.players.some((p) => !p.connected));
    let returned;
    if (reconnectBeforeResult) {
      returned = client();
      await returned.ready;
      returned.send({ t: 'join', room, token: 'record-third', resume: true });
      await returned.until((v) => v.players.every((p) => p.connected));
    }
    for (const c of [host, guest, ...(returned ? [returned] : [])]) {
      c.send(endedBy === 'match' ? { t: 'submit', word: '星星' } : { t: 'vote' });
    }
    await host.until((v) => v.phase === 'done');
    assert.equal(host.view.endedBy, endedBy);
    assert.equal(host.view.recordable, reconnectBeforeResult);
    if (returned) reconnectCompletedGames.push(host.view);
    else {
      missingPlayerGames.push(host.view);
      returned = client();
      await returned.ready;
      returned.send({ t: 'join', room, token: 'record-third', resume: true });
      await returned.until((v) => v.phase === 'done');
      assert.equal(returned.view.recordable, false, '缺人完成后重连不能改变保存资格');
      missingPlayerGames.push(returned.view);
    }
    host.send({ t: 'leave' });
    await returned.left;
    for (const c of [host, guest, returned]) c.ws.close();
  }
}

/* ---------- 消息校验、身份隔离及同一连接不能占用多个房间 ---------- */
const guardedHost = client(), guardedGuest = client(), impostor = client();
await Promise.all([guardedHost.ready, guardedGuest.ready, impostor.ready]);
for (const invalid of [null, [], true, 42, 'text']) guardedHost.send(invalid);
for (const invalid of [
  { t: 'create', token: {} }, { t: 'create', name: { toString: null } },
  { t: 'create', config: { max: {} } }, { t: 'create', config: null },
  { t: 'join', room: [] }, { t: 'submit', word: {} }, { t: 'join', resume: 'true' },
]) {
  guardedHost.send(invalid);
  assert.match(await guardedHost.untilError(), /消息格式无效/);
}
assert.ok(Number.isInteger((await heartbeatStats(guardedHost)).online), '异常消息不能导致服务退出');
guardedHost.send({ t: 'create', token: 'guard-host-secret', config: { max: 2.5, timeout: 30 } });
await guardedHost.until((v) => v.phase === 'lobby');
const guardedCode = guardedHost.view.code;
assert.equal(guardedHost.view.config.max, 2, '人数上限必须为整数');
assert.notEqual(guardedHost.id, 'guard-host-secret');
assert.equal(guardedHost.view.hostId, guardedHost.id);
guardedGuest.send({ t: 'join', room: guardedCode, token: 'guard-guest-secret' });
await guardedGuest.until((v) => v.players.length === 2);
for (const token of [guardedHost.view.hostId, guardedGuest.id]) {
  impostor.send({ t: 'join', room: guardedCode, token, resume: true });
  assert.match(await impostor.untilError(), /连接已失效/);
  impostor.send({ t: 'join', room: guardedCode, token });
  assert.match(await impostor.untilError(), /房间已满/);
}
assert.equal(guardedHost.ws.readyState, 1);
assert.equal(guardedGuest.ws.readyState, 1);
impostor.send({ t: 'create', token: 'other-room-secret' });
await impostor.until((v) => v.phase === 'lobby');
for (const request of [
  { t: 'create', token: 'new-secret' },
  { t: 'join', room: impostor.view.code, token: 'new-secret' },
  { t: 'join', room: guardedCode, token: 'new-secret' },
]) {
  guardedHost.send(request);
  assert.match(await guardedHost.untilError(), /先退出当前房间/);
  assert.equal(guardedHost.view.code, guardedCode);
}
guardedHost.send({ t: 'join', room: guardedCode, token: 'guard-host-secret', resume: true });
await heartbeatStats(guardedHost);
assert.equal(guardedHost.errors.length, 0, '同身份重复同步当前房间可以正常处理');
guardedHost.send({ t: 'start' });
await guardedGuest.until((v) => v.phase === 'play');
guardedGuest.send({ t: 'submit', word: '月亮' });
await guardedHost.until((v) => v.players.find((p) => p.id === guardedGuest.id).submitted);
guardedHost.ws.close();
await guardedGuest.until((v) => !v.players.find((p) => p.id === guardedHost.id).connected);

/* ---------- 超时期间不足两人，重连后补结算并恢复下一轮计时 ---------- */
await new Promise((resolve) => setTimeout(resolve, Math.max(0, guardedGuest.view.deadline - Date.now()) + 100));
const resumedHost = client();
await resumedHost.ready;
resumedHost.send({ t: 'join', room: guardedCode, token: 'guard-host-secret', resume: true });
await resumedHost.until((v) => v.round === 2);
assert.equal(resumedHost.id, guardedHost.id, '重连保留公开身份及房主权限');
assert.equal(resumedHost.view.hostId, guardedHost.id);
assert.ok(resumedHost.view.deadline > Date.now(), '新一轮恢复有效倒计时');
assert.deepEqual(resumedHost.view.history[0].items.map((p) => p.word), [null, '月亮']);
assert.ok(!JSON.stringify(resumedHost.view).includes('guard-host-secret'));
assert.ok(!JSON.stringify(resumedHost.view).includes('guard-guest-secret'), '状态及历史均不能泄露重连令牌');
resumedHost.send({ t: 'leave' });
await guardedGuest.left;
impostor.send({ t: 'leave' });
await impostor.left;
for (const c of [resumedHost, guardedGuest, impostor]) c.ws.close();

/* ---------- 客户端回归守卫 ---------- */
// 局域网走 http，是非安全上下文，crypto.randomUUID 与 clipboard 都不存在。
const page = await (await fetch(`http://127.0.0.1:${PORT}/`)).text();

/* ---------- 页面版本随 HTML 内容变化，更新提示由用户决定是否刷新 ---------- */
const versionResponse = await fetch(`http://127.0.0.1:${PORT}/api/version`);
assert.equal(versionResponse.headers.get('cache-control'), 'no-store');
const { version: pageVersion } = await versionResponse.json();
assert.equal(pageVersion, createHash('sha256').update(readFileSync('public/index.html')).digest('hex'));
assert.ok(page.includes(`const PAGE_VERSION = '${pageVersion}'`));
assert.ok(!page.includes('__PAGE_VERSION__'));
assert.equal((await fetch(`http://127.0.0.1:${PORT}/r/ABCD`)).headers.get('cache-control'), 'no-store');
let servedHtml = readFileSync('public/index.html', 'utf8'), httpHandler;
const serverSource = readFileSync('server.js', 'utf8');
const ipContext = { isIP, process: { env: { TRUST_PROXY: '1' } } };
vm.runInNewContext(serverSource.match(/function clientIP\([^]*?\n\}/)[0], ipContext);
const ipRequest = (remoteAddress, forwarded) => ({ socket: { remoteAddress }, headers: { 'x-forwarded-for': forwarded } });
assert.equal(ipContext.clientIP(ipRequest('203.0.113.8', '198.51.100.1')), '203.0.113.8', '公网直连不能伪造 IP');
assert.equal(ipContext.clientIP(ipRequest('::ffff:127.0.0.1', '198.51.100.1, invalid')), '127.0.0.1');
assert.equal(ipContext.clientIP(ipRequest('::1', '192.0.2.1, 2001:db8::1')), '2001:db8::1');
vm.runInNewContext(serverSource.slice(serverSource.indexOf('const server = createServer'), serverSource.indexOf('const wss =')), {
  URL, createHash, ROOT: '', join, readFile: async () => servedHtml,
  createServer: (handler) => { httpHandler = handler; },
});
async function readVersionRoute(path) {
  const response = { writeHead(status, headers) { this.status = status; this.headers = headers; }, end(body) { this.body = body; } };
  await httpHandler({ url: path }, response);
  return response;
}
assert.equal(JSON.parse((await readVersionRoute('/api/version')).body).version, pageVersion);
servedHtml += '\n<!-- updated -->';
const nextVersion = JSON.parse((await readVersionRoute('/api/version')).body).version;
assert.notEqual(nextVersion, pageVersion, '直接更新 HTML，无需重启即可获得新版本');
assert.ok((await readVersionRoute('/')).body.includes(`const PAGE_VERSION = '${nextVersion}'`));
const updateNodes = new Map();
const updates = {
  view: null, reloads: 0, result: { version: pageVersion }, ok: true,
  $: (id) => {
    if (!updateNodes.has(id)) updateNodes.set(id, { hidden: true, open: false, showModal() { this.open = true; }, close() { this.open = false; } });
    return updateNodes.get(id);
  },
  location: { reload() { updates.reloads++; } },
  fetch: async (url, options) => {
    assert.equal(url, '/api/version');
    assert.equal(options.cache, 'no-store');
    if (updates.fail) throw new Error('offline');
    return { ok: updates.ok, json: async () => updates.result };
  },
  setInterval(fn, ms) { assert.equal(ms, 30000); updates.poll = fn; },
};
vm.createContext(updates);
vm.runInContext(page.slice(page.indexOf('const PAGE_VERSION ='), page.indexOf('/* ------------------------------ 连接')), updates);
await updates.checkPageVersion();
assert.equal(updates.$('updatebanner').hidden, true);
updates.result = { version: nextVersion };
await updates.poll();
assert.equal(updates.$('updatebanner').hidden, false);
assert.equal(updates.reloads, 0, '发现更新不能自动刷新或打开弹窗');
assert.equal(updates.$('updatedialog').open, false);
updates.fail = true;
await updates.checkPageVersion();
assert.equal(updates.$('updatebanner').hidden, false, '断网保留已有更新入口');
updates.fail = false;
updates.result = { version: 'bad-version' };
await updates.checkPageVersion();
assert.equal(updates.$('updatebanner').hidden, false, '无效响应不覆盖有效状态');
updates.view = { phase: 'play' };
updates.$('updatebtn').onclick();
assert.equal(updates.$('updatedialog').open, true);
assert.equal(updates.reloads, 0);
updates.$('updatecancel').onclick();
assert.equal(updates.$('updatedialog').open, false);
assert.equal(updates.reloads, 0, '取消后继续对局');
updates.$('updatebtn').onclick();
updates.$('updateconfirm').onclick();
assert.equal(updates.reloads, 1);
updates.view = { phase: 'done' };
updates.$('updatebtn').onclick();
assert.equal(updates.reloads, 2, '对局结束后可直接主动刷新');

/* ---------- 微信输入框适配仅限 iOS，且不限制页面缩放 ---------- */
for (const [userAgent, platform, maxTouchPoints, wechatIOS] of [
  ['iPhone Mobile/15E148 MicroMessenger/8.0.65', 'iPhone', 5, true],
  ['iPad Mobile/15E148 MicroMessenger/8.0.65', 'iPad', 5, true],
  ['Macintosh Safari/605.1.15 MicroMessenger/8.0.65', 'MacIntel', 5, true],
  ['iPhone Version/18.0 Mobile/15E148 Safari/604.1', 'iPhone', 5, false],
  ['Macintosh Version/18.0 Safari/605.1.15', 'MacIntel', 0, false],
  ['Android Mobile Safari/537.36 MicroMessenger/8.0.65', 'Linux armv8l', 5, false],
  ['iPhone CriOS/130.0 Mobile/15E148 Safari/604.1', 'iPhone', 5, false],
]) {
  const root = { dataset: {} };
  vm.runInNewContext(page.match(/<script>([\s\S]*?)<\/script>/)[1], {
    navigator: { userAgent, platform, maxTouchPoints },
    document: { documentElement: root },
    localStorage: { getItem: () => null }, matchMedia: () => ({ matches: true }),
  });
  assert.equal(root.dataset.wechatIos, wechatIOS ? 'true' : undefined, userAgent);
}
assert.doesNotMatch(page, /maximum-scale|user-scalable/, '不要限制用户手动缩放页面');

for (const [file, type, signature] of [
  ['favicon.png', 'image/png', '89504e470d0a1a0a'],
  ['favicon.ico', 'image/x-icon', '00000100'],
]) {
  assert.ok(page.includes(`href="/${file}"`), '页面应引用 favicon');
  const icon = await fetch(`http://127.0.0.1:${PORT}/${file}`);
  assert.equal(icon.status, 200);
  assert.equal(icon.headers.get('content-type'), type);
  assert.ok(Buffer.from(await icon.arrayBuffer()).toString('hex').startsWith(signature));
}
// GridScan 资源必须在首页和邀请页可用，且静态路由不能暴露源码。
for (const [file, type] of [
  ['background.js', 'text/javascript; charset=utf-8'],
  ['background.css', 'text/css; charset=utf-8'],
]) {
  assert.ok(page.includes(`/assets/${file}`));
  const asset = await fetch(`http://127.0.0.1:${PORT}/assets/${file}`);
  assert.equal(asset.status, 200);
  assert.equal(asset.headers.get('content-type'), type);
  assert.ok((await asset.text()).length > 100);
}
assert.equal((await fetch(`http://127.0.0.1:${PORT}/src/background.jsx`)).status, 404);
assert.match(page, /id="grid-background" aria-hidden="true"/);
assert.match(page, /#grid-background\{[^}]*pointer-events:none/);
assert.ok((await (await fetch(`http://127.0.0.1:${PORT}/r/ABCD`)).text()).includes('/assets/background.js'));

/* ---------- 番茄保留头像飞行，全屏只飞近一颗并自动爆开、清理 ---------- */
const tomatoAsset = await fetch(`http://127.0.0.1:${PORT}/assets/tomato.png`);
assert.equal(tomatoAsset.status, 200);
assert.equal(tomatoAsset.headers.get('content-type'), 'image/png');
assert.equal(Buffer.from(await tomatoAsset.arrayBuffer()).readUInt32BE(16), 256, '本地贴图包含八个 32px 帧');
assert.match(await (await fetch(`http://127.0.0.1:${PORT}/assets/tomato-smash-LICENSE.txt`)).text(), /Copyright \(c\) 2019 Quinten Clause/);
const reactionNodes = [], reactionAnimations = [], reactionNotices = [];
function reactionNode() {
  return {
    style: { setProperty(name, value) { this[name] = value; } }, dataset: {}, children: [], isConnected: false,
    setAttribute() {}, getBoundingClientRect: () => ({ left: 20, top: 40, width: 44, height: 44 }),
    append(child) { child.isConnected = this.isConnected; this.children.push(child); },
    remove() { this.isConnected = false; },
    animate(frames, options) {
      const animation = { target: this, frames, options };
      reactionAnimations.push(animation);
      return animation;
    },
  };
}
const reactionFace = reactionNode();
reactionFace.isConnected = true;
const reactionUI = {
  view: { players: [{ id: 'sender', name: '发送者' }] }, playerId: 'receiver', currentScreen: 'play',
  innerWidth: 390, innerHeight: 844, reduceMotion: { matches: false }, CSS: { escape: (id) => id },
  $: () => ({ querySelector: () => reactionFace }), announce: (text) => reactionNotices.push(text),
  burst() {}, esc: (text) => text,
  document: {
    createElement: reactionNode,
    querySelectorAll: () => reactionNodes.filter((el) => el.isConnected && el.className.includes('react-screen')),
    body: { append(el) { el.isConnected = true; el.children.forEach((child) => { child.isConnected = true; }); reactionNodes.push(el); } },
  },
};
vm.createContext(reactionUI);
vm.runInContext(page.slice(page.indexOf('const REACTIONS ='), page.indexOf('let toastTimer')), reactionUI);
reactionUI.showReaction({ from: 'sender', to: 'receiver', kind: 'tomato' });
assert.equal(reactionNodes.length, 1, '先保留一次头像之间的飞行');
assert.equal(reactionNodes[0].className, 'react-fly tomato-sprite');
assert.equal(reactionAnimations[0].options.duration, 560);
reactionAnimations[0].onfinish();
const tomatoLayer = reactionNodes.find((el) => el.className === 'react-screen react-tomato');
assert.ok(tomatoLayer?.isConnected, '落地后接收者播放全屏番茄');
assert.equal(reactionNodes[0].isConnected, false);
assert.deepEqual(reactionNotices, ['发送者朝你扔了番茄']);
const tomatoSprite = tomatoLayer.children[0];
const approach = reactionAnimations.find((animation) => animation.target === tomatoSprite);
assert.equal(approach.options.duration, 350);
assert.ok(!approach.options.iterations, '飞近不循环');
approach.onfinish();
assert.match(tomatoSprite.style.backgroundPositionX, /^-(32|64|96|128|160|192|224)px$/, '自动切换为爆开的贴图帧');
assert.equal(tomatoLayer.children.filter((child) => child.className === 'tomato-sprite').length, 1);
assert.equal(tomatoLayer.children.filter((child) => child.className === 'tomato-particle').length, 23);
reactionAnimations.find((animation) => animation.target === tomatoLayer).onfinish();
assert.equal(tomatoLayer.isConnected, false, '爆炸结束移除整层，不再生成下一颗');
const previousNodes = reactionNodes.length;
reactionUI.playerId = 'sender';
reactionUI.showReaction({ from: 'sender', to: 'receiver', kind: 'tomato' });
reactionAnimations.findLast((animation) => animation.target === reactionNodes.at(-1)).onfinish();
assert.equal(reactionNodes.length, previousNodes + 1, '发送者只看到头像飞行，不播放接收者的全屏效果');
reactionUI.reduceMotion.matches = true;
reactionUI.playerId = 'receiver';
const previousAnimations = reactionAnimations.length;
reactionUI.showReaction({ from: 'sender', to: 'receiver', kind: 'tomato' });
assert.equal(reactionAnimations.length, previousAnimations + 1, '减少动态效果时只淡出番茄汁，无飞行、震屏或粒子');
reactionAnimations.at(-1).onfinish();
for (let i = 0; i < 4; i++) reactionUI.throwTomato('连发');
assert.equal(reactionUI.document.querySelectorAll().length, 3, '并发互动最多三层');
reactionUI.reduceMotion.matches = false;
reactionUI.showReaction({ from: 'sender', to: 'receiver', kind: 'like' });
const likeFly = reactionNodes.at(-1);
assert.equal(likeFly.textContent, '👍', '点赞继续使用原有飞行效果');
reactionAnimations.findLast((animation) => animation.target === likeFly).onfinish();
assert.equal(reactionNodes.at(-1).className, 'react-screen');
for (const layer of reactionUI.document.querySelectorAll()) layer.remove();
reactionUI.reduceMotion.matches = true;
reactionUI.reactScreen('👍', '第一个');
const firstReaction = reactionNodes.at(-1);
assert.equal(firstReaction.style['--react-x'], '0px', '单个互动保持居中');
reactionUI.throwTomato('第二个');
const secondReaction = reactionNodes.at(-1);
reactionUI.reactScreen('👍', '第三个');
const thirdReaction = reactionNodes.at(-1);
assert.equal(new Set(reactionUI.document.querySelectorAll().map((layer) => layer.dataset.slot)).size, 3, '混合点赞和番茄也占用不同位置');
assert.ok(parseFloat(secondReaction.style['--react-x']) < 0);
assert.ok(parseFloat(secondReaction.style['--react-y']) < 0);
assert.ok(parseFloat(thirdReaction.style['--react-x']) > 0);
assert.ok(parseFloat(thirdReaction.style['--react-y']) > 0);
assert.equal(secondReaction.children[0].style.left, 195 + parseFloat(secondReaction.style['--react-x']) + 'px', '番茄本体和文字使用相同横向偏移');
assert.equal(firstReaction.style['--react-x'], '0px', '新效果不移动已出现的效果');
secondReaction.remove();
reactionUI.throwTomato('复用空位');
assert.equal(reactionNodes.at(-1).dataset.slot, secondReaction.dataset.slot, '中间效果消失后复用空位，不和现存效果重叠');
reactionUI.reactScreen('👍', '超过三层');
assert.equal(firstReaction.isConnected, false, '超过三层仍移除最早的效果');
assert.equal(new Set(reactionUI.document.querySelectorAll().map((layer) => layer.dataset.slot)).size, 3);
const gridSource = readFileSync(new URL('./src/components/GridScan.jsx', import.meta.url), 'utf8');
const roomCode = gridSource.match(/const roomSize = \(\) => \{[\s\S]*?\n    \};/)[0];
const rayCode = gridSource.match(/const castRay = \(u, v\) => \{[\s\S]*?\n    \};/)[0];
for (const [width, height, expectedAspect] of [[1280, 720, 1280 / 720], [390, 844, 390 / 844], [767, 720, 0.72], [768, 720, 768 / 720]]) {
  const { roomSize, castRay } = new Function('view', 'settingsRef', 'eye',
    `${roomCode}\n${rayCode}\nreturn { roomSize, castRay };`
  )({ width, height }, { current: { cells: 8, depth: 3 } }, { x: 0, y: 0, f: 1.25 });
  const room = roomSize();
  assert.equal(room.aspect, expectedAspect, '移动端保持竖向矩形，桌面端跟随视口比例');
  assert.equal(castRay(0.5, 0.5).z, room.roomDepth, '中心点击命中纵深尽头');
  assert.ok(Math.abs(castRay(1, 0.5).x - room.aspect / 2) < 1e-9, '边缘点击命中调整后的侧墙');
}
const src = page.match(/function randomToken\(\)[\s\S]*?\n\}/)[0];
const load = (c) => new Function('crypto', src + '\nreturn randomToken;')(c);
const fakeRand = (a) => { for (let i = 0; i < a.length; i++) a[i] = (i * 7) % 256; return a; };
assert.match(load({ getRandomValues: fakeRand })(), /^p-[0-9a-f]{32}$/, '没有 randomUUID 时应回退到 getRandomValues');
assert.equal(load({ randomUUID: () => 'uuid-xyz', getRandomValues: fakeRand })(), 'uuid-xyz', '有 randomUUID 时应该用原生的');
assert.ok(/execCommand/.test(page), '缺少复制到剪贴板的兜底');
assert.ok(/endedBy/.test(page), '客户端应区分「服务器判定」和「玩家认定」两种结束方式');
assert.ok(/系统没懂，你们懂了。/.test(page), '投票认定结束时应显示另一套文案');

/* ---------- 汇总数字滚动：增减、连续变化、千位进位及减少动态效果 ---------- */
const counterAnimations = [];
function counterNode() {
  return {
    dataset: {}, children: [], hidden: false, value: '',
    set textContent(text) { this.value = text; this.children = []; },
    get textContent() { return this.children.length ? this.children.map((child) => child.textContent).join('') : this.value; },
    setAttribute(key, value) { this[key] = value; },
    append(...children) { this.children.push(...children); },
    replaceChildren(...children) { this.children = children; this.value = ''; },
    animate(frames, options) {
      const animation = { frames, options, cancelled: false, cancel() { this.cancelled = true; } };
      counterAnimations.push(animation);
      return animation;
    },
  };
}
const counterElements = new Map(['publicstats', 'onlinecount', 'gamecount'].map((id) => [id, counterNode()]));
const counterUI = { $: (id) => counterElements.get(id), document: { createElement: counterNode }, reduceMotion: { matches: false } };
vm.createContext(counterUI);
vm.runInContext(page.match(/function renderPublicStats\([^]*?\n\}/)[0], counterUI);
counterUI.renderPublicStats({ online: 9, startedGames: 999 });
assert.equal(counterAnimations.length, 0, '首次加载不从虚构数字滚动');
counterUI.renderPublicStats({ online: 10, startedGames: 1000 });
assert.equal(counterAnimations.length, 2);
assert.equal(counterAnimations[0].options.direction, 'normal', '增加时向上滚动');
assert.equal(counterElements.get('gamecount').dataset.value, '1,000');
counterUI.renderPublicStats({ online: 10, startedGames: 1000 });
assert.equal(counterAnimations.length, 2, '相同心跳读数不重复动画');
counterUI.renderPublicStats({ online: 8, startedGames: 1000 });
assert.equal(counterAnimations[0].cancelled, true, '新读数取消旧动画');
assert.equal(counterAnimations[2].options.direction, 'reverse', '减少时向下滚动');
counterAnimations[0].onfinish();
assert.equal(counterElements.get('onlinecount').statAnimation, counterAnimations[2], '旧回调不能覆盖新读数');
counterAnimations[2].onfinish();
counterAnimations[1].onfinish();
assert.equal(counterElements.get('onlinecount').textContent, '8');
assert.equal(counterElements.get('gamecount').textContent, '1,000');
counterUI.reduceMotion.matches = true;
counterUI.renderPublicStats({ online: 7, startedGames: 1001 });
assert.equal(counterAnimations.length, 3);
assert.equal(counterElements.get('onlinecount').textContent, '7');
counterUI.reduceMotion.matches = false;
counterElements.get('publicstats').hidden = true;
counterUI.renderPublicStats({ online: 6, startedGames: 1002 });
assert.equal(counterAnimations.length, 3, '隐藏的首页统计不播放动画');
counterElements.get('publicstats').hidden = false;
delete counterElements.get('onlinecount').animate;
delete counterElements.get('gamecount').animate;
counterUI.renderPublicStats({ online: 5, startedGames: 1003 });
assert.equal(counterElements.get('gamecount').textContent, '1,003', '不支持动画时直接更新');
counterUI.renderPublicStats(null);
assert.equal(counterElements.get('onlinecount').textContent, '—');
assert.equal(counterElements.get('gamecount').textContent, '—');

/* ---------- 规则与历史弹窗从对应按钮放大，减少动态效果时直接打开 ---------- */
const popupAnimations = [];
const popupUI = { reduceMotion: { matches: false }, getComputedStyle: () => ({ transform: 'matrix(1,0,0,1,0,0)', opacity: '1' }) };
vm.createContext(popupUI);
vm.runInContext(page.match(/function animateButtonDialog\([^]*?\n\}/)[0], popupUI);
const popup = {
  scrollTop: 100,
  showModal() { this.open = true; },
  close() { this.open = false; },
  getBoundingClientRect() { return { left: 100, top: 100, width: 400, height: 600 }; },
  animate(frames) { popupAnimations.push(frames); return { cancelled: false, cancel() { this.cancelled = true; } }; },
};
for (const [left, offset] of [[100, -178], [152, -126]]) {
  popupUI.animateButtonDialog(popup, { getBoundingClientRect: () => ({ left, top: 50, width: 44, height: 44 }) });
  assert.equal(popup.open, true);
  assert.equal(popup.scrollTop, 0, '打开时回到内容顶部');
  assert.equal(popupAnimations.at(-1)[0].transform, `translate(${offset}px, -328px) scale(.1)`, '起点跟随对应入口按钮');
  assert.equal(popupAnimations.at(-1)[1].opacity, 1);
}
const popupButton = { getBoundingClientRect: () => ({ left: 152, top: 50, width: 44, height: 44 }) };
const openingAnimation = popup.popupAnimation;
popupUI.animateButtonDialog(popup, popupButton, true);
assert.equal(openingAnimation.cancelled, true, '关闭时取消未完成的展开动画');
assert.equal(popup.open, true, '缩回动画完成前保留弹窗');
assert.equal(popupAnimations.at(-1)[1].transform, 'translate(-126px, -328px) scale(.1)', '关闭缩回对应入口');
const closingAnimation = popup.popupAnimation;
openingAnimation.onfinish();
assert.equal(popup.open, true, '旧动画回调不得提前关闭弹窗');
closingAnimation.onfinish();
assert.equal(popup.open, false);
assert.equal(closingAnimation.cancelled, true, '完成后清理动画');
popupUI.animateButtonDialog(popup, popupButton, true);
assert.equal(popupAnimations.length, 3, '已关闭的弹窗不重复动画');
popupUI.reduceMotion.matches = true;
popupUI.animateButtonDialog(popup, { getBoundingClientRect: () => ({}) });
assert.equal(popupAnimations.length, 3, '减少动态效果时不播放放大动画');
popupUI.animateButtonDialog(popup, popupButton, true);
assert.equal(popup.open, false, '减少动态效果时直接关闭');
popupUI.reduceMotion.matches = false;
delete popup.animate;
popupUI.animateButtonDialog(popup, { getBoundingClientRect: () => ({}) });
assert.equal(popup.open, true, '不支持动画时仍能打开弹窗');
popupUI.animateButtonDialog(popup, popupButton, true);
assert.equal(popup.open, false, '不支持动画时仍能关闭弹窗');

/* ---------- 自动规则说明：逐次进入首页、持久化选择及手动打开 ---------- */
const rulesStorage = new Map();
const rulesNodes = { rulesdialog: { open: false }, rulesdontshow: { checked: false }, rulesbtn: {} };
const rulesUI = {
  $: id => rulesNodes[id], notices: [], opens: 0, writable: true,
  store: { get: key => rulesStorage.get(key), set: (key, value) => { if (!rulesUI.writable) return false; rulesStorage.set(key, value); return true; } },
  animateButtonDialog: dialog => { dialog.open = true; rulesUI.opens++; },
  toast(text) { rulesUI.notices.push(text); },
};
vm.createContext(rulesUI);
vm.runInContext(['showRules', 'saveRulesPreference'].map(name => page.match(new RegExp(`function ${name}\\([^]*?\n\}`))[0]).join('\n'), rulesUI);
rulesUI.showRules(true);
assert.equal(rulesUI.opens, 1, '默认自动展示规则');
rulesUI.showRules(true);
assert.equal(rulesUI.opens, 1, '已打开时不重复弹出');
rulesNodes.rulesdialog.open = false;
rulesUI.showRules(true);
assert.equal(rulesUI.opens, 2, '未勾选时再次进入仍展示');
rulesNodes.rulesdontshow.checked = true;
rulesUI.saveRulesPreference();
rulesNodes.rulesdialog.open = false;
rulesUI.showRules(true);
assert.equal(rulesUI.opens, 2, '勾选后阻止自动弹出');
rulesUI.showRules();
assert.equal(rulesUI.opens, 3, '仍可以通过规按钮手动打开');
assert.equal(rulesNodes.rulesdontshow.checked, true);
rulesNodes.rulesdontshow.checked = false;
rulesUI.saveRulesPreference();
assert.equal(rulesStorage.get('swg-hide-rules'), '0', '取消勾选恢复自动弹出');
rulesUI.writable = false;
rulesNodes.rulesdontshow.checked = true;
rulesUI.saveRulesPreference();
assert.equal(rulesNodes.rulesdontshow.checked, false, '保存失败还原选择并提示');
assert.equal(rulesUI.notices.length, 1);
rulesUI.writable = true;
rulesStorage.clear();
for (const id of ['home', 'lobby', 'play', 'done', 'publicstats', 'roundresults']) rulesNodes[id] = { hidden: false, replaceChildren() {} };
Object.assign(rulesUI, { currentScreen: null, lobbyPlayerIds: new Set(), stopHistoryMotion() {}, stopBurst() {} });
vm.runInContext(page.match(/function screen\(name\) \{[^]*?\n\}/)[0], rulesUI);
rulesNodes.rulesdialog.open = false;
const opensBeforeHome = rulesUI.opens;
rulesUI.screen('home');
assert.equal(rulesUI.opens, opensBeforeHome + 1, '首次首页自动展示');
rulesNodes.rulesdialog.open = false;
rulesUI.screen('home');
assert.equal(rulesUI.opens, opensBeforeHome + 1, '同一首页重复渲染不会再次打扰');
rulesUI.screen('lobby');
rulesUI.screen('home');
assert.equal(rulesUI.opens, opensBeforeHome + 2, '从房间返回首页再次展示');


/* ---------- Toast 区分错误与正常反馈，连续提示重置颜色及关闭计时 ---------- */
const toastElement = { dataset: {}, hidden: true, setAttribute(key, value) { this[key] = value; } };
const toastTimers = new Map();
const toastAnimations = [];
toastElement.animate = (frames, options) => {
  const animation = { frames, options, cancelled: false, cancel() { this.cancelled = true; } };
  toastAnimations.push(animation);
  return animation;
};
let toastTimerId = 0;
const toastContext = {
  $: () => toastElement,
  reduceMotion: { matches: false },
  setTimeout(fn) { toastTimers.set(++toastTimerId, fn); return toastTimerId; },
  clearTimeout(id) { toastTimers.delete(id); },
};
vm.createContext(toastContext);
vm.runInContext('let toastTimer, toastAnimation;\n' + page.match(/function toast\([^]*?\n\}/)[0], toastContext);
toastContext.toast('请输入一个词');
assert.equal(toastElement.dataset.type, 'error');
assert.equal(toastElement.role, 'alert');
assert.equal(toastElement.hidden, false);
assert.equal(toastAnimations.length, 1, '新提示播放飞入动画');
toastContext.toast('链接已复制', 'success');
assert.equal(toastAnimations[0].cancelled, true, '连续提示取消之前的动画');
assert.equal(toastElement.dataset.type, 'success');
assert.equal(toastElement.role, 'status');
assert.equal(toastElement.textContent, '链接已复制');
assert.equal(toastTimers.size, 1, '新提示应清除上一条提示的计时器');
toastTimers.get(toastTimerId)();
assert.equal(toastElement.hidden, false, '飞出动画结束前保留提示');
const interruptedExit = toastAnimations.at(-1);
assert.equal(interruptedExit.options.direction, 'reverse');
toastContext.toast('连接正在恢复，请稍后再试');
interruptedExit.onfinish();
assert.equal(toastElement.hidden, false, '旧飞出动画不得隐藏新提示');
assert.equal(toastElement.dataset.type, 'error', '正常提示后出现错误时应恢复报错颜色');
toastTimers.get(toastTimerId)();
toastAnimations.at(-1).onfinish();
assert.equal(toastElement.hidden, true, '飞出动画完成后隐藏提示');
toastContext.reduceMotion.matches = true;
const toastAnimationCount = toastAnimations.length;
toastContext.toast('链接已复制', 'success');
toastTimers.get(toastTimerId)();
assert.equal(toastElement.hidden, true);
assert.equal(toastAnimations.length, toastAnimationCount, '减少动态效果时跳过飞入和飞出');
toastContext.reduceMotion.matches = false;
delete toastElement.animate;
toastContext.toast('请手动复制');
toastTimers.get(toastTimerId)();
assert.equal(toastElement.hidden, true, '不支持动画时仍能显示并自动关闭提示');

/* ---------- 不带房间码重新打开时自动恢复，主动退出及失效后清除 ---------- */
const connectionStorage = new Map();
function loadConnection(fromUrl = undefined) {
  const c = {
    ws: null, view: null, fromUrl, TOKEN: 'saved-token', playerId: 'saved-player', messages: [], notices: [], renders: 0,
    connectionOnline: false, roomSynced: false, latency: null,
    heartbeatTimer: null, heartbeatTimeout: null, heartbeatSentAt: null,
    now: 100, timers: new Map(), heartbeats: [],
    performance: { now: () => c.now },
    $: (id) => {
      if (!c.elements.has(id)) c.elements.set(id, { dataset: {}, setAttribute(key, value) { this[key] = value; } });
      return c.elements.get(id);
    },
    elements: new Map(),
    location: { protocol: 'http:', host: 'localhost' },
    history: { replaceState() {} },
    localStorage: {
      getItem: (key) => connectionStorage.get(key) ?? null,
      setItem: (key, value) => connectionStorage.set(key, value),
      removeItem: (key) => connectionStorage.delete(key),
    },
    WebSocket: class {
      readyState = 1;
      send(raw) { const m = JSON.parse(raw); (m.t === 'ping' ? c.heartbeats : c.messages).push(m); }
      close() { this.readyState = 3; }
    },
    render() { c.renders++; }, toast(msg) { c.notices.push(msg); },
    renderPublicStats(data) { c.publicStats = data; },
    setTimeout(fn, delay) { const id = ++c.timerId; c.timers.set(id, fn); if (delay === 1200) c.retry = fn; return id; },
    clearTimeout(id) { c.timers.delete(id); },
    timerId: 0,
  };
  vm.createContext(c);
  vm.runInContext(page.match(/const store = \{[\s\S]*?\n\};/)[0] + '\n'
    + page.slice(page.indexOf('function send(o)'), page.indexOf('\nopen();')), c);
  c.open();
  assert.equal(c.$('connectionstatus').textContent, '同步中 · -- ms', '首次加载及刷新页面时显示同步中');
  assert.equal(c.$('submitbtn').textContent, '同步中', '首次连接时提交按钮使用同步文案');
  c.ws.onopen();
  c.receive = (m) => c.ws.onmessage({ data: JSON.stringify(m) });
  c.receive({ t: 'pong', id: c.heartbeats.at(-1).id });
  return c;
}
let connection = loadConnection();
assert.equal(connection.messages.length, 0, '首次访问主页不能自动加入房间');
connection.receive({ t: 'state', view: { code: 'ABCD' } });
assert.equal(connectionStorage.get('swg-room'), 'ABCD', '建房或加入成功后记住房间');
connection = loadConnection();
assert.deepEqual(connection.messages[0], { t: 'join', room: 'ABCD', token: 'saved-token', resume: true });
connection.receive({ t: 'state', view: { code: 'ABCD' } });
connection.ws.onclose();
connection.retry();
connection.ws.onopen();
assert.equal(connection.messages.at(-1).room, 'ABCD', '网络断线后仍恢复当前房间');
assert.equal(loadConnection('WXYZ').messages.length, 0, '其他邀请优先，不能自动回到旧房间');
assert.equal(loadConnection('ABCD').messages[0].room, 'ABCD', '同一房间的邀请可以恢复');
connection.receive({ t: 'left' });
assert.equal(connectionStorage.has('swg-room'), false);
assert.equal(loadConnection().messages.length, 0, '主动退出或解散后不再恢复');
for (const msg of ['房间不存在', '连接已失效，请重新加入房间']) {
  connectionStorage.set('swg-room', 'ABCD');
  connection = loadConnection();
  connection.receive({ t: 'error', msg });
  assert.equal(connectionStorage.has('swg-room'), false);
  assert.equal(connection.view, null);
  assert.equal(loadConnection().messages.length, 0, '失效的连接不应重复恢复');
}
connection = loadConnection();
connection.localStorage.setItem = () => { throw new Error('quota'); };
connection.receive({ t: 'state', view: { code: 'ABCD' } });
assert.equal(connection.renders, 1, '保存失败不能阻止房间显示');
assert.match(connection.notices[0], /无法保存房间连接/);

/* ---------- 心跳测延迟，失效连接禁用提交，重连等房间同步 ---------- */
connectionStorage.clear();
connection = loadConnection();
connection.receive({ t: 'joined', token: 'saved-token', playerId: 'saved-player' });
assert.equal(connection.playerId, 'saved-player', '界面使用服务端返回的公开身份');
assert.equal(connectionStorage.get('swg-token'), 'saved-token', '重连仍使用私密令牌');
assert.equal(connection.$('connectionstatus').dataset.quality, 'good');
assert.equal(connection.$('submitbtn').disabled, false);
connection.view = { code: 'ABCD', players: [{ id: 'saved-player', submitted: false }] };
connection.timers.get(connection.heartbeatTimer)();
connection.now += 250;
connection.receive({ t: 'pong', id: connection.heartbeats.at(-1).id, stats: { online: 1234, startedGames: 5678 } });
assert.equal(connection.latency, 250);
assert.equal(connection.publicStats.online, 1234);
assert.equal(connection.publicStats.startedGames, 5678, '心跳响应更新汇总数字');
assert.equal(connection.$('connectionstatus').dataset.quality, 'slow');
connection.timers.get(connection.heartbeatTimer)();
connection.now += 600;
connection.receive({ t: 'pong', id: connection.heartbeats.at(-1).id });
assert.equal(connection.$('connectionstatus').dataset.quality, 'poor');
connection.timers.get(connection.heartbeatTimer)();
const oldSocket = connection.ws;
connection.timers.get(connection.heartbeatTimeout)();
assert.equal(oldSocket.readyState, 3);
assert.equal(connection.$('submitbtn').disabled, true);
assert.equal(connection.$('submitbtn')['aria-busy'], 'true');
assert.match(connection.$('connectionstatus').textContent, /重连中/);
assert.equal(connection.$('submitbtn').textContent, '重连中', '已有连接失效后仍显示重连中');
assert.equal(connection.publicStats, null, '断线时不展示过期统计');
vm.runInContext(page.match(/function doSubmit\(\) \{[\s\S]*?\n\}/)[0], connection);
connection.$('word').value = '月亮';
const messagesBeforeOfflineSubmit = connection.messages.length;
connection.doSubmit();
assert.equal(connection.messages.length, messagesBeforeOfflineSubmit, '断线时回车也不能提交');
assert.match(connection.notices.at(-1), /连接正在恢复/);
oldSocket.onclose();
connection.retry();
connection.ws.onopen();
connection.receive({ t: 'pong', id: connection.heartbeats.at(-1).id });
assert.match(connection.$('connectionstatus').textContent, /同步中/, '重连成功后等待房间状态时显示同步中');
assert.equal(connection.$('submitbtn').disabled, true, '恢复连接后还要等待房间状态');
connection.receive({ t: 'state', view: { code: 'ABCD', players: [{ id: 'saved-player', submitted: true }] } });
assert.equal(connection.$('submitbtn').disabled, true, '重连后已提交不能再次提交');
assert.equal(connection.$('submitbtn')['aria-busy'], 'false');
connection.receive({ t: 'state', view: { code: 'ABCD', players: [{ id: 'saved-player', submitted: false }] } });
assert.equal(connection.$('submitbtn').disabled, false);

/* ---------- 礼花只在进入结算时播放，准备及离线更新不重播 ---------- */
const elements = new Map();
let bursts = 0, stops = 0;
const context = {
  currentScreen: null, showRules() {}, URLSearchParams,
  connectionOnline: true, roomSynced: true,
  view: { phase: 'done', code: 'ABCD', config: { max: 2, timeout: 0 }, round: 1, endedBy: 'match', history: [{ round: 1, items: [] }], players: [
    { id: 'me', name: '甲', connected: true, ready: false }, { id: 'other', name: '乙', connected: true, ready: false },
  ] },
  playerId: 'me', lastPhase: 'play', lastRound: 1, seenRound: null, saidResult: 0, saidRound: 0, saidVotes: 0, lobbyPlayerIds: new Set(),
  reduceMotion: { matches: false }, location: { origin: 'http://localhost' },
  $: (id) => {
    if (!elements.has(id)) {
      const classes = new Set();
      elements.set(id, {
        open: false, dataset: {}, children: [],
        classList: { toggle(c, on) { if (on) classes.add(c); else classes.delete(c); }, contains(c) { return classes.has(c); } },
        set innerHTML(html) { this.html = html; this.children = [...html.matchAll(/data-player-id=/g)].map((_, i) => context.$(`${id}-${i}-${++elementVersion}`)); },
        get innerHTML() { return this.html || ''; },
        setAttribute(name, value) { this[name] = value; }, blur() {}, replaceChildren() { this.children = []; }, close() { this.open = false; }, showModal() { this.open = true; },
      });
    }
    return elements.get(id);
  },
  setTimeout() {},
  esc: String, playerStyle: () => '', roundCard: () => '', saveSuccessfulGame() {}, showRoundResults() {}, stopHistoryMotion() { context.$('donehistory').open = false; }, announce() {}, burst() { bursts++; }, stopBurst() { stops++; },
};
let elementVersion = 0;
vm.createContext(context);
vm.runInContext(page.match(/function updateSubmitButton\(\) \{[\s\S]*?\n\}/)[0] + '\n' + page.match(/function screen\(name\) \{[\s\S]*?\n\}/)[0] + '\n' + page.match(/function render\(\) \{[\s\S]*?\n\}/)[0]
  + '\n' + page.match(/const OFFLINE_ICON = .*/)[0] + '\n' + page.match(/function avatar\([^]*?\n\}/)[0], context);
context.render();
assert.match(elements.get('restartplayers').innerHTML, /<span class="avatar" data-player-id="me"[^>]*role="img"/, '自己的头像只展示，不能点');
assert.match(elements.get('restartplayers').innerHTML, /<button class="avatar" data-player-id="other"[^>]*type="button">/, '别人的头像可以点开反馈菜单');
const avatarImage = (id, name) => context.avatar({ id, name, connected: true }, name).match(/<img src="([^"]+)" width="44" height="44" alt="">/)[1];
assert.equal(avatarImage('me', '甲'), 'https://api.dicebear.com/10.x/fun-emoji/svg?seed=%E7%94%B2', '头像使用 Fun Emoji 主题和完整昵称');
assert.equal(avatarImage('me', '同名玩家'), avatarImage('other', '同名玩家'), '同名玩家的头像不受玩家 ID 影响');
assert.notEqual(avatarImage('me', '甲一'), avatarImage('me', '甲二'), '种子不能只取昵称首字');
assert.equal(new URL(avatarImage('me', '昵称 &?=#"<>😀')).searchParams.get('seed'), '昵称 &?=#"<>😀', '中文、表情及特殊字符不能破坏图片地址');
assert.equal(new URL(avatarImage('me', '12345678901😀'.slice(0, 12))).searchParams.get('seed'), '12345678901�', '昵称截断到半个表情时仍能渲染头像');
assert.equal(elements.get('roomexitbtn').hidden, true, '结算页隐藏顶部退出入口');
const readySquare = elements.get('restartplayers').children[0];
const otherSquare = elements.get('restartplayers').children[1];
context.view.players[0].ready = true;
context.render();
assert.equal(elements.get('restartplayers').children[0], readySquare, '准备更新应复用元素以保留过渡');
assert.equal(readySquare.classList.contains('on'), true);
context.view.players[0].ready = false;
context.render();
assert.equal(readySquare.classList.contains('on'), false, '取消准备应退回空框');
context.view.players[0].ready = true;
context.view.players[1].connected = false;
context.render();
assert.equal(bursts, 1);
assert.equal(elements.get('restartbtn').textContent, '取消准备');
assert.equal(elements.get('restartcount').textContent, '搭子还没回来，再等等。');
assert.equal(elements.get('restartplayers').children.length, 2, '断线玩家的方框应保留');
assert.equal(elements.get('restartplayers').children[0], readySquare, '断线不应重建其他玩家的方框');
assert.equal(elements.get('restartplayers').children[1], otherSquare, '断线玩家应保留原位置与元素');
assert.equal(otherSquare.classList.contains('offline'), true);
assert.equal(otherSquare.classList.contains('on'), false);
assert.equal(otherSquare['aria-label'], '乙，已断线');
assert.equal(otherSquare.disabled, true, '断线玩家不能接收反馈');
context.view.players[1].connected = true;
context.render();
assert.equal(elements.get('restartplayers').children[1], otherSquare, '重连应复用原方框');
assert.equal(otherSquare.classList.contains('offline'), false);
assert.equal(otherSquare['aria-label'], '乙，等待准备');
assert.equal(otherSquare.disabled, false);
context.view.players[1].ready = true;
context.render();
assert.equal(otherSquare.classList.contains('on'), true, '重连后仍可正常就位');
context.view.players[1].connected = false;
context.render();
assert.equal(otherSquare.classList.contains('on'), false, '断线图标优先于旧的准备状态');
context.view.phase = 'lobby';
context.render();
assert.equal(elements.get('roomexitbtn').hidden, true, '等待页隐藏顶部退出入口');
assert.equal(stops, 1, '回到等待页应停止上一局礼花');
assert.equal(elements.get('startbtn').disabled, true);
assert.equal((elements.get('lobbyplayers').innerHTML.match(/class="arriving"/g) || []).length, 2);
context.render();
assert.ok(!elements.get('lobbyplayers').innerHTML.includes('class="arriving"'), '已有玩家不应重复入场');
context.view.players.push({ id: 'new', name: '丙', connected: true });
context.view.config.max = 3;
context.render();
assert.equal((elements.get('lobbyplayers').innerHTML.match(/class="arriving"/g) || []).length, 1, '只动画新加入的玩家');
context.view.phase = 'done';
context.render();
assert.equal(bursts, 2, '下一局进入结算仍应正常播放礼花');
assert.equal(elements.get('restartplayers').children.length, 3);
assert.match(elements.get('restartcount').textContent, /1 人断线/, '仍有两人在线时也应提示断线人数');
context.view.players[0].name = '新昵称';
context.render();
assert.ok(elements.get('restartplayers').innerHTML.includes(avatarImage('me', '新昵称')), '结算页重连改名后更新头像种子');
context.view = null;
elements.get('leavedialog').open = true;
context.render();
assert.equal(elements.get('leavedialog').open, false);
assert.equal(elements.get('roomexitbtn').hidden, true, '首页隐藏顶部退出入口');
assert.equal(stops, 4, '返回主页应停止礼花');

/* ---------- 所有退出入口共用二次确认，普通玩家的确认不被状态更新关闭 ---------- */
const leaveMessages = [];
context.send = (m) => leaveMessages.push(m);
vm.runInContext(page.slice(page.indexOf("$('homebtn').onclick"), page.indexOf('/* 倒计时 */')), context);
for (const isHost of [true, false]) {
  context.view = { phase: 'lobby', hostId: isHost ? 'me' : 'other', code: 'ABCD', config: { max: 2 }, players: [] };
  for (const id of ['roomexitbtn', 'lobbyhomebtn', 'homebtn']) {
    context.$(id).onclick();
    assert.equal(elements.get('leavedialog').open, true);
    assert.equal(elements.get('leavetitle').textContent, isHost ? '解散房间？' : '退出房间？');
    assert.match(elements.get('leavehint').textContent, isHost ? /退出会解散房间/ : /当前对局进度将清空/);
    assert.equal(leaveMessages.length, 0, '打开确认框不能直接退出');
    context.render();
    assert.equal(elements.get('leavedialog').open, true, '房间更新不能关闭退出确认');
    context.$('leavecancel').onclick();
    assert.equal(elements.get('leavedialog').open, false);
    assert.equal(leaveMessages.length, 0, '取消后仍留在房间');
  }
  context.$('roomexitbtn').onclick();
  context.$('leaveconfirm').onclick();
  assert.equal(leaveMessages.pop().t, 'leave', '确认后才发送退出请求');
  context.$('leavecancel').onclick();
}
context.view = null;
context.$('roomexitbtn').onclick();
assert.equal(elements.get('leavedialog').open, false, '首页不能打开退出确认');

/* ---------- 交卷提示原位切换，保留已提交的词 ---------- */
context.view = { phase: 'play', code: 'ABCD', round: 2, history: [], players: [{ id: 'me', name: '甲', submitted: false }] };
context.render();
assert.equal(elements.get('roomexitbtn').hidden, false, '游戏中显示顶部退出入口');
assert.equal(elements.get('wordhint').textContent, '提交后不可修改');
elements.get('word').value = '月亮';
context.view.players[0].submitted = true;
context.render();
assert.equal(elements.get('wordhint').textContent, '已交卷，等搭子');
assert.equal(elements.get('wordhint').classList.contains('submitted'), true);
assert.equal(elements.get('word').value, '月亮');
assert.equal(elements.get('word').disabled, true);
context.view.round++;
context.view.players[0].submitted = false;
context.render();
assert.equal(elements.get('wordhint').classList.contains('submitted'), false);
assert.equal(elements.get('word').value, '');

/* ---------- 有在线玩家发起认定时，引导其他玩家表态 ---------- */
context.view.history = [{ round: 2, matched: false, items: [{ word: '土豆' }, { word: '马铃薯' }] }];
context.view.players.push({ id: 'other', name: '乙', connected: true, vote: false });
context.render();
assert.equal(elements.get('votebtn').textContent, '第 2 轮算同频？');
context.view.players[1].vote = true;
context.render();
assert.equal(elements.get('votebtn').textContent, '有人觉得第 2 轮算同频，去表态');
context.view.players[0].vote = true;
context.render();
assert.equal(elements.get('votebtn').textContent, '已同意，等其他人表态');
context.view.players[0].vote = false;
context.view.players[1].vote = false;
context.render();
assert.equal(elements.get('votebtn').textContent, '第 2 轮算同频？', '撤回所有投票后恢复入口文案');
context.view.players[1].vote = true;
context.view.players[1].connected = false;
context.render();
assert.equal(elements.get('votebtn').textContent, '第 2 轮算同频？', '离线玩家的旧票不触发表态提示');

/* ---------- 新结果只追加新增行并淡入，重复调用、减少动效与离开页面都干净 ---------- */
const results = {
  children: [], top: 0, cap: 200,
  get scrollHeight() { return this.children.length * 100; },
  get scrollTop() { return this.top; },
  set scrollTop(value) { this.top = Math.max(0, Math.min(value, this.scrollHeight - this.cap)); },
  replaceChildren() { this.children = []; this.top = 0; },
  set innerHTML(html) { this.replaceChildren(); this.insertAdjacentHTML('beforeend', html); },
  insertAdjacentHTML(_, html) {
    for (let i = html.split('class="rnd"').length - 1; i > 0; i--) {
      const classes = new Set();
      this.children.push({ classList: { add(c) { classes.add(c); }, remove(c) { classes.delete(c); }, contains(c) { return classes.has(c); } } });
    }
  },
};
const motion = { $: () => results, seenRound: null, reduceMotion: { matches: false }, roundCard: () => '<div class="rnd"></div>' };
vm.createContext(motion);
vm.runInContext(page.match(/function showRoundResults\([^]*?\n\}/)[0], motion);
const rounds = [];
for (let count = 1; count <= 4; count++) {
  const before = results.children;
  rounds.push({ round: count });
  motion.showRoundResults(rounds);
  assert.equal(results.children.length, count, '每轮只追加一行');
  for (let i = 0; i < before.length; i++) assert.equal(results.children[i], before[i], '已有结果复用原元素，不重播动画');
  assert.ok(results.children.at(-1).classList.contains('revealing'), '只有新追加的行淡入');
  motion.showRoundResults(rounds);
  assert.equal(results.children.length, count, '相同轮次更新不能重复追加');
}
assert.equal(results.scrollTop, Math.max(0, results.children.length * 100 - results.cap), '滚动到最新结果');
motion.reduceMotion.matches = true;
rounds.push({ round: 5 });
motion.showRoundResults(rounds);
assert.equal(results.children.length, 5, '减少动态效果时也照常追加');
assert.ok(!results.children.at(-1).classList.contains('revealing'), '减少动态效果时不淡入');
motion.reduceMotion.matches = false;
motion.showRoundResults([]);
assert.equal(results.children.length, 0, '离开页面清空历史');
assert.equal(results.scrollTop, 0);
motion.showRoundResults(rounds);
assert.equal(results.children.length, 5, '回到页面重新铺满历史');
assert.ok(results.children.at(-1).classList.contains('revealing'), '重新进入后最后一轮仍淡入');
assert.ok(!/roundFrame|\.rnd\.pending/.test(page), '追加结果不该再逐帧补间或先占位隐藏');

/* ---------- 历史折叠不依赖新 CSS，支持中途反向及离开页面 ---------- */
const historyClasses = new Set(), historyAnimations = [];
const historyDetails = {
  open: false,
  classList: { contains(c) { return historyClasses.has(c); }, remove(c) { historyClasses.delete(c); }, toggle(c, on) { if (on) historyClasses.add(c); else historyClasses.delete(c); } },
};
let historyHeight = 0, historyOpacity = '1';
const historyBody = {
  scrollHeight: 160,
  getBoundingClientRect() { return { height: historyDetails.open ? historyHeight : 160 }; },
  animate(keyframes) {
    const animation = { keyframes, cancelled: false, cancel() { this.cancelled = true; historyHeight = 160; historyOpacity = '1'; } };
    historyAnimations.push(animation);
    return animation;
  },
};
const accordion = {
  historyAnimation: null, reduceMotion: { matches: false },
  $: (id) => id === 'donehistory' ? historyDetails : historyBody,
  getComputedStyle: () => ({ opacity: historyOpacity }),
};
vm.createContext(accordion);
vm.runInContext(['stopHistoryMotion', 'toggleDoneHistory'].map((name) => page.match(new RegExp(`function ${name}\\([^]*?\\n\\}`))[0]).join('\n'), accordion);
accordion.toggleDoneHistory();
assert.equal(historyAnimations[0].keyframes[0].height, '0px');
assert.equal(historyAnimations[0].keyframes[1].height, '160px');
historyHeight = 60; historyOpacity = '.4';
accordion.toggleDoneHistory();
assert.equal(historyAnimations[0].cancelled, true);
assert.equal(historyAnimations[1].keyframes[0].height, '60px', '反向应从当前高度继续');
assert.equal(historyAnimations[1].keyframes[0].opacity, '.4');
assert.equal(historyAnimations[1].keyframes[1].height, '0px');
assert.equal(historyDetails.open, true, '收起结束前不能被原生 details 直接隐藏');
historyHeight = 35;
accordion.toggleDoneHistory();
historyAnimations[1].onfinish();
assert.equal(historyDetails.open, true, '已取消的收起回调不能覆盖新的展开');
historyAnimations[2].onfinish();
assert.equal(accordion.historyAnimation, null);
assert.equal(historyClasses.size, 0);
accordion.toggleDoneHistory();
historyAnimations.at(-1).onfinish();
assert.equal(historyDetails.open, false);
accordion.toggleDoneHistory();
const interruptedHistory = historyAnimations.at(-1);
accordion.stopHistoryMotion();
interruptedHistory.onfinish();
assert.equal(historyDetails.open, false, '离开结算页后旧回调不能重新展开');
const animationCount = historyAnimations.length;
accordion.reduceMotion.matches = true;
accordion.toggleDoneHistory();
assert.equal(historyDetails.open, true);
accordion.toggleDoneHistory();
assert.equal(historyDetails.open, false);
assert.equal(historyAnimations.length, animationCount, '减少动态效果时直接切换');

/* ---------- 本地战绩：仅保存完整成功局、去重、回看及存储失败 ---------- */
const recordStorage = new Map(), recordMessages = [], recordElements = new Map();
let failRecordWrite = false;
const archive = {
  RECORDS_KEY: 'swg-records-v1', recordedGameId: null, view: null,
  localStorage: {
    getItem(key) { return recordStorage.get(key) ?? null; },
    setItem(key, value) { if (failRecordWrite) throw new Error('quota'); recordStorage.set(key, value); },
  },
  $: (id) => { if (!recordElements.has(id)) recordElements.set(id, { dataset: {}, focus() { this.focused = true; } }); return recordElements.get(id); },
  esc: (s) => String(s ?? '').replace(/[&<>\"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])),
  toast: (msg) => recordMessages.push(msg),
};
vm.createContext(archive);
vm.runInContext(page.match(/const store = \{[\s\S]*?\n\};/)[0] + '\n' + page.match(/const answerRow = .*/)[0] + '\n' +
  page.match(/const PLAYER_COLORS = \[[\s\S]*?\n\];/)[0] + '\n' +
  ['playerStyle', 'roundCard', 'readLocalRecords', 'saveSuccessfulGame', 'recordSummary', 'renderLocalRecords', 'showLocalRecord', 'deleteLocalRecord']
    .map((name) => page.match(new RegExp(`function ${name}\\([^]*?\\n\\}`))[0]).join('\n'), archive);
const completedGame = {
  phase: 'done', gameId: 'game-one', completedAt: Date.now(), endedBy: 'match', recordable: true,
  players: [{ id: 'secret-a', name: '<甲>' }, { id: 'secret-off', name: '过客' }, { id: 'secret-b', name: '乙' }],
  history: [
    { round: 1, matched: false, items: [{ id: 'secret-a', name: '<甲>', word: '<b>猫</b>' }, { id: 'secret-b', name: '乙', word: '狗' }] },
    { round: 2, matched: true, items: [{ id: 'secret-a', name: '<甲>', word: '月亮' }, { id: 'secret-b', name: '乙', word: '月亮' }] },
  ],
};
archive.saveSuccessfulGame({ ...completedGame, phase: 'play' });
archive.saveSuccessfulGame({ ...completedGame, phase: 'lobby' });
archive.saveSuccessfulGame({ ...completedGame, recordable: false });
assert.equal(recordStorage.size, 0, '未完成、退出及缺人完成的局不保存');
archive.saveSuccessfulGame(completedGame);
let storedRecords = JSON.parse(recordStorage.get(archive.RECORDS_KEY));
assert.equal(storedRecords.length, 1);
assert.equal(storedRecords[0].history.length, 2);
assert.equal(storedRecords[0].players[0].name, '<甲>');
assert.equal(storedRecords[0].completedAt, completedGame.completedAt);
assert.ok(!recordStorage.get(archive.RECORDS_KEY).includes('secret-'), '本地记录不保存玩家的身份令牌');
archive.saveSuccessfulGame(completedGame);
archive.recordedGameId = null;
archive.saveSuccessfulGame(completedGame);
assert.equal(archive.readLocalRecords().length, 1, '状态更新及页面重载不能重复记录同一局');
archive.saveSuccessfulGame({ ...completedGame, gameId: 'game-two', endedBy: 'accepted', completedAt: completedGame.completedAt + 1000 });
storedRecords = JSON.parse(recordStorage.get(archive.RECORDS_KEY));
assert.equal(storedRecords.length, 2, '新局即使词相同也应单独记录');
assert.equal(storedRecords[0].endedBy, 'accepted', '全员认定也属于成功局');
archive.renderLocalRecords();
assert.equal(recordElements.get('recordsnotice').hidden, false);
assert.match(recordElements.get('recordslist').innerHTML, /共 2 轮/);
assert.doesNotMatch(recordElements.get('recordslist').innerHTML, /完全同频|全员认定/);
assert.match(recordElements.get('recordslist').innerHTML, /&lt;甲&gt;/);
archive.showLocalRecord('game-one');
assert.equal(recordElements.get('recordsnotice').hidden, true);
assert.equal(recordElements.get('recorddetailpanel').hidden, false);
assert.equal(recordElements.get('recorddeletebtn').dataset.recordId, 'game-one');
assert.match(recordElements.get('recorddetailrounds').innerHTML, /第 1 轮/);
assert.match(recordElements.get('recorddetailrounds').innerHTML, /第 2 轮/);
assert.match(recordElements.get('recorddetailrounds').innerHTML, /&lt;b&gt;猫&lt;\/b&gt;/);
assert.match(recordElements.get('recorddetailrounds').innerHTML, /--player-fg:#8c4b23/, '脱离房间后仍保留当局颜色，不受原房间空位影响');
const savedBeforeError = recordStorage.get(archive.RECORDS_KEY);
failRecordWrite = true;
assert.equal(archive.deleteLocalRecord('game-one'), false);
assert.equal(recordStorage.get(archive.RECORDS_KEY), savedBeforeError, '删除写入失败时保留所有记录');
archive.saveSuccessfulGame({ ...completedGame, gameId: 'game-quota' });
assert.equal(recordStorage.get(archive.RECORDS_KEY), savedBeforeError, '存储失败不能删除已有战绩');
assert.match(recordMessages.at(-1), /未保存/);
failRecordWrite = false;
recordStorage.set(archive.RECORDS_KEY, 'broken json');
assert.equal(archive.deleteLocalRecord('game-one'), false);
archive.saveSuccessfulGame({ ...completedGame, gameId: 'game-corrupt' });
assert.equal(recordStorage.get(archive.RECORDS_KEY), 'broken json', '读取失败不能覆盖原始数据');
archive.renderLocalRecords();
assert.match(recordElements.get('recordsempty').textContent, /无法读取/);
recordStorage.clear();
archive.renderLocalRecords();
assert.equal(recordElements.get('recordsempty').hidden, false);
assert.match(recordElements.get('recordsempty').textContent, /还没有/);
recordStorage.set(archive.RECORDS_KEY, savedBeforeError);
assert.equal(archive.deleteLocalRecord('game-one'), true);
assert.deepEqual(JSON.parse(recordStorage.get(archive.RECORDS_KEY)).map((r) => r.id), ['game-two'], '仅删除选中的记录');
assert.equal(recordElements.get('recorddetailpanel').hidden, true, '删除后返回列表');
assert.equal(recordElements.get('recordsnotice').hidden, false);
assert.doesNotMatch(recordElements.get('recordslist').innerHTML, /game-one/);
assert.equal(archive.deleteLocalRecord('game-two'), true);
assert.equal(recordElements.get('recordsempty').hidden, false, '删除最后一条后显示空状态');

// 使用真实服务器结算状态验证浏览器落盘，而不只检查资格字段。
recordStorage.clear();
for (const game of missingPlayerGames) archive.saveSuccessfulGame(game);
assert.equal(recordStorage.size, 0, '缺人完成及事后重连到结果页都不保存');
for (const game of reconnectCompletedGames) archive.saveSuccessfulGame(game);
assert.equal(archive.readLocalRecords().length, 2, '重连后全员完成的完全一致和投票局均保存');
for (const game of reconnectCompletedGames) archive.saveSuccessfulGame(game);
assert.equal(archive.readLocalRecords().length, 2, '重连或重复结果状态不会重复保存');

/* ---------- 配色对比度守卫：改配色前先过这一关 ---------- */
const lin = (c) => { c /= 255; return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4; };
const lum = (h) => { const n = parseInt(h.slice(1), 16); return 0.2126 * lin((n >> 16) & 255) + 0.7152 * lin((n >> 8) & 255) + 0.0722 * lin(n & 255); };
const ratio = (a, b) => { const [hi, lo] = [lum(a), lum(b)].sort((m, n) => n - m); return (hi + 0.05) / (lo + 0.05); };

for (const theme of ['dark', 'light']) {
  const block = page.match(new RegExp(`:root\\[data-theme="${theme}"\\]\\{([\\s\\S]*?)\\}`))[1];
  const tok = Object.fromEntries([...block.matchAll(/--([a-z0-9-]+):(#[0-9a-fA-F]{6})/g)].map((m) => [m[1], m[2]]));
  const pairs = [
    ['fg', 'card', 4.5], ['fg', 'bg', 4.5], ['dim', 'card', 4.5], ['dim', 'card2', 4.5],
    ['on-accent', 'accent-solid', 4.5], ['ok-ink', 'ok', 4.5],
    ['ok', 'card', 4.5], ['ok', 'card2', 4.5],
    ['warn', 'card', 4.5], ['warn', 'card2', 4.5],
    ['latency-slow', 'card', 4.5],
    ['accent', 'card2', 4.5], ['accent', 'card', 3],
    ['field-line', 'card2', 3], ['field-line', 'card', 3],
    ['line-strong', 'card', 1.5],
  ];
  for (const [f, b, need] of pairs) {
    assert.ok(tok[f] && tok[b], `${theme} 主题缺少变量 ${f} 或 ${b}`);
    const r = ratio(tok[f], tok[b]);
    assert.ok(r >= need, `${theme} 主题下 ${f} 在 ${b} 上只有 ${r.toFixed(2)}:1，需要 ${need}:1`);
  }
}

/* ---------- 无障碍钩子 ---------- */
assert.ok(/prefers-reduced-motion/.test(page), '未处理减少动效偏好');
assert.ok(/:focus-visible/.test(page), '缺少键盘焦点样式');
assert.ok(/role="status"/.test(page), '缺少给屏幕阅读器的 live region');
assert.ok(/safe-area-inset/.test(page), '未处理移动端安全区');

/* ---------- 分钟在线日志与过期局存档 ---------- */
let minuteTick;
const minuteLogs = [], expiredGames = [];
const expiredRoom = { phase: 'play', touched: 0, createdAt: 0, timer: null };
const logRooms = new Map([['OLD', expiredRoom]]);
class LogDate extends Date {
  static now() { return Date.parse('2026-11-13T16:30:00Z'); }
}
vm.runInNewContext(serverSource.slice(serverSource.lastIndexOf('setInterval(() => {'), serverSource.indexOf('server.listen(PORT')), {
  Date: LogDate, console: { log: (line) => minuteLogs.push(line) }, publicStats: () => ({ online: 7 }),
  rooms: logRooms, live: () => [], IDLE_TTL: 1, ROOM_TTL: 1, clearTimeout,
  saveGame: (room, status) => expiredGames.push(status),
  setInterval(fn, ms) { assert.equal(ms, 60_000); minuteTick = fn; return { unref() {} }; },
});
minuteTick();
assert.equal(minuteLogs[0], '[online] 2026-11-14T00:30:00+08:00 count=7', '日志使用北京时间，跨日和午夜小时正确');
assert.deepEqual(expiredGames, ['aborted']);
assert.equal(logRooms.size, 0);

/* ---------- 中断局保存、重启恢复、完成局不会被覆盖 ---------- */
const interruptedHost = client(), interruptedGuest = client();
await Promise.all([interruptedHost.ready, interruptedGuest.ready]);
interruptedHost.send({ t: 'create', token: 'interrupt-host' });
await interruptedHost.until((v) => v.phase === 'lobby');
interruptedGuest.send({ t: 'join', room: interruptedHost.view.code, token: 'interrupt-guest' });
await interruptedHost.until((v) => v.players.length === 2);
interruptedHost.send({ t: 'start' });
await interruptedGuest.until((v) => v.phase === 'play');
interruptedGuest.send({ t: 'submit', word: '保留答案' });
await interruptedHost.until((v) => v.players.some((p) => p.submitted));
const interruptedId = interruptedHost.view.gameId;
const beforeRestart = await getStats();
const metricsBeforeRestart = testDb.prepare('SELECT * FROM metrics ORDER BY name').all();
assert.equal(metric('started_games').value, beforeRestart.startedGames);
assert.equal(testDb.prepare('SELECT count(*) AS count FROM games').get().count, beforeRestart.startedGames);
assert.equal(testDb.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
let stopped = once(srv, 'exit');
srv.kill();
await stopped;
writeFileSync(serverEnv.STATS_FILE, '{broken');
srv = spawn(process.execPath, ['server.js'], { env: serverEnv, stdio: 'ignore' });
async function waitForRestart() {
  for (let i = 0; i < 60; i++) {
    try { if ((await fetch(`http://127.0.0.1:${PORT}/api/stats`)).ok) return; } catch {}
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error('重启失败');
}
await waitForRestart();
assert.deepEqual(await getStats(), { ...beforeRestart, online: 0 }, '重启后累计值和统计起点保留，在线归零');
assert.deepEqual(testDb.prepare('SELECT * FROM metrics ORDER BY name').all(), metricsBeforeRestart,
  '已迁移的数据库不会重复读取旧 JSON，也不会因重启刷新更新时间');
assert.equal(game(interruptedId).status, 'interrupted');
assert.equal(JSON.parse(game(interruptedId).current_round_json).items[0].word, '保留答案');
assert.equal(game(firstGameId).status, 'done');
assert.ok(testDb.prepare("SELECT count(*) AS count FROM games WHERE status = 'aborted'").get().count > 0,
  '中途退出的对局也保留');
stopped = once(srv, 'exit');
srv.kill();
await stopped;
testDb.close();

/* ---------- 旧统计只迁移一次，可信单层代理的 IP 与事务回滚 ---------- */
serverEnv.DATABASE_FILE = join(statsDirectory, 'migration.sqlite');
serverEnv.TRUST_PROXY = '1';
const legacyStats = JSON.stringify({ startedGames: 27, since: '2026-11-01T00:00:00.000Z' });
writeFileSync(serverEnv.STATS_FILE, legacyStats);
srv = spawn(process.execPath, ['server.js'], { env: serverEnv, stdio: 'ignore' });
await waitForRestart();
testDb = new DatabaseSync(serverEnv.DATABASE_FILE);
assert.equal((await getStats()).startedGames, 27);
assert.equal(metric('started_games').since, '2026-11-01T08:00:00.000+08:00');
assert.equal(Date.parse(metric('started_games').since), Date.parse(JSON.parse(legacyStats).since));
assert.equal(metric('site_visits').value, 0);
assert.equal(readFileSync(serverEnv.STATS_FILE, 'utf8'), legacyStats, '迁移不改写原始文件');
const proxyHost = client({ 'X-Forwarded-For': '192.0.2.9, 198.51.100.7' }), proxyGuest = client();
await Promise.all([proxyHost.ready, proxyGuest.ready]);
proxyHost.send({ t: 'create', token: 'proxy-host' });
await proxyHost.until((v) => v.phase === 'lobby');
proxyGuest.send({ t: 'join', room: proxyHost.view.code, token: 'proxy-guest' });
await proxyHost.until((v) => v.players.length === 2);
proxyHost.send({ t: 'start' });
await proxyGuest.until((v) => v.phase === 'play');
assert.equal(JSON.parse(game(proxyHost.view.gameId).players_json)[0].ip, '198.51.100.7');
assert.equal((await getStats()).startedGames, 28);
proxyHost.send({ t: 'leave' });
await proxyGuest.left;
assert.equal(game(proxyHost.view.gameId).status, 'aborted');

// 在计数更新处制造故障，已插入的历史行必须随同回滚。
testDb.exec(`CREATE TRIGGER fail_start BEFORE UPDATE ON metrics WHEN OLD.name = 'started_games'
  BEGIN SELECT RAISE(ABORT, 'test write failure'); END`);
const failedHost = client(), failedGuest = client();
await Promise.all([failedHost.ready, failedGuest.ready]);
failedHost.send({ t: 'create', token: 'failed-host' });
await failedHost.until((v) => v.phase === 'lobby');
failedGuest.send({ t: 'join', room: failedHost.view.code, token: 'failed-guest' });
await failedHost.until((v) => v.players.length === 2);
stopped = once(srv, 'exit');
failedHost.send({ t: 'start' });
assert.notEqual((await stopped)[0], 0, '数据库写入失败应明确报错停止，不静默丢失统计');
assert.equal(metric('started_games').value, 28);
assert.equal(game(failedHost.view.gameId), undefined, '开局记录和总数在同一事务中');
testDb.exec('DROP TRIGGER fail_start');
testDb.close();

/* ---------- 升级时合并旧 WAL 中已提交但尚未写回主文件的数据 ---------- */
srv = spawn(process.execPath, ['--input-type=module', '-e', `
  import { DatabaseSync } from 'node:sqlite';
  const db = new DatabaseSync(process.env.DATABASE_FILE);
  db.exec("PRAGMA journal_mode = WAL; UPDATE metrics SET value = 321 WHERE name = 'site_visits';"
    + "UPDATE metrics SET since = '2026-11-13T16:30:00.123Z', updated_at = '2026-11-13T16:30:00.123Z';"
    + "UPDATE games SET started_at = '2026-12-31T20:30:00.456Z', updated_at = '2026-12-31T20:30:00.456Z', ended_at = '2026-12-31T20:30:00.456Z'");
  process.kill(process.pid, 'SIGKILL');
`], { env: serverEnv, stdio: 'ignore' });
assert.equal((await once(srv, 'exit'))[1], 'SIGKILL');
copyFileSync(serverEnv.DATABASE_FILE, snapshotFile);
const staleSnapshot = new DatabaseSync(snapshotFile, { readOnly: true });
assert.equal(staleSnapshot.prepare("SELECT value FROM metrics WHERE name = 'site_visits'").get().value, 0,
  '复现单独复制 WAL 主文件遗漏已提交数据');
staleSnapshot.close();
srv = spawn(process.execPath, ['server.js'], { env: serverEnv, stdio: 'ignore' });
await waitForRestart();
testDb = new DatabaseSync(serverEnv.DATABASE_FILE);
assert.equal(testDb.prepare('PRAGMA journal_mode').get().journal_mode, 'delete');
assert.equal(metric('site_visits').value, 321, '切换日志模式后保留 WAL 中的提交');
assert.equal(metric('started_games').value, 28);
assert.equal(game(proxyHost.view.gameId).status, 'aborted');
for (const row of testDb.prepare('SELECT * FROM metrics').all()) {
  assert.equal(row.since, '2026-11-14T00:30:00.123+08:00', '旧统计 UTC 时间转换后跨日正确');
  assert.equal(row.updated_at, row.since);
}
for (const column of ['started_at', 'updated_at', 'ended_at']) {
  assert.equal(game(proxyHost.view.gameId)[column], '2027-01-01T04:30:00.456+08:00', '旧对局时间转换后跨年且保留毫秒');
  assert.equal(Date.parse(game(proxyHost.view.gameId)[column]), Date.parse('2026-12-31T20:30:00.456Z'));
}
copyFileSync(serverEnv.DATABASE_FILE, snapshotFile);
const upgradedSnapshot = new DatabaseSync(snapshotFile, { readOnly: true });
assert.equal(upgradedSnapshot.prepare("SELECT value FROM metrics WHERE name = 'site_visits'").get().value, 321);
assert.equal(upgradedSnapshot.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
upgradedSnapshot.close();
stopped = once(srv, 'exit');
srv.kill();
await stopped;
testDb.close();

/* ---------- 坏文件不覆盖、不清零 ---------- */
serverEnv.DATABASE_FILE = join(statsDirectory, 'bad-legacy.sqlite');
writeFileSync(serverEnv.STATS_FILE, '{broken');
srv = spawn(process.execPath, ['server.js'], { env: serverEnv, stdio: 'ignore' });
assert.notEqual((await once(srv, 'exit'))[0], 0, '损坏统计文件不能悄悄清零');
assert.equal(readFileSync(serverEnv.STATS_FILE, 'utf8'), '{broken');
serverEnv.DATABASE_FILE = join(statsDirectory, 'broken.sqlite');
writeFileSync(serverEnv.DATABASE_FILE, 'broken database');
srv = spawn(process.execPath, ['server.js'], { env: serverEnv, stdio: 'ignore' });
assert.notEqual((await once(srv, 'exit'))[0], 0, '损坏数据库不能清零');
assert.equal(readFileSync(serverEnv.DATABASE_FILE, 'utf8'), 'broken database');

console.log('✅ 全部通过');
srv.kill();
process.exit(0);
