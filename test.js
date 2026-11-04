import { spawn } from 'node:child_process';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { WebSocket } from 'ws';

// 随机端口：上一次断言失败的测试不会杀子进程，固定端口会被孤儿进程占着跑旧代码
const PORT = 3100 + Math.floor(Math.random() * 500);
const srv = spawn(process.execPath, ['server.js'], { env: { ...process.env, PORT }, stdio: 'ignore' });
let started = false;
srv.on('exit', (c) => { if (!started) { console.error(`服务端启动失败，退出码 ${c}`); process.exit(1); } });
process.on('exit', () => { try { srv.kill(); } catch {} });   // 断言失败时也要收尸

for (let i = 0; i < 60 && !started; i++) {
  try { await fetch(`http://127.0.0.1:${PORT}/`); started = true; }
  catch { await new Promise((r) => setTimeout(r, 50)); }
}
if (!started) { console.error('服务端 3 秒内没有就绪'); process.exit(1); }

function client() {
  const ws = new WebSocket(`ws://127.0.0.1:${PORT}`);
  const c = { ws, view: null, errors: [], waiters: [] };
  c.left = new Promise((resolve) => { c.resolveLeft = resolve; });
  ws.on('message', (raw) => {
    const m = JSON.parse(raw);
    if (m.t === 'error') c.errors.push(m.msg);
    if (m.t === 'left') c.resolveLeft(m);
    if (m.t === 'state') { c.view = m.view; c.waiters = c.waiters.filter((w) => !w(c.view)); }
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
  c.mine = () => c.view.players.find((p) => p.id === 'tokA' || p.id === 'tokB' || p.id === 'tokC');
  return c;
}

const A = client(), B = client();
await A.ready; await B.ready;

/* ---------- 建房与加入 ---------- */
A.send({ t: 'create', token: 'tokA', name: '甲', config: { max: 2, timeout: 0 } });
await A.until((v) => v.phase === 'lobby');
const code = A.view.code;
const firstGameId = A.view.gameId;
assert.equal(code.length, 4);
assert.equal(typeof firstGameId, 'string');
assert.equal(A.view.completedAt, null);
assert.equal(A.view.interrupted, false);
assert.equal(A.view.hostId, 'tokA');

B.send({ t: 'join', room: code, token: 'tokB', name: '乙' });
await B.until((v) => v.players.length === 2);

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

// 本轮提交的词绝不能提前泄露给对方
A.send({ t: 'submit', word: '猫' });
await new Promise((r) => setTimeout(r, 150));
assert.ok(!JSON.stringify(A.view).includes('猫'), '本轮已提交的词不能出现在下发的状态里');
assert.equal(A.view.players.find((p) => p.id === 'tokA').submitted, true);
assert.equal(A.view.players.find((p) => p.id === 'tokB').submitted, false);

// 第二个词一到就立刻结算并进入下一轮，不一致不会自动结束本局
B.send({ t: 'submit', word: '狗' });
await A.until((v) => v.round === 2);
assert.equal(A.view.phase, 'play', '不一致应进入下一轮，而不是结束本局');
assert.equal(A.view.endedBy, null);
assert.equal(A.view.history.length, 1);
assert.equal(A.view.history[0].matched, false, '服务器判定不一致');
assert.deepEqual(A.view.history[0].items.map((i) => i.word), ['猫', '狗']);
assert.deepEqual(A.view.used, ['猫', '狗'], '出现过的词进入禁用列表');

// 跨轮禁词（含全角空格归一化）
A.send({ t: 'submit', word: '　猫' });
assert.match(await A.untilError(), /出现过了/);

// 投票是 toggle，且必须全员同意
A.send({ t: 'vote' });
await A.until((v) => v.players.find((p) => p.id === 'tokA').vote === true);
assert.equal(A.view.phase, 'play', '只有一个人投票时本局不能结束');
A.send({ t: 'vote' });
await A.until((v) => v.players.find((p) => p.id === 'tokA').vote === false);
A.send({ t: 'vote' });
await A.until((v) => v.players.find((p) => p.id === 'tokA').vote === true);
B.send({ t: 'vote' });
await A.until((v) => v.phase === 'done');
assert.equal(A.view.endedBy, 'accepted', '投票认定的结束方式');
assert.equal(A.view.history.length, 1, '认定时本局共 1 轮');
assert.equal(A.view.gameId, firstGameId);
assert.ok(Number.isFinite(A.view.completedAt), '成功认定应记录完成时间');
const firstCompletedAt = A.view.completedAt;

/* ---------- 第二局：完全一致 -> 服务器直接判成功 ---------- */
A.send({ t: 'restart' });
await A.until((v) => v.phase === 'done' && v.players.find((p) => p.id === 'tokA').ready);
assert.equal(A.view.history.length, 1, '一人准备不能清空上一局');
assert.equal(A.view.completedAt, firstCompletedAt, '准备更新不能改变对局时间');
A.send({ t: 'restart' });
await A.until((v) => !v.players.find((p) => p.id === 'tokA').ready);
B.send({ t: 'restart' });
await A.until((v) => v.players.find((p) => p.id === 'tokB').ready);
A.send({ t: 'restart' });
await A.until((v) => v.phase === 'play' && v.round === 1);
assert.deepEqual(A.view.history, []);
assert.deepEqual(A.view.used, []);
assert.equal(A.view.endedBy, null);
assert.ok(A.view.players.every((p) => !p.ready), '新局应清空所有准备状态');
assert.notEqual(A.view.gameId, firstGameId, '每局应有独立标识，支持本地去重');
assert.equal(A.view.completedAt, null);
assert.equal(A.view.interrupted, false);

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
assert.equal(A.view.interrupted, false);

// 结束后不能再提交
A.send({ t: 'submit', word: '太阳' });
A.send({ t: 'vote' });
await new Promise((r) => setTimeout(r, 150));
assert.equal(A.view.history.length, 2, '结束后不应产生新轮次或新票');

/* ---------- 主动退出与解散 ---------- */
A.send({ t: 'restart' });
await A.until((v) => v.players.find((p) => p.id === 'tokA').ready);
B.send({ t: 'leave' });
assert.equal((await B.left).dissolved, undefined, '普通玩家退出不解散房间');
await A.until((v) => v.phase === 'lobby' && v.players.length === 1);
assert.equal(A.view.hostId, 'tokA');
assert.equal(A.view.players[0].ready, false);
assert.equal(A.view.players[0].submitted, false);
assert.deepEqual(A.view.history, []);
assert.deepEqual(A.view.used, []);
assert.equal(A.view.round, 0);
assert.equal(A.view.endedBy, null);
A.send({ t: 'start' });
assert.match(await A.untilError(), /至少需要两个人/);
C.send({ t: 'join', room: code, token: 'tokC', name: '丙' });
await A.until((v) => v.players.length === 2);
assert.equal(A.view.players[1].id, 'tokC', '退出后的空位应可以重新加入');
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
assert.equal(D.view.interrupted, false, '开局前已离线的玩家不算中途断线');
const completedBeforeDisconnect = D.view.completedAt;
E.send({ t: 'restart' });
await D.until((v) => v.players.find((p) => p.id === 'tokE').ready);
E.ws.close();
await D.until((v) => !v.players.find((p) => p.id === 'tokE').connected);
assert.equal(D.view.players.find((p) => p.id === 'tokE').ready, false);
assert.equal(D.view.interrupted, false, '成功之后断线不应作废已完成的战绩');
assert.equal(D.view.completedAt, completedBeforeDisconnect);
D.send({ t: 'restart' });
await D.until((v) => v.players.find((p) => p.id === 'tokD').ready);
assert.equal(D.view.phase, 'done', '只剩一人时不能自动开局');
const E2 = client();
await E2.ready;
E2.send({ t: 'join', room: anotherCode, token: 'tokE' });
await E2.until((v) => v.players.find((p) => p.id === 'tokE').connected);
assert.equal(E2.view.players.find((p) => p.id === 'tokE').ready, false);
E2.send({ t: 'restart' });
await D.until((v) => v.phase === 'play' && v.history.length === 0);
E2.ws.close();
await D.until((v) => !v.players.find((p) => p.id === 'tokE').connected);
assert.equal(D.view.interrupted, true, '中途断线应标记本局不可留存');
const E3 = client();
await E3.ready;
E3.send({ t: 'join', room: anotherCode, token: 'tokE' });
await E3.until((v) => v.players.find((p) => p.id === 'tokE').connected);
for (const c of [D, E3]) c.send({ t: 'submit', word: '太阳' });
await D.until((v) => v.phase === 'done');
assert.equal(D.view.endedBy, 'match');
assert.equal(D.view.interrupted, true, '重连后完成也不能留存中断的对局');
D.send({ t: 'leave' });
assert.equal((await E3.left).dissolved, true, '有计时器的房间也应能正常解散');

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
const assignedName = nicknameHost.view.players.find((p) => p.id === 'nickname-0').name;
guests[0].ws.close();
await nicknameHost.until((v) => !v.players.find((p) => p.id === 'nickname-0').connected);
const reconnectGuest = client();
await reconnectGuest.ready;
reconnectGuest.send({ t: 'join', room: nicknameCode, token: 'nickname-0' });
await reconnectGuest.until((v) => v.players.find((p) => p.id === 'nickname-0').connected);
assert.equal(reconnectGuest.view.players.find((p) => p.id === 'nickname-0').name, assignedName);
nicknameHost.send({ t: 'leave' });
await reconnectGuest.left;

/* ---------- 客户端回归守卫 ---------- */
// 局域网走 http，是非安全上下文，crypto.randomUUID 与 clipboard 都不存在。
const page = await (await fetch(`http://127.0.0.1:${PORT}/`)).text();
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
const src = page.match(/function randomToken\(\)[\s\S]*?\n\}/)[0];
const load = (c) => new Function('crypto', src + '\nreturn randomToken;')(c);
const fakeRand = (a) => { for (let i = 0; i < a.length; i++) a[i] = (i * 7) % 256; return a; };
assert.match(load({ getRandomValues: fakeRand })(), /^p-[0-9a-f]{32}$/, '没有 randomUUID 时应回退到 getRandomValues');
assert.equal(load({ randomUUID: () => 'uuid-xyz', getRandomValues: fakeRand })(), 'uuid-xyz', '有 randomUUID 时应该用原生的');
assert.ok(/execCommand/.test(page), '缺少复制到剪贴板的兜底');
assert.ok(!/cfgtopic|"topic"|v\.topic/.test(page), '话题相关代码应已删干净');
assert.ok(/endedBy/.test(page), '客户端应区分「服务器判定」和「玩家认定」两种结束方式');
assert.ok(/系统没懂，你们懂了。/.test(page), '投票认定结束时应显示另一套文案');

/* ---------- 礼花只在进入结算时播放，准备及离线更新不重播 ---------- */
const elements = new Map();
let bursts = 0, stops = 0;
const context = {
  view: { phase: 'done', code: 'ABCD', config: { max: 2, timeout: 0 }, round: 1, endedBy: 'match', history: [{ round: 1, items: [] }], players: [
    { id: 'me', name: '甲', connected: true, ready: false }, { id: 'other', name: '乙', connected: true, ready: false },
  ] },
  TOKEN: 'me', lastPhase: 'play', lastRound: 1, seenRound: null, saidResult: 0, saidRound: 0, saidVotes: 0, lobbyPlayerIds: new Set(),
  reduceMotion: { matches: false }, location: { origin: 'http://localhost' },
  $: (id) => {
    if (!elements.has(id)) {
      const classes = new Set();
      elements.set(id, {
        open: false, dataset: {}, children: [],
        classList: { toggle(c, on) { if (on) classes.add(c); else classes.delete(c); }, contains(c) { return classes.has(c); } },
        set innerHTML(html) { this.html = html; this.children = [...html.matchAll(/class="ready-square"/g)].map((_, i) => context.$(`${id}-${i}-${++elementVersion}`)); },
        get innerHTML() { return this.html || ''; },
        setAttribute(name, value) { this[name] = value; }, blur() {}, replaceChildren() { this.children = []; }, close() { this.open = false; },
      });
    }
    return elements.get(id);
  },
  setTimeout() {},
  esc: String, playerStyle: () => '', roundCard: () => '', saveSuccessfulGame() {}, stopRoundMotion() {}, showRoundResults() {}, stopHistoryMotion() { context.$('donehistory').open = false; }, announce() {}, burst() { bursts++; }, stopBurst() { stops++; },
};
let elementVersion = 0;
vm.createContext(context);
vm.runInContext(page.match(/function screen\(name\) \{[\s\S]*?\n\}/)[0] + '\n' + page.match(/function render\(\) \{[\s\S]*?\n\}/)[0], context);
context.render();
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
context.view.players[1].connected = true;
context.render();
assert.equal(elements.get('restartplayers').children[1], otherSquare, '重连应复用原方框');
assert.equal(otherSquare.classList.contains('offline'), false);
assert.equal(otherSquare['aria-label'], '乙，等待准备');
context.view.players[1].ready = true;
context.render();
assert.equal(otherSquare.classList.contains('on'), true, '重连后仍可正常就位');
context.view.players[1].connected = false;
context.render();
assert.equal(otherSquare.classList.contains('on'), false, '断线图标优先于旧的准备状态');
context.view.phase = 'lobby';
context.render();
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
context.view = null;
elements.get('leavedialog').open = true;
context.render();
assert.equal(elements.get('leavedialog').open, false);
assert.equal(stops, 4, '返回主页应停止礼花');

/* ---------- 交卷提示原位切换，保留已提交的词 ---------- */
context.view = { phase: 'play', code: 'ABCD', round: 2, history: [], players: [{ id: 'me', name: '甲', submitted: false }] };
context.render();
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
assert.ok(!page.includes('id="playstatus"'), '交卷提示不应另占一行');

/* ---------- 新结果先腾空间，再淡入；更新与取消不遗留动画 ---------- */
const frames = new Map();
let nextFrame = 0;
const results = {
  children: [], style: { height: '' }, cap: 250, top: 0,
  getBoundingClientRect() { return { height: Math.min(this.cap, this.style.height === '' ? this.children.length * 100 : parseFloat(this.style.height)) }; },
  get scrollHeight() { return Math.max(this.children.length * 100, this.getBoundingClientRect().height); },
  get scrollTop() { return this.top; },
  set scrollTop(value) { this.top = Math.max(0, Math.min(value, this.scrollHeight - this.getBoundingClientRect().height)); },
  replaceChildren() { this.children = []; this.top = 0; },
  set innerHTML(html) { this.replaceChildren(); this.insertAdjacentHTML('beforeend', html); },
  insertAdjacentHTML(_, html) {
    for (const match of html.matchAll(/class="rnd"/g)) {
      const classes = new Set();
      this.children.push({ classList: { add(c) { classes.add(c); }, remove(c) { classes.delete(c); }, contains(c) { return classes.has(c); } } });
    }
  },
  querySelectorAll() { return this.children.filter((row) => row.classList.contains('pending')); },
};
const motion = {
  $: () => results, seenRound: null, roundFrame: 0, reduceMotion: { matches: false }, roundCard: () => '<div class="rnd"></div>',
  requestAnimationFrame(fn) { frames.set(++nextFrame, fn); return nextFrame; }, cancelAnimationFrame(id) { frames.delete(id); },
};
vm.createContext(motion);
vm.runInContext(['stopRoundMotion', 'showRoundResults'].map((name) => page.match(new RegExp(`function ${name}\\([^]*?\\n\\}`))[0]).join('\n'), motion);
const tick = (time) => { const batch = [...frames.values()]; frames.clear(); for (const fn of batch) fn(time); };
const rounds = [];
for (let count = 1; count <= 4; count++) {
  const oldHeight = results.getBoundingClientRect().height, oldScroll = results.scrollTop, previous = results.children[0];
  rounds.push({ round: count });
  motion.showRoundResults(rounds);
  const added = results.children.at(-1);
  assert.equal(results.getBoundingClientRect().height, oldHeight, '追加时先保持旧高度');
  assert.ok(added.classList.contains('pending'), '腾位置时新行应不可见');
  motion.showRoundResults(rounds);
  assert.equal(results.children.length, count, '相同轮次更新不能重复追加');
  if (previous) assert.equal(results.children[0], previous, '已有结果保留原元素');
  tick(0); tick(90);
  assert.ok(added.classList.contains('pending'), '扩高或滚动未完成不能提前淡入');
  if (oldHeight < results.cap) assert.ok(results.getBoundingClientRect().height > oldHeight, '未达上限先扩高');
  if (count >= 3) assert.ok(results.scrollTop > oldScroll, '到达或跨过高度上限后先向上滚动');
  tick(180);
  assert.equal(results.getBoundingClientRect().height, Math.min(250, count * 100));
  assert.equal(results.scrollTop, Math.max(0, count * 100 - 250));
  assert.ok(!added.classList.contains('pending') && added.classList.contains('revealing'), '位置留好后新行再淡入');
  assert.equal(results.style.height, '', '结束后恢复自适应高度');
}
rounds.push({ round: 5 });
motion.showRoundResults(rounds);
motion.stopRoundMotion();
assert.equal(frames.size, 0, '离开页面应取消未完成的动画');
assert.equal(results.querySelectorAll().length, 0, '取消后不应遗留不可见的新行');
motion.reduceMotion.matches = true;
rounds.push({ round: 6 });
motion.showRoundResults(rounds);
assert.equal(frames.size, 0, '减少动态效果时不应逐帧滚动');
assert.equal(results.scrollTop, 350);
assert.equal(results.querySelectorAll().length, 0);
assert.ok(!/keyframes fresh|\.rnd\.fresh/.test(page), '新结果不再闪背景');

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
vm.runInContext(page.match(/const PLAYER_COLORS = \[[\s\S]*?\n\];/)[0] + '\n' +
  ['playerStyle', 'roundCard', 'readLocalRecords', 'saveSuccessfulGame', 'recordSummary', 'renderLocalRecords', 'showLocalRecord', 'deleteLocalRecord']
    .map((name) => page.match(new RegExp(`function ${name}\\([^]*?\\n\\}`))[0]).join('\n'), archive);
const completedGame = {
  phase: 'done', gameId: 'game-one', completedAt: Date.now(), endedBy: 'match', interrupted: false,
  players: [{ id: 'secret-a', name: '<甲>' }, { id: 'secret-off', name: '过客' }, { id: 'secret-b', name: '乙' }],
  history: [
    { round: 1, matched: false, items: [{ id: 'secret-a', name: '<甲>', word: '<b>猫</b>' }, { id: 'secret-b', name: '乙', word: '狗' }] },
    { round: 2, matched: true, items: [{ id: 'secret-a', name: '<甲>', word: '月亮' }, { id: 'secret-b', name: '乙', word: '月亮' }] },
  ],
};
archive.saveSuccessfulGame({ ...completedGame, phase: 'play' });
archive.saveSuccessfulGame({ ...completedGame, phase: 'lobby' });
archive.saveSuccessfulGame({ ...completedGame, interrupted: true });
assert.equal(recordStorage.size, 0, '未完成、退出及中途断线的局不保存');
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

console.log('✅ 全部通过');
srv.kill();
process.exit(0);
