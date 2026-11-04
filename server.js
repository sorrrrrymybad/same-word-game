import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { networkInterfaces } from 'node:os';
import { WebSocketServer } from 'ws';

const ROOT = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 3000);

const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 去掉易混的 0 O 1 I
const CODE_LEN = 4;
const WORD_MAX = 20;
const ROOM_TTL = 6 * 60 * 60 * 1000;
const IDLE_TTL = 10 * 60 * 1000;
const DEFAULT_NAMES = [
  '不烧心选手', '规矩不能坏', '良心小摊主', '贵点但靠谱',
  '大水理论家', '吨吨大水壶', '雷霆大水', '大觉特困生',
  '胆子肥嘟嘟', '肥嘟嘟搭子', '爱你老己', '老己先猜',
  '老己明天见', '薄肌练习生', '纯过日子人', '省电不内耗',
];

const rooms = new Map();

/* -------------------------------- 工具 -------------------------------- */

const norm = (s) => s.normalize('NFKC').replace(/\s+/g, '').toLowerCase();
const live = (room) => [...room.players.values()].filter((p) => p.connected);
const hostOf = (room) => room.order.find((t) => room.players.has(t)) ?? null;

function randomName(room) {
  const used = new Set([...room.players.values()].map((p) => p.name));
  const available = DEFAULT_NAMES.filter((name) => !used.has(name));
  return available[Math.floor(Math.random() * available.length)];
}

function makeCode() {
  let c;
  do {
    c = '';
    for (let i = 0; i < CODE_LEN; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
  } while (rooms.has(c));
  return c;
}

function makeRoom(config) {
  return {
    code: makeCode(),
    config,               // { max, timeout }
    phase: 'lobby',       // lobby | play | done
    round: 0,
    deadline: null,
    timer: null,
    used: new Map(),      // 归一化词 -> 原始词，跨轮累积
    words: new Map(),     // token -> 本轮原始词
    votes: new Map(),     // token -> true，针对「最近一轮结果」的认定票
    ready: new Set(),
    history: [],          // [{ round, items: [{id,name,word|null}], matched }]
    endedBy: null,        // 'match' 服务器判定完全一致 | 'accepted' 玩家投票认定
    gameId: randomUUID(),
    completedAt: null,
    interrupted: false,
    order: [],            // token 顺序
    players: new Map(),   // token -> { token, name, ws, connected }
    createdAt: Date.now(),
    touched: Date.now(),
  };
}

// history 只装已结算的轮次，所以本轮的词不会被带出去
function view(room) {
  return {
    code: room.code,
    config: room.config,
    phase: room.phase,
    round: room.round,
    deadline: room.deadline,
    used: [...room.used.values()],
    hostId: hostOf(room),
    endedBy: room.endedBy,
    gameId: room.gameId,
    completedAt: room.completedAt,
    interrupted: room.interrupted,
    players: room.order.filter((t) => room.players.has(t)).map((t) => {
      const p = room.players.get(t);
      return {
        id: t,
        name: p.name,
        connected: p.connected,
        submitted: room.words.has(t),
        vote: room.votes.get(t) === true,
        ready: room.ready.has(t),
      };
    }),
    history: room.history,
  };
}

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const push = (room) => { const v = view(room); for (const p of room.players.values()) send(p.ws, { t: 'state', view: v }); };

/* ------------------------------ 回合结算 ------------------------------ */

function startRound(room) {
  room.round += 1;
  room.phase = 'play';
  room.words.clear();
  room.deadline = room.config.timeout ? Date.now() + room.config.timeout * 1000 : null;
  clearTimeout(room.timer);
  room.timer = null;
  if (room.config.timeout) {
    room.timer = setTimeout(() => {
      room.timer = null;
      if (room.phase === 'play') completeRound(room);
    }, room.config.timeout * 1000);
  }
  push(room);
}

// 全员提交完（或超时）就结算，服务器只负责判断是否完全一致
function completeRound(room) {
  if (live(room).length < 2) return;
  clearTimeout(room.timer);
  room.timer = null;
  room.deadline = null;

  const items = room.order.filter((t) => room.players.get(t)?.connected).map((t) => ({
    id: t,
    name: room.players.get(t).name,
    word: room.words.has(t) ? room.words.get(t) : null,
  }));
  const said = items.filter((i) => i.word);
  const matched = said.length === items.length && new Set(said.map((i) => norm(i.word))).size === 1;

  room.history.push({ round: room.round, items, matched });
  for (const w of said) room.used.set(norm(w.word), w.word);
  room.votes.clear();          // 上一轮的票作废，新一轮重新投

  // 完全一致：服务器直接判成功，本局当场结束，不给投票的机会
  if (matched) {
    room.phase = 'done';
    room.endedBy = 'match';
    room.completedAt = Date.now();
    return push(room);
  }

  // 不一致：进入下一轮，玩家可以在新输入页上投票认定（土豆 / 马铃薯 那种情况）
  startRound(room);
}

// 任何人改票后检查一次：所有在线玩家都认定 → 本局结束
function settle(room) {
  if (room.phase !== 'play' || !room.history.length) return false;
  const alive = live(room);
  if (alive.length < 2) return false;
  if (!alive.every((p) => room.votes.get(p.token) === true)) return false;
  room.phase = 'done';
  room.endedBy = 'accepted';
  room.completedAt = Date.now();
  clearTimeout(room.timer);
  room.timer = null;
  room.deadline = null;
  return true;
}

/* ------------------------------ 连接逻辑 ------------------------------ */

function resetGame(room) {
  clearTimeout(room.timer);
  room.timer = null;
  room.deadline = null;
  room.history.length = 0;
  room.used.clear();
  room.words.clear();
  room.votes.clear();
  room.ready.clear();
  room.round = 0;
  room.endedBy = null;
  room.gameId = randomUUID();
  room.completedAt = null;
  room.interrupted = false;
}

function restartIfReady(room) {
  const alive = live(room);
  if (room.phase !== 'done' || alive.length < 2 || !alive.every((p) => room.ready.has(p.token))) return false;
  resetGame(room);
  startRound(room);
  return true;
}

function joinRoom(conn, room, name, token) {
  name = String(name || '').trim().slice(0, 12);
  const p = room.players.get(token);

  if (p) {                                   // 重连：换 socket，保留位置
    if (room.phase === 'play' && p.ws !== conn.ws) room.interrupted = true;
    if (p.ws && p.ws !== conn.ws) { try { p.ws.close(); } catch {} }
    p.ws = conn.ws;
    p.connected = true;
    if (name) p.name = name;
  } else {
    if (room.phase !== 'lobby') return send(conn.ws, { t: 'error', msg: '游戏已经开始了' });
    if (room.players.size >= room.config.max) return send(conn.ws, { t: 'error', msg: '房间已满' });
    room.players.set(token, {
      token,
      name: name || randomName(room),
      ws: conn.ws,
      connected: true,
    });
    room.order.push(token);
  }

  conn.token = token;
  conn.room = room;
  room.touched = Date.now();
  send(conn.ws, { t: 'joined', token });
  push(room);
}

function onMessage(conn, m) {
  const ws = conn.ws;

  if (m.t === 'create') {
    const cfg = m.config || {};
    const room = makeRoom({
      max: Math.min(8, Math.max(2, Number(cfg.max) || 2)),
      timeout: [30, 60, 0].includes(Number(cfg.timeout)) ? Number(cfg.timeout) : 0,
    });
    rooms.set(room.code, room);
    return joinRoom(conn, room, m.name, m.token || randomUUID());
  }

  const room = conn.room;
  const player = room?.players.get(conn.token);
  if (!player || player.ws !== ws || !player.connected || rooms.get(room.code) !== room) return;
  room.touched = Date.now();

  if (m.t === 'leave') {
    conn.room = null;
    if (hostOf(room) === conn.token) {
      resetGame(room);
      rooms.delete(room.code);
      for (const p of room.players.values()) send(p.ws, { t: 'left', dissolved: true });
      room.players.clear();
      room.order.length = 0;
    } else {
      room.players.delete(conn.token);
      room.order = room.order.filter((t) => t !== conn.token);
      resetGame(room);
      room.phase = 'lobby';
      send(ws, { t: 'left' });
      push(room);
    }
    return;
  }

  if (m.t === 'start') {
    if (room.phase !== 'lobby') return;
    if (hostOf(room) !== conn.token) return send(ws, { t: 'error', msg: '只有房主能开始' });
    if (live(room).length < 2) return send(ws, { t: 'error', msg: '至少需要两个人' });
    return startRound(room);
  }

  if (m.t === 'submit') {
    if (room.phase !== 'play') return send(ws, { t: 'error', msg: '现在不能提交' });
    if (room.words.has(conn.token)) return;
    const raw = String(m.word ?? '').trim().slice(0, WORD_MAX);
    if (!norm(raw)) return send(ws, { t: 'error', msg: '请输入一个词' });
    if (room.used.has(norm(raw))) return send(ws, { t: 'error', msg: `「${raw}」前面出现过了，换一个` });
    room.words.set(conn.token, raw);
    const alive = live(room);
    if (alive.length >= 2 && alive.every((p) => room.words.has(p.token))) completeRound(room);
    else push(room);
    return;
  }

  // 在输入页给「上一轮结果」投票，投票是 toggle
  if (m.t === 'vote') {
    if (room.phase !== 'play') return;
    const last = room.history.at(-1);
    if (!last) return send(ws, { t: 'error', msg: '还没有可以认定的结果' });
    if (last.matched) return send(ws, { t: 'error', msg: '已经一致了，不用投票' });
    if (room.votes.get(conn.token) === true) room.votes.delete(conn.token);
    else room.votes.set(conn.token, true);
    settle(room);
    push(room);
    return;
  }

  if (m.t === 'restart') {
    if (room.phase !== 'done') return;
    if (room.ready.has(conn.token)) room.ready.delete(conn.token);
    else room.ready.add(conn.token);
    if (!restartIfReady(room)) push(room);
    return;
  }
}

/* -------------------------------- HTTP -------------------------------- */

const lanIPs = Object.values(networkInterfaces()).flat()
  .filter((i) => i && i.family === 'IPv4' && !i.internal)
  .map((i) => i.address)
  .filter((ip) => /^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(ip)); // 只报内网地址

const server = createServer(async (req, res) => {
  const path = new URL(req.url, 'http://localhost').pathname;
  if (path === '/favicon.png' || path === '/favicon.ico') {
    try {
      const icon = await readFile(join(ROOT, 'public', path.slice(1)));
      res.writeHead(200, { 'content-type': path.endsWith('.png') ? 'image/png' : 'image/x-icon' });
      return res.end(icon);
    } catch {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
      return res.end('not found');
    }
  }
  if (path === '/' || path.startsWith('/r/')) {
    // ponytail: 每次读盘，改前端不用重启；文件很小，交给系统页缓存
    const html = await readFile(join(ROOT, 'public/index.html'));
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    return res.end(html);
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('not found');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  const conn = { ws, room: null, token: null };

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'join') {
      const room = rooms.get(String(m.room || '').toUpperCase());
      if (!room) return send(ws, { t: 'error', msg: '房间不存在' });
      return joinRoom(conn, room, m.name, m.token || randomUUID());
    }
    onMessage(conn, m);
  });

  ws.on('close', () => {
    const room = conn.room;
    if (!room || !conn.token || rooms.get(room.code) !== room) return;
    const p = room.players.get(conn.token);
    if (!p || p.ws !== ws) return;         // 已被新连接顶替，忽略
    if (room.phase === 'play') room.interrupted = true;
    p.connected = false;
    p.ws = null;
    room.ready.delete(conn.token);
    room.touched = Date.now();
    if (room.phase === 'done' && restartIfReady(room)) return;
    push(room);
    if (room.phase !== 'play') return;
    const alive = live(room);
    if (alive.length >= 2 && alive.every((p) => room.words.has(p.token))) completeRound(room);
    else settle(room) && push(room);
  });
});

setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    const dead = !live(room).length && now - room.touched > IDLE_TTL;
    if (dead || now - room.createdAt > ROOM_TTL) rooms.delete(code);
  }
}, 60_000).unref();

server.listen(PORT, () => {
  console.log(`同频猜词 本机   http://localhost:${PORT}`);
  for (const ip of lanIPs) console.log(`          局域网 http://${ip}:${PORT}`);
});
