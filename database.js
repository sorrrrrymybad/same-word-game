// Author: Sorrymybad
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = dirname(fileURLToPath(import.meta.url));
function beijingTime(value = Date.now()) {
  const date = new Date(value);
  date.setUTCHours(date.getUTCHours() + 8);
  return date.toISOString().replace('Z', '+08:00');
}

const legacyFile = process.env.STATS_FILE || join(ROOT, 'data/stats.json');
const databaseFile = process.env.DATABASE_FILE || join(dirname(legacyFile), 'game.sqlite');
mkdirSync(dirname(databaseFile), { recursive: true });
const db = new DatabaseSync(databaseFile);
db.exec(`
  PRAGMA busy_timeout = 5000;
  PRAGMA journal_mode = DELETE;
  PRAGMA synchronous = FULL;
  CREATE TABLE IF NOT EXISTS metrics (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL CHECK (value >= 0),
    since TEXT NOT NULL,
    updated_at TEXT NOT NULL
  ) STRICT;
  CREATE TABLE IF NOT EXISTS games (
    id TEXT PRIMARY KEY,
    room_code TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    ended_at TEXT,
    player_count INTEGER NOT NULL,
    players_json TEXT NOT NULL,
    history_json TEXT NOT NULL,
    current_round_json TEXT,
    status TEXT NOT NULL CHECK (status IN ('play', 'done', 'aborted', 'interrupted')),
    ended_by TEXT
  ) STRICT;
`);

function transaction(write) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = write();
    db.exec('COMMIT');
    return result;
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}

transaction(() => {
  if (db.prepare('SELECT count(*) AS count FROM metrics').get().count) return;
  const now = beijingTime();
  let saved = { startedGames: 0, since: now };
  try {
    saved = JSON.parse(readFileSync(legacyFile, 'utf8'));
    if (!saved || !Number.isSafeInteger(saved.startedGames) || saved.startedGames < 0
        || typeof saved.since !== 'string' || !Number.isFinite(Date.parse(saved.since))) {
      throw new Error('旧统计文件内容无效，请检查或恢复备份');
    }
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }
  const insert = db.prepare('INSERT INTO metrics (name, value, since, updated_at) VALUES (?, ?, ?, ?)');
  insert.run('started_games', saved.startedGames, beijingTime(saved.since), now);
  insert.run('site_visits', 0, now, now);
});

transaction(() => {
  for (const [table, columns] of [
    ['metrics', ['since', 'updated_at']],
    ['games', ['started_at', 'updated_at', 'ended_at']],
  ]) {
    for (const column of columns) {
      db.exec(`UPDATE ${table} SET ${column} = strftime('%Y-%m-%dT%H:%M:%f', ${column}, '+8 hours') || '+08:00'
        WHERE ${column} LIKE '%Z'`);
    }
  }
});

// 内存房间不会在重启后恢复，保留最后一次已写入的对局内容。
const startupTime = beijingTime();
db.prepare("UPDATE games SET status = 'interrupted', ended_at = ?, updated_at = ? WHERE status = 'play'")
  .run(startupTime, startupTime);

const increment = db.prepare('UPDATE metrics SET value = value + 1, updated_at = ? WHERE name = ?');
const readMetrics = db.prepare('SELECT name, value, since, updated_at FROM metrics');
const insertGame = db.prepare(`INSERT INTO games
  (id, room_code, started_at, updated_at, player_count, players_json, history_json, status)
  VALUES (?, ?, ?, ?, ?, ?, '[]', 'play')`);
const updateGame = db.prepare(`UPDATE games SET updated_at = ?, ended_at = ?, players_json = ?,
  history_json = ?, current_round_json = ?, status = ?, ended_by = ? WHERE id = ? AND status = 'play'`);

export function getMetrics() {
  return Object.fromEntries(readMetrics.all().map((row) => [row.name, row]));
}

export function recordVisit() {
  increment.run(beijingTime(), 'site_visits');
}

export function recordGameStart(room, players) {
  const now = beijingTime();
  transaction(() => {
    insertGame.run(room.gameId, room.code, now, now, players.length, JSON.stringify(players));
    increment.run(now, 'started_games');
  });
}

export function saveGame(room, status = room.phase) {
  const now = beijingTime();
  const current = room.phase === 'play' ? {
    round: room.round,
    items: room.order.filter((token) => room.words.has(token)).map((token) => ({
      id: room.players.get(token).id,
      name: room.players.get(token).name,
      word: room.words.get(token),
    })),
    votes: [...room.votes.keys()].map((token) => room.players.get(token)?.id).filter(Boolean),
  } : null;
  updateGame.run(now, status === 'play' ? null : now, JSON.stringify([...room.gamePlayers.values()]),
    JSON.stringify(room.history), current ? JSON.stringify(current) : null, status, room.endedBy, room.gameId);
}
