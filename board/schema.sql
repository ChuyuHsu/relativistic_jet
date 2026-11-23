CREATE TABLE IF NOT EXISTS cards (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT NOT NULL DEFAULT '',
  area TEXT NOT NULL DEFAULT '',
  priority INTEGER NOT NULL DEFAULT 100,   -- 數字小的先做
  status TEXT NOT NULL DEFAULT 'open',     -- open / claimed / dropped（中段看 phase）
  owner TEXT,
  owner_thread TEXT,
  parent TEXT,
  phase TEXT,                              -- 上游排隊器分支狀態的快取
  phase_at INTEGER,
  branch TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS cards_status ON cards(status, priority);
CREATE TABLE IF NOT EXISTS events (
  n INTEGER PRIMARY KEY AUTOINCREMENT,
  card_id TEXT NOT NULL,
  at INTEGER NOT NULL,
  actor TEXT NOT NULL,
  note TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS relays (
  name TEXT PRIMARY KEY,
  upstream TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS cards_branch ON cards(branch);
-- 排隊器事件去重（至少送一次，對方可能重送）
CREATE TABLE IF NOT EXISTS hook_seen (key TEXT PRIMARY KEY);
-- 要叫醒負責串的事件，攢批後送出
CREATE TABLE IF NOT EXISTS outbox (
  n INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_thread TEXT NOT NULL DEFAULT '',
  card_id TEXT, branch TEXT, kind TEXT NOT NULL, detail TEXT,
  at INTEGER NOT NULL, sent_at INTEGER
);
CREATE TABLE IF NOT EXISTS fires (at INTEGER NOT NULL);
