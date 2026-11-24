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
  attempt INTEGER NOT NULL DEFAULT 1,       -- 第幾次分支（退回後換新分支重做 +1）
  approved_commit TEXT,                     -- 放行：審過的提交；撤回清空
  approved_by TEXT,
  approved_at INTEGER,
  submitted_commit TEXT,                    -- 送審的提交；放行必須等於它
  submitted_at INTEGER,
  rejected_reason TEXT,                     -- 審查退回的原因（重新送審清掉）
  rejected_by TEXT,
  rejected_at INTEGER,
  retest INTEGER NOT NULL DEFAULT 0,        -- 重測計數（被排隊器退回的卡，審查者或管理者按重測 +1）
  stuck_notified_at INTEGER,                -- 卡住提醒最後發的時間（有新動靜才會再提醒）
  last_result TEXT,                         -- 排隊器最新結果（事件整理成 JSON：kind、commit、detail、failed_items、to…）
  phase TEXT,                              -- 上游排隊器分支狀態的快取
  phase_at INTEGER,
  branch TEXT,
  dropping_at INTEGER, superseded_by TEXT, discard_reason TEXT, drop_unmerged INTEGER, drop_checked_at INTEGER,
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
CREATE UNIQUE INDEX IF NOT EXISTS cards_branch ON cards(branch) WHERE branch IS NOT NULL;
-- 排隊器事件去重（至少送一次，對方可能重送）
CREATE TABLE IF NOT EXISTS hook_seen (key TEXT PRIMARY KEY);
-- 要叫醒負責串的事件，攢批後送出
CREATE TABLE IF NOT EXISTS outbox (
  n INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_thread TEXT NOT NULL DEFAULT '',
  card_id TEXT, branch TEXT, kind TEXT NOT NULL, detail TEXT,
  at INTEGER NOT NULL, sent_at INTEGER, claimed_at INTEGER, claim_id TEXT
);
CREATE TABLE IF NOT EXISTS fires (at INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS fire_log (n INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, status INTEGER, body TEXT);

CREATE TABLE IF NOT EXISTS hook_log (n INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, body TEXT, outcome TEXT);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS loop_perf (n INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, main_sha TEXT, kind TEXT, wall_s REAL, cores INTEGER, cpu_util REAL, items INTEGER, top TEXT, metrics TEXT);
