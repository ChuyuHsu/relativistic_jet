CREATE TABLE IF NOT EXISTS cards (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT NOT NULL DEFAULT '',
  area TEXT NOT NULL DEFAULT '',
  priority INTEGER NOT NULL DEFAULT 100,   -- 數字小的先做
  status TEXT NOT NULL DEFAULT 'open',     -- open / claimed / merged / dropped（中段看 phase；merged＝已進 main，不再算進行中）
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
CREATE INDEX IF NOT EXISTS events_card ON events(card_id, n);   -- 讀單張卡的事件不用掃全表
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
-- 省讀取量（D1 免費額度每日 500 萬 rows_read）：沒有索引的查詢每次都掃全表
CREATE INDEX IF NOT EXISTS outbox_unsent ON outbox(n) WHERE sent_at IS NULL;
CREATE INDEX IF NOT EXISTS cards_dropping ON cards(dropping_at) WHERE dropping_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS cards_area ON cards(area);
CREATE INDEX IF NOT EXISTS cards_parent ON cards(parent) WHERE parent IS NOT NULL;
CREATE INDEX IF NOT EXISTS cards_superseded ON cards(superseded_by) WHERE superseded_by IS NOT NULL;
-- BD-03 版本號與 304：meta.rev 是全域版本號，卡片、流程狀態、效能表任何變動都加一（用觸發器，不靠程式記得加）
INSERT OR IGNORE INTO meta(key,value,updated_at) VALUES('rev','0',0);
CREATE TRIGGER IF NOT EXISTS rev_cards_i AFTER INSERT ON cards BEGIN UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='rev'; END;
CREATE TRIGGER IF NOT EXISTS rev_cards_u AFTER UPDATE ON cards BEGIN UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='rev'; END;
CREATE TRIGGER IF NOT EXISTS rev_cards_d AFTER DELETE ON cards BEGIN UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='rev'; END;
CREATE TRIGGER IF NOT EXISTS rev_meta_i AFTER INSERT ON meta WHEN NEW.key!='rev' BEGIN UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='rev'; END;
CREATE TRIGGER IF NOT EXISTS rev_meta_u AFTER UPDATE ON meta WHEN NEW.key!='rev' BEGIN UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='rev'; END;
CREATE TRIGGER IF NOT EXISTS rev_meta_d AFTER DELETE ON meta WHEN OLD.key!='rev' BEGIN UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='rev'; END;
CREATE TRIGGER IF NOT EXISTS rev_perf_i AFTER INSERT ON loop_perf BEGIN UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='rev'; END;
