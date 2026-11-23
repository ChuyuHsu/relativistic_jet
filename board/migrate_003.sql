-- 登記制：分支名由看板給定、對排隊器的呼叫可補送。只跑一次。
ALTER TABLE cards ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1;
CREATE TABLE IF NOT EXISTS sync_jobs (
  n INTEGER PRIMARY KEY AUTOINCREMENT,
  card_id TEXT NOT NULL,
  op TEXT NOT NULL,                       -- track / untrack / approve / revoke
  payload TEXT NOT NULL,                  -- 要送給上游排隊器的 JSON
  state TEXT NOT NULL DEFAULT 'pending',  -- pending / sending（某個行程正在送）/ done / failed（上游明確拒絕，不再重送）
  claimed_at INTEGER,                     -- 轉成 sending 的時間；太久沒結果由 runSync 放回 pending
  tries INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  done_at INTEGER
);
CREATE INDEX IF NOT EXISTS sync_pending ON sync_jobs(state, n);
