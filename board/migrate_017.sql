-- BD-04 清理：hook_seen（排隊器事件去重表）加時間欄，整理時只留 7 天
ALTER TABLE hook_seen ADD COLUMN at INTEGER;
UPDATE hook_seen SET at=CAST(strftime('%s','now') AS INTEGER) WHERE at IS NULL;
CREATE INDEX IF NOT EXISTS hook_seen_at ON hook_seen(at);
