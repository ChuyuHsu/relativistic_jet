-- 登記制與放行改為卡片欄位：分支名由看板給、放行存在卡片上、排隊器自己拉清單。只跑一次。
ALTER TABLE cards ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1;
ALTER TABLE cards ADD COLUMN approved_commit TEXT;
ALTER TABLE cards ADD COLUMN approved_by TEXT;
ALTER TABLE cards ADD COLUMN approved_at INTEGER;
ALTER TABLE cards ADD COLUMN last_result TEXT;
DROP TABLE IF EXISTS relays;
