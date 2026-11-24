-- 省讀取量（D1 免費額度每日 500 萬 rows_read）：沒有索引的查詢每次都掃全表
CREATE INDEX IF NOT EXISTS outbox_unsent ON outbox(n) WHERE sent_at IS NULL;
CREATE INDEX IF NOT EXISTS cards_dropping ON cards(dropping_at) WHERE dropping_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS cards_area ON cards(area);
CREATE INDEX IF NOT EXISTS cards_parent ON cards(parent) WHERE parent IS NOT NULL;
CREATE INDEX IF NOT EXISTS cards_superseded ON cards(superseded_by) WHERE superseded_by IS NOT NULL;
