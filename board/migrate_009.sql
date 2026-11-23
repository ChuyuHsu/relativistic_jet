-- 放棄防呆：放棄待查欄位
ALTER TABLE cards ADD COLUMN dropping_at INTEGER;
ALTER TABLE cards ADD COLUMN superseded_by TEXT;
ALTER TABLE cards ADD COLUMN discard_reason TEXT;
ALTER TABLE cards ADD COLUMN drop_unmerged INTEGER;
ALTER TABLE cards ADD COLUMN drop_checked_at INTEGER;
