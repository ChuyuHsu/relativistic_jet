-- 階段線：送審與審查退回。只跑一次。
ALTER TABLE cards ADD COLUMN submitted_commit TEXT;
ALTER TABLE cards ADD COLUMN submitted_at INTEGER;
ALTER TABLE cards ADD COLUMN rejected_reason TEXT;
ALTER TABLE cards ADD COLUMN rejected_by TEXT;
ALTER TABLE cards ADD COLUMN rejected_at INTEGER;
ALTER TABLE cards ADD COLUMN stuck_notified_at INTEGER;
