-- 收信端領取：領取時間與領取編號
ALTER TABLE outbox ADD COLUMN claimed_at INTEGER;
ALTER TABLE outbox ADD COLUMN claim_id TEXT;
