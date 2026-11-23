-- 卡片只保留前段與結局，中段改看上游分支狀態（phase 快取）。只跑一次。
ALTER TABLE cards ADD COLUMN phase TEXT;
ALTER TABLE cards ADD COLUMN phase_at INTEGER;
UPDATE cards SET phase=CASE status WHEN 'pushed' THEN 'pending' WHEN 'test_red' THEN 'rejected' WHEN 'ready' THEN 'selftest_green' ELSE status END, status='claimed'
  WHERE status IN ('pushed','test_red','ready','queued','merged','rejected');
