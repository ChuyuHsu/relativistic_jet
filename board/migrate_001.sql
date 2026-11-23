-- 已部署的資料庫升級用（schema.sql 是新裝用）。每條只能跑一次。
ALTER TABLE cards ADD COLUMN owner_thread TEXT;
ALTER TABLE cards ADD COLUMN parent TEXT;
UPDATE cards SET status='claimed' WHERE status='doing';
UPDATE cards SET status='pushed' WHERE status='review';
