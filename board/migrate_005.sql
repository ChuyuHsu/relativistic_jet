-- 規格 docs/CARD_FLOW.md：重測計數。只跑一次。
ALTER TABLE cards ADD COLUMN retest INTEGER NOT NULL DEFAULT 0;
