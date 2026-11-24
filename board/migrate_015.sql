-- BD-01 讀取量止血：事件表按卡號建索引；已合併的卡 status 改成 merged，不再留在「claimed」的掃描範圍
CREATE INDEX IF NOT EXISTS events_card ON events(card_id, n);
UPDATE cards SET status='merged' WHERE phase='merged' AND status='claimed';
