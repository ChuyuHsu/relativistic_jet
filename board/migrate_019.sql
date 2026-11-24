-- NIGHT-02 每晚完整層結果：排隊器用 full_result 事件回報，存最近 14 次；GET /api/nightly 讀
CREATE TABLE IF NOT EXISTS nightly (n INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, main_sha TEXT, started_at INTEGER, finished_at INTEGER, total INTEGER, red TEXT, timeouts TEXT);
CREATE TRIGGER IF NOT EXISTS rev_nightly_i AFTER INSERT ON nightly BEGIN UPDATE meta SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='rev'; END;
