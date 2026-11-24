-- BD-07 串號轉址：舊串號 → 新串號。信箱領信時沿著轉址鏈解析收件串，不用到處改串號
CREATE TABLE IF NOT EXISTS thread_redirect (from_thread TEXT PRIMARY KEY, to_thread TEXT NOT NULL, at INTEGER NOT NULL);
