-- 背景測試效能量測（排隊器 loop_perf 事件）
CREATE TABLE IF NOT EXISTS loop_perf (n INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, main_sha TEXT, kind TEXT, wall_s REAL, cores INTEGER, cpu_util REAL, items INTEGER, top TEXT);
