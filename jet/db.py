"""SQLite 結構（DESIGN 第 8 節）。只有排隊器主行程寫。"""
import sqlite3

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS branches(
  name TEXT PRIMARY KEY,
  head_sha TEXT NOT NULL,
  content_id TEXT NOT NULL,          -- 內容指紋（patch-id）；重疊到新 main 不變
  parent TEXT REFERENCES branches(name),  -- 堆疊：父分支（NULL＝普通分支）
  state TEXT NOT NULL,
  seen_at REAL NOT NULL DEFAULT 0,   -- 首次看到時間（自測順序，換新提交不變）
  queued_at REAL,                    -- 首次排隊時間（順位保留用；內容變了才清）
  prio_override INTEGER NOT NULL DEFAULT 0,
  fail_count INTEGER NOT NULL DEFAULT 0,   -- 批測紅次數
  docs_only INTEGER,                 -- 分類指令結果：1 純文件／0 要測／NULL 未分類
  reject_reason TEXT,
  merged_sha TEXT,
  approved_sha TEXT,                 -- 外部來源清單上最後一次看到的放行版本（事件帶它）
  retest INTEGER,                    -- 外部來源清單上最後一次看到的重測計數
  updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS branches_state ON branches(state);

CREATE TABLE IF NOT EXISTS tracked(    -- 登記制：只管這張表裡的分支（設定 git.scope="tracked"）
  branch TEXT PRIMARY KEY,
  parent TEXT,                         -- 登記時帶的父分支（分支第一次被掃到時套用）
  parent_applied INTEGER NOT NULL DEFAULT 0,
  at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS approvals(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  branch TEXT NOT NULL, content_id TEXT NOT NULL,
  by TEXT, at REAL NOT NULL, revoked_at REAL
);
CREATE INDEX IF NOT EXISTS approvals_branch ON approvals(branch, content_id);

CREATE TABLE IF NOT EXISTS batches(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  base_sha TEXT NOT NULL, status TEXT NOT NULL,  -- testing|probing|merged|aborted
  started_at REAL NOT NULL, finished_at REAL, merged_sha TEXT, merged_count INTEGER
);
CREATE TABLE IF NOT EXISTS batch_members(
  batch_id INTEGER NOT NULL REFERENCES batches(id),
  pos INTEGER NOT NULL, branch TEXT NOT NULL, content_id TEXT NOT NULL,
  squash_sha TEXT, PRIMARY KEY(batch_id, pos)
);

CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,                -- selftest|batch_point|probe|baseline|adhoc
  branch TEXT, batch_id INTEGER, size INTEGER,
  layer TEXT NOT NULL, commit_sha TEXT NOT NULL, base_sha TEXT,
  status TEXT NOT NULL,              -- queued|running|pass|fail|timeout|infra|cancelled
  cores TEXT, pgid INTEGER, started_at REAL, finished_at REAL,
  exit_code INTEGER, log_path TEXT, report_path TEXT
);
CREATE INDEX IF NOT EXISTS runs_status ON runs(status);

CREATE TABLE IF NOT EXISTS run_items(
  run_id INTEGER NOT NULL REFERENCES runs(id),
  item_id TEXT NOT NULL, status TEXT NOT NULL, summary TEXT,
  PRIMARY KEY(run_id, item_id)
);

CREATE TABLE IF NOT EXISTS baseline(   -- main 基準：該層在這個 main 上本來就紅的項目
  main_sha TEXT NOT NULL, layer TEXT NOT NULL,
  failed_items TEXT NOT NULL,        -- JSON 陣列
  recorded_at REAL NOT NULL,
  copied INTEGER NOT NULL DEFAULT 0, -- 1＝純文件更動時沿用上一份，不算一次新的 main 量測
  PRIMARY KEY(main_sha, layer)
);

CREATE TABLE IF NOT EXISTS events(     -- 待辦／通知事件
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,                -- rejected|conflict|halted|queue_error
  branch TEXT, detail TEXT, at REAL NOT NULL, notified_at REAL, resolved_at REAL,
  commit_sha TEXT,                   -- 事件發生時分支的提交（給 webhook 用）
  approved_sha TEXT,                 -- 事件依據的放行版本（外部來源模式）
  retest INTEGER                     -- 事件依據的重測計數
);
CREATE TABLE IF NOT EXISTS shas(       -- 看過的每個提交對應的內容指紋（放行版本被覆蓋後還能比對）
  branch TEXT NOT NULL, sha TEXT NOT NULL, content_id TEXT NOT NULL,
  PRIMARY KEY(branch, sha)
);
CREATE TABLE IF NOT EXISTS audit(      -- 所有寫入操作留紀錄
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at REAL NOT NULL, actor TEXT, action TEXT NOT NULL, args TEXT
);
"""


def connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    migrate(con)
    return con


def migrate(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    cols = [r[1] for r in con.execute("PRAGMA table_info(baseline)")]
    if "copied" not in cols:                                    # 舊資料庫補欄位
        con.execute("ALTER TABLE baseline ADD COLUMN copied INTEGER NOT NULL DEFAULT 0")
    cols = [r[1] for r in con.execute("PRAGMA table_info(events)")]
    if "commit_sha" not in cols:   # 舊資料庫補欄位
        con.execute("ALTER TABLE events ADD COLUMN commit_sha TEXT")
    for col, typ in (("approved_sha", "TEXT"), ("retest", "INTEGER")):
        if col not in cols:
            con.execute(f"ALTER TABLE events ADD COLUMN {col} {typ}")
    cols = [r[1] for r in con.execute("PRAGMA table_info(runs)")]
    for col, typ in (("workdir", "TEXT"), ("timeout_s", "REAL"), ("units", "INTEGER"),
                     ("approved_sha", "TEXT"), ("retest", "INTEGER")):    # 換版接管要用的
        if col not in cols:
            con.execute(f"ALTER TABLE runs ADD COLUMN {col} {typ}")
    cols = [r[1] for r in con.execute("PRAGMA table_info(branches)")]
    for col, typ in (("approved_sha", "TEXT"), ("retest", "INTEGER")):
        if col not in cols:
            con.execute(f"ALTER TABLE branches ADD COLUMN {col} {typ}")
    if con.execute("SELECT 1 FROM meta WHERE key='instance_id'").fetchone() is None:
        import secrets
        con.execute("INSERT INTO meta VALUES('instance_id', ?)", (secrets.token_hex(4),))   # 資料庫建立時產生，事件編號用它當前綴
    row = con.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if row is None:
        con.execute("INSERT INTO meta VALUES('schema_version', ?)", (str(SCHEMA_VERSION),))
    elif int(row[0]) > SCHEMA_VERSION:
        raise RuntimeError("資料庫版本比程式新，拒絕開啟")


def backup(con: sqlite3.Connection, dest_path: str) -> None:
    """用 SQLite 備份 API 做一致的本機備份（每小時一次）。"""
    dst = sqlite3.connect(dest_path)
    try:
        con.backup(dst)
    finally:
        dst.close()
