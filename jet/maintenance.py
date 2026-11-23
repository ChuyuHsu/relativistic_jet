"""維運：每小時備份、還原、設定檔熱重載前自檢（DESIGN 第 8、9 節）。

- 備份：用 SQLite backup API 做一致的本機備份，放在 state_dir/backups/，留最近 keep 份。
- 還原：資料庫壞了就拿最新的好備份蓋回去。**寧可重審，不可誤合**：
    · 備份之後才放行的分支，放行記號已經不在（還原後要重新確認）；
    · 備份之後才撤回的放行，從 revocations.log（只增不改的文字檔）補回撤回，避免撤回過的放行復活；
    · 還原前正在跑的工作、批全部視為作廢，分支回到安全狀態，並登記一筆待辦通知人。
  還原後排隊器第一輪 tick 會重新掃一次 Git 上的分支（那是調度核心的工作）。
- 設定檔重載：新的檔先完整檢查；壞的不載、沿用舊的；改到「要重啟才安全」的欄位也拒絕。
"""
import fcntl
import glob
import hashlib
import json
import os
import shutil
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

from . import config, db, states
from .states import Event, State

DB_NAME = "jet.sqlite"
BACKUP_DIR = "backups"
REVOCATION_LOG = "revocations.log"
LOCK_NAME = "jet.lock"

# 換這些欄位要重啟才安全：熱重載時拒絕
RESTART_ONLY = ("remote", "state_dir", "work_dir", "listen", "read_key_file", "write_key_file", "pool", "source")


class MaintenanceError(Exception):
    pass


def db_path(state_dir: str) -> str:
    return os.path.join(state_dir, DB_NAME)


# ---------------------------------------------------------------- 鎖

class StateLock:
    """state_dir 的獨佔鎖：排隊器主行程活著就一直持有；還原只在拿得到鎖（＝主行程沒在跑）時做。"""

    def __init__(self, state_dir: str):
        os.makedirs(state_dir, exist_ok=True)
        self.path = os.path.join(state_dir, LOCK_NAME)
        self._f = None

    def acquire(self) -> bool:
        f = open(self.path, "a+")
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            f.close()
            return False
        f.seek(0)
        f.truncate()
        f.write(str(os.getpid()))
        f.flush()
        self._f = f
        return True

    def release(self) -> None:
        if self._f is not None:
            try:
                fcntl.flock(self._f, fcntl.LOCK_UN)
            finally:
                self._f.close()
                self._f = None


# ---------------------------------------------------------------- 撤回紀錄（還原用）

def log_revocation(state_dir: str, branch: str, at: float) -> None:
    """撤回放行時同步記一行到只增不改的文字檔；資料庫壞掉還原時用來補回撤回。"""
    os.makedirs(state_dir, exist_ok=True)
    with open(os.path.join(state_dir, REVOCATION_LOG), "a", encoding="utf-8") as f:
        f.write(json.dumps({"branch": branch, "at": at}, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def read_revocations(state_dir: str, since: float) -> List[Dict[str, Any]]:
    path = os.path.join(state_dir, REVOCATION_LOG)
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue   # 壞行略過（寧可多撤回，所以略過只可能少撤回一行：下面不依賴它做放行）
                if isinstance(r, dict) and isinstance(r.get("branch"), str) and float(r.get("at", 0)) > since:
                    out.append(r)
    except FileNotFoundError:
        pass
    return out


# ---------------------------------------------------------------- 備份

def list_backups(state_dir: str) -> List[str]:
    """由新到舊。"""
    return sorted(glob.glob(os.path.join(state_dir, BACKUP_DIR, "jet-*.sqlite")), reverse=True)


def integrity_ok(path: str) -> bool:
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            return con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            con.close()
    except sqlite3.Error:
        return False


def make_backup(con: sqlite3.Connection, state_dir: str, now: Optional[float] = None, keep: int = 48) -> str:
    """做一份備份、驗證能開，再輪替掉太舊的。回備份路徑。"""
    now = time.time() if now is None else now
    d = os.path.join(state_dir, BACKUP_DIR)
    os.makedirs(d, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime(now))
    final = os.path.join(d, f"jet-{stamp}.sqlite")
    tmp = final + ".part"
    try:
        db.backup(con, tmp)
        c2 = sqlite3.connect(tmp)
        try:
            c2.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('backup_made_at', ?)", (repr(now),))
            c2.commit()
        finally:
            c2.close()
        if not integrity_ok(tmp):
            raise MaintenanceError("備份做出來但完整性檢查沒過")
        os.replace(tmp, final)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    for old in list_backups(state_dir)[max(1, keep):]:
        os.remove(old)
    return final


class BackupSchedule:
    """每 interval_s 秒備份一次；重啟後從最新備份檔的時間接著算，不會一啟動就狂備份。"""

    def __init__(self, state_dir: str, interval_s: int = 3600, keep: int = 48):
        self.state_dir, self.interval_s, self.keep = state_dir, interval_s, keep
        latest = list_backups(state_dir)
        self.last = os.path.getmtime(latest[0]) if latest else 0.0
        self.last_error: Optional[str] = None

    def tick(self, con, now: Optional[float] = None) -> Optional[str]:
        now = time.time() if now is None else now
        if now - self.last < self.interval_s:
            return None
        self.last = now   # 失敗也等下個間隔再試，不要每輪都試
        try:
            path = make_backup(con, self.state_dir, now, self.keep)
            self.last_error = None
            return path
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {e}"
            return None


# ---------------------------------------------------------------- 還原

def restore(state_dir: str, backup_path: Optional[str] = None, now: Optional[float] = None) -> Dict[str, Any]:
    """把備份蓋回去。主行程必須沒在跑（拿不到鎖就拒絕）。壞掉的舊資料庫改名留著，不刪。"""
    now = time.time() if now is None else now
    lock = StateLock(state_dir)
    if not lock.acquire():
        raise MaintenanceError("排隊器還在跑（state_dir 的鎖被持有），請先停掉再還原")
    try:
        cands = [backup_path] if backup_path else list_backups(state_dir)
        chosen = None
        for p in cands:
            if p and os.path.exists(p) and integrity_ok(p):
                chosen = p
                break
        if chosen is None:
            raise MaintenanceError("找不到完整性檢查通過的備份")
        target = db_path(state_dir)
        moved = None
        if os.path.exists(target):
            moved = f"{target}.broken-{int(now)}"
            for suffix in ("", "-wal", "-shm"):
                if os.path.exists(target + suffix):
                    os.replace(target + suffix, moved + suffix)
        shutil.copyfile(chosen, target)
        con = db.connect(target)
        try:
            made_at = float(db_meta(con, "backup_made_at", "0") or 0)
            summary = _after_restore(con, state_dir, made_at, now)
        finally:
            con.close()
        summary.update({"restored_from": chosen, "old_db": moved, "backup_made_at": made_at})
        return summary
    finally:
        lock.release()


def db_meta(con, key: str, default: Optional[str] = None) -> Optional[str]:
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def _after_restore(con, state_dir: str, made_at: float, now: float) -> Dict[str, Any]:
    con.execute("BEGIN IMMEDIATE")
    try:
        # 1. 備份之後撤回的放行：補回撤回（撤回過的放行不能復活）
        revoked = 0
        for r in read_revocations(state_dir, made_at):
            revoked += con.execute("UPDATE approvals SET revoked_at=? WHERE branch=? AND revoked_at IS NULL",
                                   (now, r["branch"])).rowcount
        # 2. 還原前在跑的工作與批全部作廢
        con.execute("UPDATE runs SET status='cancelled', finished_at=? WHERE status IN ('queued','running')", (now,))
        con.execute("UPDATE batches SET status='aborted', finished_at=? WHERE status IN ('testing','probing')", (now,))
        # 3. 分支回到安全狀態：自測中→待自測；批測中→排隊
        fixed = 0
        for row in con.execute("SELECT name, state FROM branches WHERE state IN ('selftesting','batching')").fetchall():
            ev = Event.PUSHED if row["state"] == State.SELFTESTING.value else Event.BATCH_DEFERRED
            con.execute("UPDATE branches SET state=?, updated_at=? WHERE name=?",
                        (states.transition(row["state"], ev).value, now, row["name"]))
            fixed += 1
        con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('restored_at', ?)", (repr(now),))
        con.execute("INSERT INTO events(kind, branch, detail, at) VALUES('queue_error', NULL, ?, ?)",
                    ("資料庫已從備份還原：備份之後才放行的分支要重新確認放行，排隊器會重新掃一次 Git 分支", now))
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return {"revocations_replayed": revoked, "branches_reset": fixed}


# ---------------------------------------------------------------- 設定檔熱重載

def _file_sig(path: str) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except OSError:
        return None


def restart_only_diffs(old: config.Config, new: config.Config) -> List[str]:
    return [n for n in RESTART_ONLY if getattr(old, n) != getattr(new, n)]


class ConfigWatcher:
    """每輪 poll 一次：檔案內容變了才重載；新的壞了或改到要重啟的欄位就沿用舊的，並回報原因。"""

    def __init__(self, path: str, cfg: config.Config):
        self.path, self.cfg = path, cfg
        self._sig = _file_sig(path)
        self.last_error: Optional[str] = None

    def poll(self) -> Tuple[config.Config, Optional[str]]:
        """回 (目前生效的設定, 新的錯誤訊息或 None)。同一份壞檔只回報一次。"""
        sig = _file_sig(self.path)
        if sig is None:
            if self._sig is not None:
                self._sig = None
                self.last_error = "設定檔讀不到，沿用舊的"
                return self.cfg, self.last_error
            return self.cfg, None
        if sig == self._sig:
            return self.cfg, None
        self._sig = sig
        try:
            new = config.load(self.path)
        except (config.ConfigError, OSError) as e:
            self.last_error = f"設定檔有問題，沿用舊的：{e}"
            return self.cfg, self.last_error
        diffs = restart_only_diffs(self.cfg, new)
        if diffs:
            self.last_error = "這些欄位要重啟才能改，這次重載整份不採用：" + "、".join(diffs)
            return self.cfg, self.last_error
        self.cfg, self.last_error = new, None
        return self.cfg, None


def record_queue_error(con, detail: str, now: Optional[float] = None) -> None:
    now = time.time() if now is None else now
    con.execute("INSERT INTO events(kind, branch, detail, at) VALUES('queue_error', NULL, ?, ?)",
                (detail[:500], now))
