"""查詢與操作（DESIGN 第 7 節）。

讀：任何執行緒都可以用「唯讀連線」呼叫 `query_*`，不會寫資料庫。
寫：一律包成命令丟進 `WriteQueue`，由排隊器主行程在每一輪 tick 裡呼叫 `drain` 執行，
    執行時寫 audit。除了主行程以外沒有任何寫入者（原則 2）。

這個模組只用標準庫，也不認識任何特定專案：分支名、提交、測試項目都只是字串。
"""
import json
import queue
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from . import states
from .states import Event, State

# 分支名：保守的白名單，擋掉空白、控制字元與路徑跳脫
BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/\-]{0,199}$")
LAYERS = ("selftest", "gate", "full")
ONLY_RE = re.compile(r"^[A-Za-z0-9_.:/\-]{1,200}$")

ACTIVE_STATES = (State.SELFTESTING.value, State.BATCHING.value)


class OpError(Exception):
    """操作被拒（參數不對、狀態不允許）。訊息直接回給呼叫者。"""
    def __init__(self, message: str, code: int = 400):
        super().__init__(message)
        self.code = code


# ---------------------------------------------------------------- 讀

def open_readonly(path: str) -> sqlite3.Connection:
    """唯讀連線：給 HTTP 執行緒用。WAL 模式下可和主行程同時讀。"""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    con.row_factory = sqlite3.Row
    return con


def _rows(cur) -> List[Dict[str, Any]]:
    return [dict(r) for r in cur.fetchall()]


def _meta(con, key: str, default: Optional[str] = None) -> Optional[str]:
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def _has_approval(con, branch: str, content_id: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM approvals WHERE branch=? AND content_id=? AND revoked_at IS NULL LIMIT 1",
        (branch, content_id)).fetchone()
    return row is not None


def query_status(con, now: Optional[float] = None) -> Dict[str, Any]:
    now = time.time() if now is None else now
    counts = {s.value: 0 for s in State}
    for r in con.execute("SELECT state, count(*) n FROM branches GROUP BY state"):
        counts[r["state"]] = r["n"]
    running = _rows(con.execute(
        "SELECT id, kind, branch, layer, size, started_at FROM runs WHERE status='running' ORDER BY id"))
    batch = con.execute(
        "SELECT id, status, started_at FROM batches WHERE status IN ('testing','probing') "
        "ORDER BY id DESC LIMIT 1").fetchone()
    last_merge = con.execute(
        "SELECT finished_at, merged_sha, merged_count FROM batches WHERE status='merged' "
        "ORDER BY id DESC LIMIT 1").fetchone()
    open_todo = con.execute("SELECT count(*) FROM events WHERE resolved_at IS NULL").fetchone()[0]
    return {
        "now": now,
        "paused": _meta(con, "paused", "0") == "1",
        "counts": counts,
        "queue_len": counts[State.QUEUED.value],
        "running": running,
        "batch": dict(batch) if batch else None,
        "last_merge": dict(last_merge) if last_merge else None,
        "main_sha": _meta(con, "main_sha"),
        "open_todo": open_todo,
        "restored_at": _meta(con, "restored_at"),
    }


def query_branches(con, state: Optional[str] = None) -> List[Dict[str, Any]]:
    sql = ("SELECT name, state, parent, prio_override, queued_at, fail_count, docs_only, "
           "reject_reason, merged_sha, head_sha, content_id, updated_at FROM branches")
    args: tuple = ()
    if state:
        sql += " WHERE state=?"
        args = (state,)
    sql += " ORDER BY prio_override DESC, COALESCE(queued_at, updated_at), name"
    out = _rows(con.execute(sql, args))
    for b in out:
        b["approved"] = _has_approval(con, b["name"], b["content_id"])
    return out


def query_branch(con, name: str) -> Dict[str, Any]:
    row = con.execute("SELECT * FROM branches WHERE name=?", (name,)).fetchone()
    if row is None:
        raise OpError("沒有這條分支", 404)
    b = dict(row)
    b["approved"] = _has_approval(con, name, b["content_id"])
    b["approvals"] = _rows(con.execute(
        "SELECT by, at, revoked_at, content_id FROM approvals WHERE branch=? ORDER BY id DESC LIMIT 10",
        (name,)))
    b["children"] = [r[0] for r in con.execute("SELECT name FROM branches WHERE parent=?", (name,))]
    runs = _rows(con.execute(
        "SELECT id, kind, layer, commit_sha, status, started_at, finished_at, exit_code "
        "FROM runs WHERE branch=? ORDER BY id DESC LIMIT 10", (name,)))
    for r in runs:
        r["failed_items"] = _rows(con.execute(
            "SELECT item_id, status, summary FROM run_items WHERE run_id=? AND status IN ('fail','timeout')",
            (r["id"],)))
    b["runs"] = runs
    b["events"] = _rows(con.execute(
        "SELECT id, kind, detail, at, resolved_at FROM events WHERE branch=? ORDER BY id DESC LIMIT 10",
        (name,)))
    return b


def query_merged(con, limit: int = 50) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    return _rows(con.execute(
        "SELECT name, merged_sha, updated_at FROM branches WHERE state='merged' "
        "ORDER BY updated_at DESC LIMIT ?", (limit,)))


def query_todo(con) -> Dict[str, Any]:
    """待辦＝尚未處理的事件，加上正等人處理的分支（退回、暫停）。"""
    events = _rows(con.execute(
        "SELECT id, kind, branch, detail, at, notified_at FROM events "
        "WHERE resolved_at IS NULL ORDER BY id DESC LIMIT 200"))
    waiting = _rows(con.execute(
        "SELECT name, state, reject_reason, fail_count, updated_at FROM branches "
        "WHERE state IN ('rejected','halted') ORDER BY updated_at DESC LIMIT 200"))
    return {"events": events, "waiting": waiting}


def _avg(con, sql: str, args: tuple = ()) -> Optional[float]:
    v = con.execute(sql, args).fetchone()[0]
    return None if v is None else round(float(v), 1)


def query_metrics(con, now: Optional[float] = None) -> Dict[str, Any]:
    now = time.time() if now is None else now
    day = now - 86400
    merged_24h = con.execute(
        "SELECT COALESCE(sum(merged_count),0) FROM batches WHERE status='merged' AND finished_at>=?",
        (day,)).fetchone()[0]
    return {
        "now": now,
        "merged_24h": int(merged_24h),
        "batches_24h": con.execute(
            "SELECT count(*) FROM batches WHERE started_at>=?", (day,)).fetchone()[0],
        "aborted_24h": con.execute(
            "SELECT count(*) FROM batches WHERE status='aborted' AND started_at>=?", (day,)).fetchone()[0],
        "selftest_avg_s": _avg(con, "SELECT avg(finished_at-started_at) FROM runs WHERE kind='selftest' "
                                    "AND finished_at IS NOT NULL AND finished_at>=?", (day,)),
        "batch_avg_s": _avg(con, "SELECT avg(finished_at-started_at) FROM batches WHERE status='merged' "
                                 "AND finished_at IS NOT NULL AND finished_at>=?", (day,)),
        "queue_wait_avg_s": _avg(con, "SELECT avg(? - queued_at) FROM branches WHERE state='queued' "
                                      "AND queued_at IS NOT NULL", (now,)),
        "runs_by_status": {r["status"]: r["n"] for r in con.execute(
            "SELECT status, count(*) n FROM runs WHERE started_at>=? GROUP BY status", (day,))},
        "rejected_24h": con.execute(
            "SELECT count(*) FROM events WHERE kind='rejected' AND at>=?", (day,)).fetchone()[0],
    }


# ---------------------------------------------------------------- 寫（只有主行程呼叫）

def _audit(con, now: float, actor: str, action: str, args: Dict[str, Any]) -> None:
    con.execute("INSERT INTO audit(at, actor, action, args) VALUES(?,?,?,?)",
                (now, actor, action, json.dumps(args, ensure_ascii=False, sort_keys=True)))


def _need_branch(con, name: Any) -> sqlite3.Row:
    if not isinstance(name, str) or not BRANCH_RE.match(name):
        raise OpError("分支名格式不對")
    row = con.execute("SELECT * FROM branches WHERE name=?", (name,)).fetchone()
    if row is None:
        raise OpError("沒有這條分支", 404)
    return row


def _set_state(con, row, event: Event, now: float, reason: Optional[str] = None) -> str:
    new = states.transition(row["state"], event).value
    if new != row["state"]:
        con.execute("UPDATE branches SET state=?, updated_at=?, reject_reason=COALESCE(?, reject_reason) "
                    "WHERE name=?", (new, now, reason, row["name"]))
    return new


def op_approve(con, now, actor, args):
    row = _need_branch(con, args.get("branch"))
    if row["state"] == State.MERGED.value:
        raise OpError("已經合入了")
    # 放行記在「審的那個提交」上：請求必須帶審過的版本，跟目前的對不上就拒絕（作者可能剛推了新版）
    sha = args.get("sha")
    if not isinstance(sha, str) or len(sha) < 7:
        raise OpError("放行要帶審過的版本（sha 或 content_id）", 400)
    content_id = row["content_id"]
    if not content_id or not (sha == content_id or row["head_sha"].startswith(sha) or sha.startswith(row["head_sha"])):
        raise OpError("分支的版本已經變了，請重新審過再放行", 409)
    if _has_approval(con, row["name"], content_id):
        return {"branch": row["name"], "state": row["state"], "already": True}
    by = str(args.get("by") or actor)[:100]
    con.execute("INSERT INTO approvals(branch, content_id, by, at) VALUES(?,?,?,?)",
                (row["name"], content_id, by, now))
    # 放行只是記號，排隊由主迴圈在「自測綠且放行」時發 READY；這裡不改狀態
    return {"branch": row["name"], "state": row["state"], "approved": True}


def op_revoke(con, now, actor, args, pre_commit=None):
    row = _need_branch(con, args.get("branch"))
    if pre_commit is not None:
        pre_commit("revoke", args, now)   # 先寫只增不改的撤回紀錄；寫失敗整個撤回失敗
    cur = con.execute("UPDATE approvals SET revoked_at=? WHERE branch=? AND revoked_at IS NULL",
                      (now, row["name"]))
    state = _set_state(con, row, Event.REVOKE, now)
    return {"branch": row["name"], "state": state, "revoked": cur.rowcount}


def op_priority(con, now, actor, args):
    row = _need_branch(con, args.get("branch"))
    value = args.get("value")
    if isinstance(value, bool) or not isinstance(value, int) or not -1000 <= value <= 1000:
        raise OpError("優先序要是 -1000 到 1000 的整數")
    con.execute("UPDATE branches SET prio_override=?, updated_at=? WHERE name=?",
                (value, now, row["name"]))
    return {"branch": row["name"], "priority": value}


def op_set_parent(con, now, actor, args):
    row = _need_branch(con, args.get("branch"))
    parent = args.get("parent")
    if parent in (None, ""):
        parent = None
    else:
        prow = _need_branch(con, parent)
        if prow["name"] == row["name"]:
            raise OpError("不能把自己登記成父分支")
        seen, cur = {row["name"]}, prow
        while cur is not None:   # 往上走，不能繞回自己
            if cur["name"] in seen:
                raise OpError("父分支串成了圈")
            seen.add(cur["name"])
            cur = (con.execute("SELECT * FROM branches WHERE name=?", (cur["parent"],)).fetchone()
                   if cur["parent"] else None)
    if parent == row["parent"]:
        return {"branch": row["name"], "parent": parent, "unchanged": True}
    con.execute("UPDATE branches SET parent=?, updated_at=? WHERE name=?", (parent, now, row["name"]))
    state = row["state"]
    try:   # 測試底變了：已測過的要重測（第 3 節「堆疊分支」）
        state = _set_state(con, row, Event.PARENT_CHANGED, now)
    except states.IllegalTransition:
        pass
    return {"branch": row["name"], "parent": parent, "state": state}


def op_adhoc_run(con, now, actor, args):
    row = _need_branch(con, args.get("branch"))
    layer = args.get("layer", "selftest")
    if layer not in LAYERS:
        raise OpError("layer 只能是 " + "、".join(LAYERS))
    only = args.get("only")
    if only is not None and (not isinstance(only, str) or not ONLY_RE.match(only)):
        raise OpError("only 格式不對")
    dup = con.execute("SELECT id FROM runs WHERE kind='adhoc' AND status IN ('queued','running') "
                      "AND branch=? AND layer=? AND commit_sha=?",
                      (row["name"], layer, row["head_sha"])).fetchone()
    if dup:
        return {"run_id": dup[0], "already": True}
    cur = con.execute("INSERT INTO runs(kind, branch, layer, commit_sha, status) "
                      "VALUES('adhoc', ?, ?, ?, 'queued')", (row["name"], layer, row["head_sha"]))
    if only:   # 指定項目：記在 log_path 欄之外沒有欄位，借 meta 存一份給執行端讀
        con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                    (f"adhoc_only:{cur.lastrowid}", only))
    return {"run_id": cur.lastrowid, "branch": row["name"], "layer": layer, "only": only}


def op_pause(con, now, actor, args):
    con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('paused','1')")
    return {"paused": True}


def op_resume(con, now, actor, args):
    branch = args.get("branch")
    if branch:
        row = _need_branch(con, branch)
        if row["state"] != State.HALTED.value:
            raise OpError("這條分支不是暫停狀態")
        state = _set_state(con, row, Event.RESUME, now)
        con.execute("UPDATE branches SET fail_count=0 WHERE name=?", (row["name"],))
        return {"branch": row["name"], "state": state}
    con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('paused','0')")
    return {"paused": False}


def op_resolve(con, now, actor, args):
    eid = args.get("id")
    if isinstance(eid, bool) or not isinstance(eid, int):
        raise OpError("id 要是整數")
    cur = con.execute("UPDATE events SET resolved_at=? WHERE id=? AND resolved_at IS NULL", (now, eid))
    if cur.rowcount == 0:
        raise OpError("沒有這個待辦，或已處理", 404)
    return {"id": eid, "resolved": True}


ACTIONS: Dict[str, Callable] = {
    "approve": op_approve, "revoke": op_revoke, "priority": op_priority,
    "set-parent": op_set_parent, "adhoc-run": op_adhoc_run,
    "pause": op_pause, "resume": op_resume, "resolve": op_resolve,
}


def apply(con, action: str, args: Dict[str, Any], actor: str, now: Optional[float] = None, pre_commit=None):
    """在主行程執行一個寫入命令；成功才寫 audit（失敗的也留一筆，方便追查）。"""
    now = time.time() if now is None else now
    fn = ACTIONS.get(action)
    if fn is None:
        raise OpError("不認得的操作", 404)
    if not isinstance(args, dict):
        raise OpError("參數必須是物件")
    con.execute("BEGIN IMMEDIATE")
    try:
        result = fn(con, now, actor, args, pre_commit) if action == "revoke" else fn(con, now, actor, args)
        _audit(con, now, actor, action, args)
        con.execute("COMMIT")
        return result
    except BaseException as e:
        con.execute("ROLLBACK")
        if isinstance(e, OpError):   # 被拒的操作也留紀錄（不在交易裡，獨立一筆）
            _audit(con, now, actor, action + ":rejected", dict(args, error=str(e)))
        raise


class _Pending:
    def __init__(self, action, args, actor):
        self.action, self.args, self.actor = action, args, actor
        self.done = threading.Event()
        self.result: Any = None
        self.error: Optional[OpError] = None

    def wait(self, timeout: float) -> bool:
        return self.done.wait(timeout)


class WriteQueue:
    """HTTP 執行緒把寫入命令放進來；主行程 `drain` 時才真的執行。"""

    def __init__(self, maxsize: int = 1000):
        self._q: "queue.Queue[_Pending]" = queue.Queue(maxsize)

    def submit(self, action: str, args: Dict[str, Any], actor: str) -> _Pending:
        p = _Pending(action, args, actor)
        try:
            self._q.put_nowait(p)
        except queue.Full:
            raise OpError("寫入佇列滿了，稍後再試", 503)
        return p

    def drain(self, con, now: Optional[float] = None, limit: int = 100,
              pre_commit: Optional[Callable[[str, Dict[str, Any], float], None]] = None) -> int:
        n = 0
        while n < limit:
            try:
                p = self._q.get_nowait()
            except queue.Empty:
                break
            try:
                p.result = apply(con, p.action, p.args, p.actor, now, pre_commit)
            except OpError as e:
                p.error = e
            except Exception as e:   # 不讓一個壞命令拖垮主迴圈
                p.error = OpError(f"內部錯誤：{type(e).__name__}", 500)
            p.done.set()
            n += 1
        return n
