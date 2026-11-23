"""資料存取：所有對 SQLite 的讀寫都走這裡（api／cli／scheduler 共用）。
每個寫入函式一個交易；人或代理的寫入帶 actor 並寫 audit。"""
import json
import time
from typing import Iterable, List, Optional

from .states import Event, State, transition

now = time.time  # 測試可替換：store.now = lambda: 123.0


def _row(con, sql, *a):
    return con.execute(sql, a).fetchone()


# ---- 讀 ----
def get_branch(con, name):
    return _row(con, "SELECT * FROM branches WHERE name=?", name)


def list_branches(con, state: Optional[str] = None):
    if state:
        return con.execute("SELECT * FROM branches WHERE state=? ORDER BY name", (state,)).fetchall()
    return con.execute("SELECT * FROM branches ORDER BY name").fetchall()


def children(con, name):
    return con.execute("SELECT * FROM branches WHERE parent=?", (name,)).fetchall()


def is_approved(con, name, content_id) -> bool:
    return _row(con, "SELECT 1 FROM approvals WHERE branch=? AND content_id=? AND revoked_at IS NULL",
                name, content_id) is not None


def get_meta(con, key, default=None):
    r = _row(con, "SELECT value FROM meta WHERE key=?", key)
    return r[0] if r else default


def set_meta(con, key, value):
    con.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)))


def get_baseline(con, main_sha, layer) -> Optional[set]:
    r = _row(con, "SELECT failed_items FROM baseline WHERE main_sha=? AND layer=?", main_sha, layer)
    return set(json.loads(r[0])) if r else None


def set_baseline(con, main_sha, layer, failed: Iterable[str], copied: bool = False):
    con.execute("INSERT OR REPLACE INTO baseline(main_sha,layer,failed_items,recorded_at,copied) VALUES(?,?,?,?,?)",
                (main_sha, layer, json.dumps(sorted(failed)), now(), 1 if copied else 0))


def _history(con, layer, n) -> List[set]:
    rows = con.execute("SELECT failed_items FROM baseline WHERE layer=? AND copied=0 "
                       "ORDER BY recorded_at DESC, rowid DESC LIMIT ?", (layer, int(n))).fetchall()
    return [set(json.loads(r[0])) for r in rows]


def known_red(con, layer, n=5) -> set:
    """已知紅：最近 n 次 main 完整結果裡，任何一次紅過的項目（抖動項也算，不計成新紅）。"""
    out = set()
    for h in _history(con, layer, n):
        out |= h
    return out


def flaky_items(con, layer="gate", n=5) -> set:
    """抖動項：最近 n 次 main 結果裡，有的紅、有的綠的項目（至少要有兩次量測）。"""
    h = _history(con, layer, n)
    if len(h) < 2:
        return set()
    return set.union(*h) - set.intersection(*h)


# ---- 狀態 ----
# 哪些狀態轉移要對外報：事件種類 → webhook 事件名
_EVENT_KIND = {Event.PUSHED: "pushed", Event.SELFTEST_PASS: "tested_green", Event.SELFTEST_FAIL: "tested_red",
               Event.READY: "queued", Event.BATCH_MERGED: "merged", Event.DOCS_MERGED: "merged"}


def apply_event(con, name, event: Event, **fields) -> State:
    """依狀態機轉移並寫回，fields 一併更新（如 reject_reason）。"""
    r = get_branch(con, name)
    new = transition(r["state"], event)
    sets = {"state": new.value, "updated_at": now(), **fields}
    con.execute(f"UPDATE branches SET {','.join(k+'=?' for k in sets)} WHERE name=?",
                (*sets.values(), name))
    kind = _EVENT_KIND.get(event)
    if kind and new.value != r["state"]:
        cur = get_branch(con, name)   # 推新提交時 head_sha 已經在 fields 裡換掉了，要讀更新後的
        emit(con, kind, name, {"from": r["state"], "to": new.value, "reason": fields.get("reject_reason"),
                               "merged_sha": cur["merged_sha"] if kind == "merged" else None}, cur["head_sha"])
    return new


def add_event(con, kind, branch=None, detail=None):
    sha = None
    if branch:
        b = get_branch(con, branch)
        sha = b["head_sha"] if b else None
    con.execute("INSERT INTO events(kind,branch,detail,at,commit_sha) VALUES(?,?,?,?,?)",
                (kind, branch, detail, now(), sha))


MACHINE_KINDS = ("pushed", "tested_green", "tested_red", "queued", "merged", "approved", "revoked")   # 給機器看的事件：不進人的待辦（寫入時就標成已處理）


def emit(con, kind, branch, detail: dict, commit_sha=None):
    """寫一筆給 webhook 的狀態事件；只有設定了要送這類事件時（meta emit_machine_events=1）才寫。"""
    if get_meta(con, "emit_machine_events", "0") != "1":
        return
    t = now()
    con.execute("INSERT INTO events(kind,branch,detail,at,resolved_at,commit_sha) VALUES(?,?,?,?,?,?)",
                (kind, branch, json.dumps(detail, ensure_ascii=False, sort_keys=True), t, t, commit_sha))


def add_info_event(con, kind, branch=None, detail=None):
    """資訊型事件（tracked／untracked）：記錄用，不算待辦、不通知。"""
    t = now()
    con.execute("INSERT INTO events(kind,branch,detail,at,notified_at,resolved_at) VALUES(?,?,?,?,?,?)",
                (kind, branch, detail, t, t, t))


def audit(con, actor, action, args=None):
    con.execute("INSERT INTO audit(at,actor,action,args) VALUES(?,?,?,?)",
                (now(), actor, action, json.dumps(args, ensure_ascii=False) if args is not None else None))


# ---- 人工操作（api／cli 呼叫）----
class NoSuchBranch(Exception):
    pass


def _must(con, name):
    r = get_branch(con, name)
    if r is None:
        raise NoSuchBranch(name)
    return r


def approve(con, name, by="?"):
    """放行：記在這條分支「目前這個內容」上。"""
    r = _must(con, name)
    con.execute("INSERT INTO approvals(branch,content_id,by,at) VALUES(?,?,?,?)",
                (name, r["content_id"], by, now()))
    audit(con, by, "approve", {"branch": name, "content_id": r["content_id"]})
    emit(con, "approved", name, {"by": by, "content_id": r["content_id"]}, r["head_sha"])


def revoke(con, name, by="?"):
    r = _must(con, name)
    con.execute("UPDATE approvals SET revoked_at=? WHERE branch=? AND revoked_at IS NULL", (now(), name))
    audit(con, by, "revoke", {"branch": name})
    emit(con, "revoked", name, {"by": by}, r["head_sha"])


def set_priority(con, name, prio: int, by="?"):
    _must(con, name)
    con.execute("UPDATE branches SET prio_override=? WHERE name=?", (int(prio), name))
    audit(con, by, "priority", {"branch": name, "prio": int(prio)})


def set_parent(con, name, parent: Optional[str], by="?"):
    """登記父分支（None＝取消）。擋自己、擋成環。"""
    _must(con, name)
    if parent is not None:
        if _must(con, parent)["state"] == State.MERGED.value:
            parent = None          # 父已合入＝普通分支
    if parent is not None:
        p = parent
        while p is not None:
            if p == name:
                raise ValueError("父分支不能成環")
            p = get_branch(con, p)["parent"]
    con.execute("UPDATE branches SET parent=? WHERE name=?", (parent, name))
    if get_branch(con, name)["state"] in (State.SELFTESTING.value, State.SELFTEST_GREEN.value,
                                          State.QUEUED.value):
        apply_event(con, name, Event.PARENT_CHANGED)
    audit(con, by, "set_parent", {"branch": name, "parent": parent})


def resume_branch(con, name, by="?"):
    _must(con, name)
    apply_event(con, name, Event.RESUME, fail_count=0)
    audit(con, by, "resume", {"branch": name})


def set_paused(con, paused: bool, by="?", reason: Optional[str] = None):
    set_meta(con, "paused", "1" if paused else "0")
    set_meta(con, "pause_reason", (reason or ("人工暫停（%s）" % by)) if paused else "")
    if paused:
        set_meta(con, "pause_by", by)
    audit(con, by, "pause" if paused else "resume_all")


def is_paused(con) -> bool:
    return get_meta(con, "paused", "0") == "1"


# ---- 登記制（git.scope = "tracked"）----
class TrackError(Exception):
    """登記／取消登記的條件不符（介面回 400／409）。"""


def is_tracked(con, branch) -> bool:
    return _row(con, "SELECT 1 FROM tracked WHERE branch=?", branch) is not None


def tracked_names(con) -> set:
    return {r[0] for r in con.execute("SELECT branch FROM tracked")}


def list_tracked(con) -> List[dict]:
    out = []
    for r in con.execute("SELECT t.branch, t.parent, t.at, b.state FROM tracked t "
                         "LEFT JOIN branches b ON b.name=t.branch ORDER BY t.at, t.branch"):
        out.append({"branch": r["branch"], "parent": r["parent"], "at": r["at"], "state": r["state"]})
    return out


def track(con, branch: str, parent: Optional[str] = None, by="?") -> dict:
    """登記一條分支（冪等）。可帶父分支：父必須也登記過，或已合入。回 {created, branch, parent}。"""
    if not isinstance(branch, str) or not branch.strip():
        raise TrackError("branch 不能空")
    if parent is not None:
        if parent == branch:
            raise TrackError("父分支不能是自己")
        merged = _row(con, "SELECT 1 FROM branches WHERE name=? AND state='merged'", parent)
        if not is_tracked(con, parent) and merged is None:
            raise TrackError(f"父分支 {parent} 沒有登記（先登記父分支）")
        if merged is not None:
            parent = None                       # 父已合入＝普通分支
        p = parent
        while p is not None:                    # 登記表裡的父鏈不能成環
            if p == branch:
                raise TrackError("父分支不能成環")
            r = _row(con, "SELECT parent FROM tracked WHERE branch=?", p)
            p = r["parent"] if r else None
    existing = _row(con, "SELECT parent FROM tracked WHERE branch=?", branch)
    if existing is None:
        con.execute("INSERT INTO tracked(branch,parent,parent_applied,at) VALUES(?,?,0,?)", (branch, parent, now()))
        add_info_event(con, "tracked", branch, f"parent={parent}" if parent else None)
        audit(con, by, "track", {"branch": branch, "parent": parent})
        return {"created": True, "branch": branch, "parent": parent}
    if parent is not None and parent != existing["parent"]:    # 重複登記帶了不同的父：更新
        con.execute("UPDATE tracked SET parent=?, parent_applied=0 WHERE branch=?", (parent, branch))
        audit(con, by, "track", {"branch": branch, "parent": parent})
    return {"created": False, "branch": branch, "parent": parent or existing["parent"]}


def untrack(con, branch: str, by="?") -> bool:
    """取消登記（冪等）。回傳本來有沒有登記。正在跑的自測、排隊、批測由排隊器下一輪處理掉。"""
    if _row(con, "SELECT 1 FROM tracked WHERE parent=?", branch) is not None:
        raise TrackError(f"{branch} 還有登記著的子分支，先取消子分支")
    existed = is_tracked(con, branch)
    if existed:
        con.execute("DELETE FROM tracked WHERE branch=?", (branch,))
        add_info_event(con, "untracked", branch)
        audit(con, by, "untrack", {"branch": branch})
    return existed
