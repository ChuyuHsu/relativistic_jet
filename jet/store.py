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


def set_baseline(con, main_sha, layer, failed: Iterable[str]):
    con.execute("INSERT OR REPLACE INTO baseline VALUES(?,?,?,?)",
                (main_sha, layer, json.dumps(sorted(failed)), now()))


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


def set_paused(con, paused: bool, by="?"):
    set_meta(con, "paused", "1" if paused else "0")
    audit(con, by, "pause" if paused else "resume_all")


def is_paused(con) -> bool:
    return get_meta(con, "paused", "0") == "1"
