"""通知（DESIGN 第 7 節，M1）。

有事需要人處理（退回、撞檔、暫停、排隊器自己出事）時，攢 batch_seconds 秒後一次打到設定的網址。
合入成功不逐條通知。送失敗就保留，下次補送（至少送一次；對方要能容忍重複，每個事件都帶 id）。

寫入規矩：標記「已通知」是資料庫寫入，只在主行程的 tick 裡做。網路請求放在背景執行緒，
不卡主迴圈，結果由下一輪 tick 收。
"""
import json
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

NOTIFY_KINDS = ("rejected", "conflict", "halted", "queue_error")   # 要人處理的（預設只送這些）
MACHINE_KINDS = ("pushed", "tested_green", "tested_red", "queued", "merged", "approved", "revoked",
                 "superseded", "parent_blocked")  # 給機器看的狀態變化
ALL_KINDS = NOTIFY_KINDS + MACHINE_KINDS
KIND_TEXT = {
    "rejected": "被退回", "conflict": "跟 main 撞檔", "halted": "暫停待人處理", "queue_error": "排隊器出狀況",
    "pushed": "有新提交", "tested_green": "自測綠", "tested_red": "自測紅", "queued": "進佇列",
    "merged": "已合入", "approved": "已放行", "revoked": "已撤回放行",
    "superseded": "內容變了、放行失效", "parent_blocked": "父分支擋住",
}
LOCAL_HOSTS = ("127.0.0.1", "localhost", "::1")
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
MAX_EVENTS_PER_POST = 50
MAX_DETAIL = 500
BACKOFF_BASE_S = 60
BACKOFF_MAX_S = 1800


class NotifyError(Exception):
    pass


def check_url(url: Optional[str]) -> Optional[str]:
    """只接受 http／https；其他（file:、ftp: …）一律拒絕。None 表示不通知。"""
    if url is None or url == "":
        return None
    u = urlparse(url)
    if u.scheme not in ("http", "https") or not u.netloc:
        raise NotifyError("notify.url 只能是 http 或 https 網址")
    return url


def check_bearer_url(url: Any) -> str:
    """要帶權杖的網址：只收 https；只有本機位址才可 http（權杖不明文上網）。"""
    if not isinstance(url, str) or not url:
        raise NotifyError("網址必填")
    u = urlparse(url)
    if (u.scheme == "https" and u.hostname) or (u.scheme == "http" and u.hostname in LOCAL_HOSTS):
        return url
    raise NotifyError("帶權杖的網址只能是 https（本機位址才可用 http）")


def read_token(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            tok = f.read().strip()
    except OSError as e:
        raise NotifyError(f"讀不到權杖檔：{e.strerror or e}")
    if not tok or "\n" in tok or "\r" in tok:
        raise NotifyError("權杖檔是空的或格式不對")
    return tok


def check_destinations(dests: Any) -> List[Dict[str, Any]]:
    """檢查並整理設定檔 notify.destinations；壞的丟 NotifyError。"""
    if dests is None:
        return []
    if not isinstance(dests, list):
        raise NotifyError("notify.destinations 必須是陣列")
    out, seen = [], set()
    for d in dests:
        if not isinstance(d, dict):
            raise NotifyError("notify.destinations 每一項必須是物件")
        name = d.get("name")
        if not isinstance(name, str) or not NAME_RE.match(name):
            raise NotifyError("通知目的地的 name 要是小寫英數、底線、減號（最長 32）")
        if name in seen:
            raise NotifyError(f"通知目的地名稱重複：{name}")
        seen.add(name)
        url, tf = d.get("url"), d.get("token_file")
        if tf is not None and (not isinstance(tf, str) or not tf):
            raise NotifyError("token_file 必須是檔案路徑字串")
        check_bearer_url(url) if tf else check_url(url)
        if not url:
            raise NotifyError(f"通知目的地 {name} 沒有 url")
        kinds = d.get("kinds", list(NOTIFY_KINDS))
        if kinds == "all":
            kinds = list(ALL_KINDS)
        if not isinstance(kinds, list) or not kinds or not all(k in ALL_KINDS for k in kinds):
            raise NotifyError("kinds 必須是 " + "、".join(ALL_KINDS) + " 的非空陣列，或 \"all\"")
        bs = d.get("batch_seconds", 60)
        if isinstance(bs, bool) or not isinstance(bs, (int, float)) or bs < 0:
            raise NotifyError("batch_seconds 必須是 ≥ 0 的數字")
        out.append({"name": name, "url": url, "token_file": tf, "kinds": list(kinds), "batch_s": bs})
    return out


def post_json(url: str, payload: Dict[str, Any], timeout: float = 10.0,
              headers: Optional[Dict[str, str]] = None) -> None:
    """打一次；2xx 才算成功，其餘丟 NotifyError（訊息不含權杖）。"""
    check_url(url)
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    hdrs = {"Content-Type": "application/json; charset=utf-8", "User-Agent": "relativistic-jet"}
    hdrs.update(headers or {})
    req = urllib.request.Request(url, data=data, method="POST", headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if not 200 <= r.status < 300:
                raise NotifyError(f"對方回 {r.status}")
    except urllib.error.HTTPError as e:
        raise NotifyError(f"對方回 {e.code}")
    except (urllib.error.URLError, OSError) as e:
        raise NotifyError(f"送不出去：{getattr(e, 'reason', e)}")


def build_payload(events: List[Dict[str, Any]], now: float, destination: Optional[str] = None) -> Dict[str, Any]:
    lines = []
    for e in events:
        who = e.get("branch") or "系統"
        lines.append(f"{who}：{KIND_TEXT.get(e['kind'], e['kind'])}" +
                     (f"（{e['detail']}）" if e.get("detail") else ""))
    out = {
        "schema": 1,
        "sent_at": now,
        "count": len(events),
        "text": f"合併佇列有 {len(events)} 件事要看：\n" + "\n".join(lines),
        "events": [{k: v for k, v in {
            "id": e.get("eid", e["id"]), "kind": e["kind"], "branch": e.get("branch"), "commit": e.get("commit"),
            "detail": e.get("detail"), "at": e["at"],
            "from": e.get("from"), "to": e.get("to"), "failed_items": e.get("failed_items"),
            "approved_sha": e.get("approved_sha"), "retest": e.get("retest"),
        }.items() if v is not None or k in ("branch", "detail")} for e in events],
    }
    if destination:
        out["destination"] = destination
    return out


def _enrich(con, r) -> Dict[str, Any]:
    """主行程裡補欄位：commit、狀態事件的 from／to、被退回事件的紅項清單。"""
    e = dict(r)
    iid = con.execute("SELECT value FROM meta WHERE key='instance_id'").fetchone()
    if iid:
        e["eid"] = f"{iid[0]}:{e['id']}"       # 跨資料庫重置仍唯一
    e["commit"] = e.pop("commit_sha", None)
    if e.get("detail"):
        e["detail"] = str(e["detail"])[:MAX_DETAIL]
    if e["kind"] in MACHINE_KINDS and e.get("detail"):
        try:
            d = json.loads(e["detail"])
        except ValueError:
            d = {}
        if isinstance(d, dict):
            e["from"], e["to"] = d.get("from"), d.get("to")
            e["detail"] = str(d.get("reason") or d.get("merged_sha") or d.get("by") or "")[:MAX_DETAIL] or None
    if e["kind"] in ("rejected", "tested_red") and e.get("branch"):
        run = con.execute("SELECT id FROM runs WHERE branch=? AND status IN ('fail','timeout') ORDER BY id DESC LIMIT 1",
                          (e["branch"],)).fetchone()
        if run is None:   # 批測紅：紅項記在這條分支所屬批次的測試上
            run = con.execute(
                "SELECT r.id FROM runs r JOIN batch_members m ON m.batch_id=r.batch_id "
                "WHERE m.branch=? AND r.status IN ('fail','timeout') ORDER BY r.id DESC LIMIT 1",
                (e["branch"],)).fetchone()
        if run is not None:
            e["failed_items"] = [dict(x) for x in con.execute(
                "SELECT item_id, status, summary FROM run_items WHERE run_id=? AND status IN ('fail','timeout') "
                "ORDER BY item_id LIMIT 50", (run[0],))]
    # 人看的 detail 一律帶提交與紅項（只吃 id／kind／branch／detail 的對方也看得到）；結構化欄位另外有
    parts = []
    if e.get("commit"):
        parts.append(str(e["commit"])[:8])
    if e.get("detail"):
        parts.append(e["detail"])
    if e.get("failed_items"):
        parts.append("紅項：" + "、".join(x["item_id"] for x in e["failed_items"][:10]))
    e["detail"] = "｜".join(parts)[:MAX_DETAIL] if parts else e.get("detail")
    return e


class _Dest:
    def __init__(self, name, url, token_file, kinds, batch_s, replay):
        self.name, self.url, self.token_file = name, url, token_file
        self.kinds, self.batch_s, self.replay = list(kinds), batch_s, replay
        self.thread: Optional[threading.Thread] = None
        self.inflight: List[int] = []
        self.result: Optional[str] = None   # None＝還沒結果；"" 成功；其他字串是錯誤
        self.done = False
        self.fails = 0
        self.next_try = 0.0
        self.last_error: Optional[str] = None


class Notifier:
    """主行程每輪 tick 呼叫一次 `tick(con)`。可有多個目的地，各自有進度（meta 的 notify_cursor:<名字>）：
    事件依 id 由小到大送，同一目的地同時只有一批在送，成功才前進；失敗保留重送（退避），不丟、不亂序。"""

    def __init__(self, url: Optional[str] = None, batch_s: int = 60, timeout: float = 10.0,
                 poster: Optional[Callable[..., None]] = None,
                 destinations: Optional[List[Dict[str, Any]]] = None):
        self.timeout = timeout
        self._post = poster or post_json
        self.dests: List[_Dest] = []
        if url:
            self.dests.append(_Dest("default", check_url(url), None, NOTIFY_KINDS, max(0, int(batch_s)), True))
        for d in destinations or []:
            self.dests.append(_Dest(d["name"], d["url"], d.get("token_file"), d["kinds"],
                                    d.get("batch_s", 60), False))

    @property
    def url(self):
        return self.dests[0].url if self.dests else None

    @property
    def last_error(self) -> Optional[str]:
        errs = [f"{d.name}：{d.last_error}" for d in self.dests if d.last_error]
        return "；".join(errs) or None

    @property
    def wants_machine_events(self) -> bool:
        return any(k in MACHINE_KINDS for d in self.dests for k in d.kinds)

    @staticmethod
    def _cursor_key(d) -> str:
        return f"notify_cursor:{d.name}"

    def _cursor(self, con, d) -> int:
        row = con.execute("SELECT value FROM meta WHERE key=?", (self._cursor_key(d),)).fetchone()
        if row is not None:
            return int(row[0])
        # 新目的地只收「從現在起」的事件，不重送歷史；舊式單一網址照舊補送還沒通知過的
        start = 0 if d.replay else (con.execute("SELECT COALESCE(max(id),0) FROM events").fetchone()[0])
        con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (self._cursor_key(d), str(start)))
        return start

    def _collect_result(self, con, d, now: float) -> None:
        if d.thread is None or not d.done:
            return
        d.thread.join()
        d.thread = None
        ids, err = d.inflight, d.result
        d.inflight, d.result, d.done = [], None, False
        if err:
            d.fails += 1
            d.last_error = err
            d.next_try = now + min(BACKOFF_MAX_S, BACKOFF_BASE_S * (2 ** (d.fails - 1)))
            return
        d.fails, d.last_error = 0, None
        marks = ",".join("?" * len(ids))
        con.execute(f"UPDATE events SET notified_at=? WHERE id IN ({marks}) AND notified_at IS NULL", (now, *ids))
        con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (self._cursor_key(d), str(max(ids))))

    def _run(self, d, payload, headers) -> None:
        try:
            if headers:
                self._post(d.url, payload, self.timeout, headers)
            else:
                self._post(d.url, payload, self.timeout)
            d.result = ""
        except Exception as e:   # 任何錯都只是「這次沒送成」，不能弄壞主行程
            d.result = str(e) or type(e).__name__
        d.done = True

    def tick(self, con, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        for d in self.dests:
            self._tick_one(con, d, now)

    def _tick_one(self, con, d, now: float) -> None:
        self._collect_result(con, d, now)
        if d.thread is not None or now < d.next_try:
            return
        cur = self._cursor(con, d)
        marks = ",".join("?" * len(d.kinds))
        mm = ",".join("?" * len(MACHINE_KINDS))
        extra = " AND notified_at IS NULL" if d.replay else ""
        rows = con.execute(
            f"SELECT id, kind, branch, detail, at, commit_sha, approved_sha, retest FROM events WHERE id>? AND kind IN ({marks}) "
            f"AND (resolved_at IS NULL OR kind IN ({mm})){extra} ORDER BY id LIMIT ?",
            (cur, *d.kinds, *MACHINE_KINDS, MAX_EVENTS_PER_POST)).fetchall()
        if not rows:
            return
        if now - min(r["at"] for r in rows) < d.batch_s:   # 攢批：最舊的事件滿 batch_s 才送
            return
        headers = None
        if d.token_file:
            try:
                headers = {"Authorization": "Bearer " + read_token(d.token_file)}
            except NotifyError as e:
                d.fails += 1
                d.last_error = str(e)
                d.next_try = now + min(BACKOFF_MAX_S, BACKOFF_BASE_S * (2 ** (d.fails - 1)))
                return
        events = [_enrich(con, r) for r in rows]
        d.inflight = [e["id"] for e in events]
        d.done = False
        d.result = None
        d.thread = threading.Thread(target=self._run,
                                    args=(d, build_payload(events, now, d.name if d.token_file or not d.replay else None),
                                          headers), name=f"jet-notify-{d.name}", daemon=True)
        d.thread.start()
