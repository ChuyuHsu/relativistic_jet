"""通知（DESIGN 第 7 節，M1）。

有事需要人處理（退回、撞檔、暫停、排隊器自己出事）時，攢 batch_seconds 秒後一次打到設定的網址。
合入成功不逐條通知。送失敗就保留，下次補送（至少送一次；對方要能容忍重複，每個事件都帶 id）。

寫入規矩：標記「已通知」是資料庫寫入，只在主行程的 tick 裡做。網路請求放在背景執行緒，
不卡主迴圈，結果由下一輪 tick 收。
"""
import json
import logging
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

NOTIFY_KINDS = ("rejected", "conflict", "halted", "queue_error")   # 要人處理的（預設只送這些）
MACHINE_KINDS = ("pushed", "tested_green", "tested_red", "queued", "merged", "approved", "revoked",
                 "superseded", "parent_blocked", "drop_checked", "selftest_started", "batch_started",
                 "workflow_status", "loop_red", "loop_green", "loop_culprit", "loop_triage", "loop_perf", "state_snapshot", "full_result", "idle_blocked")  # 給機器看的狀態變化
ALL_KINDS = NOTIFY_KINDS + MACHINE_KINDS
KIND_TEXT = {
    "rejected": "被退回", "conflict": "跟 main 撞檔", "halted": "暫停待人處理", "queue_error": "排隊器出狀況",
    "pushed": "有新提交", "tested_green": "自測綠", "tested_red": "自測紅", "queued": "進佇列",
    "merged": "已合入", "approved": "已放行", "revoked": "已撤回放行",
    "superseded": "內容變了、放行失效", "parent_blocked": "父分支擋住", "drop_checked": "放棄查核結果", "selftest_started": "開始自測", "batch_started": "開始批測",
    "workflow_status": "工作流狀態", "loop_red": "背景測試發現主線紅", "loop_green": "背景測試主線綠", "loop_culprit": "背景測試找到兇手", "loop_triage": "背景測試請看板先看代碼找嫌疑", "loop_perf": "背景測試性能量測", "idle_blocked": "開發機沒有可測的卡（被擋住或沒放行）", "state_snapshot": "排隊器現況快照", "full_result": "完整層結果",
}
LOOP_KINDS = ("workflow_status", "loop_red", "loop_green", "loop_culprit", "loop_triage", "loop_perf")
LOOP_FIELDS = ("mode", "loop", "main_sha", "culprit_commit", "culprit_branch", "hotfix_id", "failed_items", "green_sha", "commits", "deadline_at", "perf")
LOCAL_HOSTS = ("127.0.0.1", "localhost", "::1")
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
MAX_EVENTS_PER_POST = 10          # 每次請求最多幾筆（可用 max_events 調）；對方處理慢時整批會逾時，所以預設小
DEFAULT_TIMEOUT_S = 30.0
ALERT_AFTER_S = 600               # 連續失敗超過這麼久：發 queue_error、寫 log
SPLIT_RETRY_S = 5                 # 對半切後馬上再試，不用等指數退避
log = logging.getLogger(__name__)
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
        me = d.get("max_events", MAX_EVENTS_PER_POST)
        if isinstance(me, bool) or not isinstance(me, int) or not 1 <= me <= 200:
            raise NotifyError("max_events 必須是 1～200 的整數")
        to = d.get("timeout_s", DEFAULT_TIMEOUT_S)
        if isinstance(to, bool) or not isinstance(to, (int, float)) or to <= 0:
            raise NotifyError("timeout_s 必須 > 0")
        out.append({"name": name, "url": url, "token_file": tf, "kinds": list(kinds), "batch_s": bs,
                    "max_events": me, "timeout_s": to})
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
            "run_id": e.get("run_id"), "cores": e.get("cores"), "batch_id": e.get("batch_id"),
            "pos": e.get("pos"), "batch_size": e.get("batch_size"),
            "card": e.get("card"), "target": e.get("target"), "unmerged": e.get("unmerged"),
            "covered": e.get("covered"), "missing_count": e.get("missing_count"),
            "missing_files": e.get("missing_files"), "dropping_at": e.get("dropping_at"),
            "mode": e.get("mode"), "loop": e.get("loop"), "main_sha": e.get("main_sha"),
            "culprit_commit": e.get("culprit_commit"), "culprit_branch": e.get("culprit_branch"),
            "hotfix_id": e.get("hotfix_id"), "green_sha": e.get("green_sha"), "commits": e.get("commits"),
            "deadline_at": e.get("deadline_at"), "perf": e.get("perf"), "branches": e.get("branches"),
            "started_at": e.get("started_at"), "finished_at": e.get("finished_at"), "total": e.get("total"),
            "red": e.get("red"), "timeouts": e.get("timeouts"),
            "pending": e.get("pending"), "approved_pending": e.get("approved_pending"), "blockers": e.get("blockers"),
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
    raw_detail = e.get("detail")
    if e.get("detail"):
        e["detail"] = str(e["detail"])[:MAX_DETAIL]
    if e["kind"] in MACHINE_KINDS and e.get("detail"):
        try:
            d = json.loads(str(raw_detail))           # 用完整內容解析：結構化欄位不能被截短的 detail 弄壞（檔案清單很長時）
        except ValueError:
            d = {}
        if isinstance(d, dict):
            e["from"], e["to"] = d.get("from"), d.get("to")
            if e["kind"] in ("selftest_started", "batch_started"):
                for k in ("run_id", "cores", "batch_id", "pos", "batch_size"):
                    e[k] = d.get(k)
                e["to"] = "selftesting" if e["kind"] == "selftest_started" else "batching"   # 看板的 phase 靠它顯示「自測中／批測中」
                d = {"reason": (f"run #{d.get('run_id')} 核 {d.get('cores')}" if e["kind"] == "selftest_started"
                                else f"批 #{d.get('batch_id')} 第 {d.get('pos')}/{d.get('batch_size')} 條")}
            if e["kind"] in LOOP_KINDS:
                for k in LOOP_FIELDS:
                    if k in d:
                        e[k] = d[k]
                d = {"reason": d.get("detail")}
            if e["kind"] == "full_result":
                for k in ("main_sha", "started_at", "finished_at", "total", "red", "timeouts"):
                    e[k] = d.get(k)
                d = {"reason": f"{d.get('total')} 項，紅 {len(d.get('red') or [])}、逾時 {len(d.get('timeouts') or [])}"}
            if e["kind"] == "idle_blocked":
                for k in ("pending", "approved_pending", "blockers"):
                    e[k] = d.get(k)
                d = {"reason": d.get("detail")}
            if e["kind"] == "state_snapshot":
                e["branches"] = d.get("branches") or []
                d = {"reason": f"{len(e['branches'])} 條分支"}
            if e["kind"] == "drop_checked":
                for k in ("card", "target", "unmerged", "covered", "missing_count", "missing_files", "dropping_at"):
                    e[k] = d.get(k)
                d = {"reason": ("已全部包含於 " if d.get("covered") else "有 %s 個檔不在 " % d.get("missing_count")) + str(d.get("target"))}
            e["detail"] = str(d.get("reason") or d.get("merged_sha") or d.get("by") or "")[:MAX_DETAIL] or None
    if e["kind"] in ("rejected", "tested_red", "halted") and e.get("branch"):
        run = con.execute("SELECT id FROM runs WHERE branch=? AND kind!='recheck' AND status IN ('fail','timeout') ORDER BY id DESC LIMIT 1",
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
            for i, x in enumerate(e["failed_items"]):      # 失敗摘要：前 10 項給足，其餘縮短（事件整包不要太大）
                if x.get("summary"):
                    x["summary"] = x["summary"][:1200 if i < 10 else 200]
    # 人看的 detail 一律帶提交與紅項（只吃 id／kind／branch／detail 的對方也看得到）；結構化欄位另外有
    parts = []
    if e.get("commit"):
        parts.append(str(e["commit"])[:8])
    if e.get("detail"):
        parts.append(e["detail"])
    if e.get("failed_items"):
        parts.append("紅項：" + "、".join((x["item_id"] if isinstance(x, dict) and "item_id" in x else x.get("id", "?") if isinstance(x, dict) else str(x)) for x in e["failed_items"][:10]))
    e["detail"] = "｜".join(parts)[:MAX_DETAIL] if parts else e.get("detail")
    return e


class _Dest:
    def __init__(self, name, url, token_file, kinds, batch_s, replay, max_events=MAX_EVENTS_PER_POST,
                 timeout_s=DEFAULT_TIMEOUT_S):
        self.name, self.url, self.token_file = name, url, token_file
        self.kinds, self.batch_s, self.replay = list(kinds), batch_s, replay
        self.max_events, self.timeout_s = max_events, timeout_s
        self.limit = max_events             # 目前每批筆數：失敗就對半切，成功再慢慢放大
        self.suspect: Optional[int] = None  # 單筆也送不出去的事件：下一筆送得出去才確定是它的問題
        self.fail_since: Optional[float] = None
        self.alerted = False
        self.poison: Optional[dict] = None  # 最近一次被跳過的事件
        self.thread: Optional[threading.Thread] = None
        self.inflight: List[int] = []
        self.result: Optional[str] = None   # None＝還沒結果；"" 成功；其他字串是錯誤
        self.done = False
        self.fails = 0
        self.next_try = 0.0
        self.last_error: Optional[str] = None
        self.last_code: Optional[int] = None


class Notifier:
    """主行程每輪 tick 呼叫一次 `tick(con)`。可有多個目的地，各自有進度（meta 的 notify_cursor:<名字>）：
    事件依 id 由小到大送，同一目的地同時只有一批在送，成功才前進；失敗保留重送（退避），不丟、不亂序。"""

    def __init__(self, url: Optional[str] = None, batch_s: int = 60, timeout: float = DEFAULT_TIMEOUT_S,
                 poster: Optional[Callable[..., None]] = None,
                 destinations: Optional[List[Dict[str, Any]]] = None):
        self.timeout = timeout
        self._post = poster or post_json
        self.dests: List[_Dest] = []
        if url:
            self.dests.append(_Dest("default", check_url(url), None, NOTIFY_KINDS, max(0, int(batch_s)), True,
                                    MAX_EVENTS_PER_POST, timeout))
        for d in destinations or []:
            self.dests.append(_Dest(d["name"], d["url"], d.get("token_file"), d["kinds"],
                                    d.get("batch_s", 60), False, d.get("max_events", MAX_EVENTS_PER_POST),
                                    d.get("timeout_s", timeout)))

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
            if d.fail_since is None:
                d.fail_since = now
            if len(ids) > 1:                                   # 一批送不出去：對半切再試（可能是某幾筆讓對方處理太久）
                d.limit = max(1, len(ids) // 2)
                d.suspect = None
                d.next_try = now + SPLIT_RETRY_S
            else:
                if d.suspect is not None and d.suspect not in ids:
                    d.suspect = None                           # 拿「後面那一筆」來比對也失敗：是對方整個壞了，不是毒藥事件
                elif d.suspect is None and ids:
                    d.suspect = ids[0]                         # 單筆失敗：下一輪改送它後面的一筆來比對
                d.next_try = now + min(BACKOFF_MAX_S, BACKOFF_BASE_S * (2 ** (min(d.fails, 12) - 1)))
            if not d.alerted and now - d.fail_since >= ALERT_AFTER_S:
                d.alerted = True
                msg = f"通知 {d.name} 已連續失敗 {(now - d.fail_since) / 60:.0f} 分鐘：{err[:200]}"
                log.error(msg)
                con.execute("INSERT INTO events(kind,detail,at) VALUES('queue_error',?,?)", (msg, now))
        else:
            if d.suspect is not None and ids and min(ids) > d.suspect:   # 後面的送得出去、它卻不行：它是毒藥，跳過
                d.poison = {"id": d.suspect, "at": now}
                log.error("通知 %s：事件 %s 單獨送一直失敗而後面的送得出去，跳過它", d.name, d.suspect)
                con.execute("INSERT INTO events(kind,detail,at,notified_at,resolved_at) VALUES('queue_error',?,?,?,?)",
                            (f"通知 {d.name} 跳過了送不出去的事件 #{d.suspect}", now, now, now))
            d.suspect = None
            d.fails, d.last_error, d.fail_since, d.alerted = 0, None, None, False
            d.limit = min(d.max_events, max(d.limit * 2, 1))
            marks = ",".join("?" * len(ids))
            con.execute(f"UPDATE events SET notified_at=? WHERE id IN ({marks}) AND notified_at IS NULL", (now, *ids))
            con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (self._cursor_key(d), str(max(ids))))
        con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",     # 每次送出都記回應碼（狀態頁看得到）
                    (f"notify_last:{d.name}", json.dumps(
                        {"at": now, "code": d.last_code, "events": len(ids), "error": err or None,
                         "fails": d.fails, "limit": d.limit, "since": d.fail_since,
                         "suspect": d.suspect, "skipped": d.poison}, ensure_ascii=False)))

    def _run(self, d, payload, headers) -> None:
        try:
            if headers:
                self._post(d.url, payload, d.timeout_s, headers)
            else:
                self._post(d.url, payload, d.timeout_s)
            d.result = ""
            d.last_code = 200
        except Exception as e:   # 任何錯都只是「這次沒送成」，不能弄壞主行程
            d.result = str(e) or type(e).__name__
            m = re.search(r"對方回 (\d+)", d.result)
            d.last_code = int(m.group(1)) if m else None
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
        def fetch(after):
            return con.execute(
                f"SELECT id, kind, branch, detail, at, commit_sha, approved_sha, retest FROM events WHERE id>? AND kind IN ({marks}) "
                f"AND (resolved_at IS NULL OR kind IN ({mm})){extra} ORDER BY id LIMIT ?",
                (after, *d.kinds, *MACHINE_KINDS, d.limit)).fetchall()
        rows = []
        if d.suspect is not None:
            rows = fetch(d.suspect)[:1]                        # 比對用：只送嫌疑事件後面的一筆
            if not rows:
                d.suspect = None                               # 後面沒有別的事件可比對：直接重試它
        if d.suspect is None:
            rows = fetch(cur)
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
