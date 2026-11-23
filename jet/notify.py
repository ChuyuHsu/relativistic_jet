"""通知（DESIGN 第 7 節，M1）。

有事需要人處理（退回、撞檔、暫停、排隊器自己出事）時，攢 batch_seconds 秒後一次打到設定的網址。
合入成功不逐條通知。送失敗就保留，下次補送（至少送一次；對方要能容忍重複，每個事件都帶 id）。

寫入規矩：標記「已通知」是資料庫寫入，只在主行程的 tick 裡做。網路請求放在背景執行緒，
不卡主迴圈，結果由下一輪 tick 收。
"""
import json
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

NOTIFY_KINDS = ("rejected", "conflict", "halted", "queue_error")
KIND_TEXT = {
    "rejected": "被退回", "conflict": "跟 main 撞檔", "halted": "暫停待人處理", "queue_error": "排隊器出狀況",
}
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


def post_json(url: str, payload: Dict[str, Any], timeout: float = 10.0) -> None:
    """打一次；2xx 才算成功，其餘丟 NotifyError。"""
    check_url(url)
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if not 200 <= r.status < 300:
                raise NotifyError(f"對方回 {r.status}")
    except urllib.error.HTTPError as e:
        raise NotifyError(f"對方回 {e.code}")
    except (urllib.error.URLError, OSError) as e:
        raise NotifyError(f"送不出去：{getattr(e, 'reason', e)}")


def build_payload(events: List[Dict[str, Any]], now: float) -> Dict[str, Any]:
    lines = []
    for e in events:
        who = e.get("branch") or "系統"
        lines.append(f"{who}：{KIND_TEXT.get(e['kind'], e['kind'])}" +
                     (f"（{e['detail']}）" if e.get("detail") else ""))
    return {
        "schema": 1,
        "sent_at": now,
        "count": len(events),
        "text": f"合併佇列有 {len(events)} 件事要看：\n" + "\n".join(lines),
        "events": [{"id": e["id"], "kind": e["kind"], "branch": e.get("branch"),
                    "detail": e.get("detail"), "at": e["at"]} for e in events],
    }


class Notifier:
    """主行程每輪 tick 呼叫一次 `tick(con)`。"""

    def __init__(self, url: Optional[str], batch_s: int = 180, timeout: float = 10.0,
                 poster: Optional[Callable[[str, Dict[str, Any], float], None]] = None):
        self.url = check_url(url)
        self.batch_s = max(0, int(batch_s))
        self.timeout = timeout
        self._post = poster or post_json
        self._thread: Optional[threading.Thread] = None
        self._inflight_ids: List[int] = []
        self._result: Optional[Optional[str]] = None   # None＝還沒結果；"" 成功；其他字串是錯誤
        self._done = False
        self._fails = 0
        self._next_try = 0.0
        self.last_error: Optional[str] = None

    def _collect_result(self, con, now: float) -> None:
        if self._thread is None or not self._done:
            return
        self._thread.join()
        self._thread = None
        ids, err = self._inflight_ids, self._result
        self._inflight_ids, self._result, self._done = [], None, False
        if err:
            self._fails += 1
            self.last_error = err
            self._next_try = now + min(BACKOFF_MAX_S, BACKOFF_BASE_S * (2 ** (self._fails - 1)))
            return
        self._fails, self.last_error = 0, None
        marks = ",".join("?" * len(ids))
        con.execute(f"UPDATE events SET notified_at=? WHERE id IN ({marks})", (now, *ids))

    def _run(self, payload: Dict[str, Any]) -> None:
        try:
            self._post(self.url, payload, self.timeout)
            self._result = ""
        except Exception as e:   # 任何錯都只是「這次沒送成」，不能弄壞主行程
            self._result = str(e) or type(e).__name__
        self._done = True

    def tick(self, con, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        if not self.url:
            return
        self._collect_result(con, now)
        if self._thread is not None or now < self._next_try:
            return
        marks = ",".join("?" * len(NOTIFY_KINDS))
        rows = con.execute(
            f"SELECT id, kind, branch, detail, at FROM events WHERE notified_at IS NULL "
            f"AND resolved_at IS NULL AND kind IN ({marks}) ORDER BY id LIMIT ?",
            (*NOTIFY_KINDS, MAX_EVENTS_PER_POST)).fetchall()
        if not rows:
            return
        if now - min(r["at"] for r in rows) < self.batch_s:   # 攢批：最舊的事件滿 batch_s 才送
            return
        events = []
        for r in rows:
            e = dict(r)
            if e.get("detail"):
                e["detail"] = str(e["detail"])[:MAX_DETAIL]
            events.append(e)
        self._inflight_ids = [e["id"] for e in events]
        self._done = False
        self._result = None
        self._thread = threading.Thread(target=self._run, args=(build_payload(events, now),),
                                        name="jet-notify", daemon=True)
        self._thread.start()
