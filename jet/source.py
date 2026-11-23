"""外部來源（DESIGN 第 3 節）：排隊器定時去一個網址拉清單，清單就是「管哪些分支、放行到哪個版本」的唯一依據。

清單格式（JSON 陣列）：[{"branch", "parent"?, "approved_sha"?, "retest"?}]。
- 拉不到、非 200、逾時、格式不對：保留上一次成功的清單，只記錯誤，絕不因此取消登記或撤回放行。
- 空陣列是真的「一條都不管」。
- 只往外連；權杖放檔案，不進設定檔。網路請求在背景執行緒，結果由主行程下一輪收（資料庫只有主行程寫）。
"""
import json
import threading
import time
import urllib.error
import urllib.request
from typing import Callable, List, Optional

from .notify import NotifyError, check_bearer_url, read_token


class SourceError(Exception):
    pass


def parse_list(data) -> List[dict]:
    """檢查並整理清單；格式不對丟 SourceError。"""
    if not isinstance(data, list):
        raise SourceError("清單必須是 JSON 陣列")
    out, seen = [], set()
    for e in data:
        if not isinstance(e, dict) or not isinstance(e.get("branch"), str) or not e["branch"]:
            raise SourceError("清單每一筆要有 branch（字串）")
        b = e["branch"]
        if b in seen:
            raise SourceError(f"清單分支重複：{b}")
        seen.add(b)
        par, appr, rt = e.get("parent"), e.get("approved_sha"), e.get("retest", 0)
        if par is not None and (not isinstance(par, str) or not par or par == b):
            raise SourceError(f"{b} 的 parent 不對")
        if appr is not None and (not isinstance(appr, str) or not appr):
            raise SourceError(f"{b} 的 approved_sha 不對")
        if isinstance(rt, bool) or not isinstance(rt, int) or rt < 0:
            raise SourceError(f"{b} 的 retest 要是 ≥0 的整數")
        out.append({"branch": b, "parent": par or None, "approved_sha": appr or None, "retest": rt})
    names = {e["branch"] for e in out}
    for e in out:                      # 父鏈不能成環（父不在清單上是允許的：那是「父卡擋住」）
        p, hops = e["parent"], 0
        while p in names:
            if p == e["branch"] or hops > len(out):
                raise SourceError(f"{e['branch']} 的父鏈成環")
            p = next(x["parent"] for x in out if x["branch"] == p)
            hops += 1
    return out


def http_fetch(url: str, token_file: Optional[str], timeout: float) -> List[dict]:
    # 有些代管平台會擋 urllib 預設的 User-Agent（回 403），所以自己報名字
    headers = {"Accept": "application/json", "User-Agent": "relativistic-jet"}
    if token_file:
        try:
            headers["Authorization"] = "Bearer " + read_token(token_file)
        except NotifyError as e:
            raise SourceError(str(e))
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if r.status != 200:
                raise SourceError(f"對方回 {r.status}")
            body = r.read(8 << 20)
    except urllib.error.HTTPError as e:
        raise SourceError(f"對方回 {e.code}")
    except (urllib.error.URLError, OSError) as e:
        raise SourceError(f"連不上：{getattr(e, 'reason', e)}")
    try:
        return parse_list(json.loads(body.decode("utf-8")))
    except ValueError as e:
        raise SourceError(f"不是合法的 JSON：{e}")


class SourcePoller:
    """主行程每輪 tick 呼叫 poll()。entries＝最後一次成功的清單（None＝還沒成功過）。"""

    def __init__(self, url: str, token_file: Optional[str] = None, poll_seconds: float = 15,
                 timeout: float = 10.0, fetcher: Optional[Callable[[], List[dict]]] = None,
                 clock: Callable[[], float] = time.time, threaded: bool = True):
        check_bearer_url(url) if token_file else None
        self.url, self.token_file, self.poll_s, self.timeout = url, token_file, poll_seconds, timeout
        self._fetcher = fetcher or (lambda: http_fetch(self.url, self.token_file, self.timeout))
        self.clock, self.threaded = clock, threaded
        self.entries: Optional[List[dict]] = None
        self.ok_at: Optional[float] = None       # 最後一次成功的時間
        self.error: Optional[str] = None         # 目前的錯誤（成功就清掉）
        self.new_data = False                    # 有新的成功結果還沒被主行程處理
        self._next = 0.0
        self._thread: Optional[threading.Thread] = None
        self._res = None

    def _work(self):
        try:
            self._res = (True, self._fetcher())
        except Exception as e:
            self._res = (False, str(e) or type(e).__name__)

    def _take(self) -> Optional[str]:
        """收結果；回傳新出現的錯誤訊息（給主行程發事件），沒有就 None。"""
        ok, val = self._res
        self._res = None
        if ok:
            self.entries, self.ok_at, self.error, self.new_data = val, self.clock(), None, True
            return None
        first = self.error != val
        self.error = val
        return val if first else None

    def poll(self) -> Optional[str]:
        """推進一步；回傳新的錯誤訊息或 None。"""
        err = None
        if self._thread is not None:
            if self._thread.is_alive():
                return None
            self._thread = None
            err = self._take()
        now = self.clock()
        if now >= self._next:
            self._next = now + self.poll_s
            if self.threaded:
                self._thread = threading.Thread(target=self._work, name="jet-source", daemon=True)
                self._thread.start()
            else:
                self._work()
                err = self._take() or err
        return err

    def refresh_now(self) -> Optional[str]:
        """推 main 前同步重拉一次。失敗沿用舊清單；回傳目前的錯誤（成功為 None）。"""
        if self._thread is not None:
            self._thread.join(self.timeout + 1)
            if self._thread.is_alive():
                return "上一次拉取還沒回來"
            self._thread = None
            self._take()
        self._work()
        self._take()
        return self.error

    def age(self) -> Optional[float]:
        return None if self.ok_at is None else self.clock() - self.ok_at
