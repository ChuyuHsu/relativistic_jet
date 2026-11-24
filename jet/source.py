"""外部來源（DESIGN 第 3 節）：排隊器定時去一個網址拉清單，清單就是「管哪些分支、放行到哪個版本」的唯一依據。

清單格式（JSON 陣列）：[{"branch", "parent"?, "approved_sha"?, "retest"?, "priority"?}]（priority 數字小的先）。
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
            continue                      # 同名分支重複：留第一筆，不讓一筆壞資料拖垮整份清單
        seen.add(b)
        par, appr, rt = e.get("parent"), e.get("approved_sha"), e.get("retest", 0)
        if par is not None and (not isinstance(par, str) or not par or par == b):
            raise SourceError(f"{b} 的 parent 不對")
        if appr is not None and (not isinstance(appr, str) or not appr):
            raise SourceError(f"{b} 的 approved_sha 不對")
        if isinstance(rt, bool) or not isinstance(rt, int) or rt < 0:
            raise SourceError(f"{b} 的 retest 要是 ≥0 的整數")
        pr = e.get("priority")
        if pr is not None and (isinstance(pr, bool) or not isinstance(pr, (int, float))):
            raise SourceError(f"{b} 的 priority 要是數字")
        ps = e.get("parent_status")
        if ps is not None and ps not in ("open", "dropped"):
            raise SourceError(f"{b} 的 parent_status 只能是 open 或 dropped")
        out.append({"branch": b, "parent": par or None, "parent_status": ps, "approved_sha": appr or None, "retest": rt, "priority": pr})
    names = {e["branch"] for e in out}
    for e in out:                      # 父鏈不能成環（父不在清單上是允許的：那是「父卡擋住」）
        p, hops = e["parent"], 0
        while p in names:
            if p == e["branch"] or hops > len(out):
                raise SourceError(f"{e['branch']} 的父鏈成環")
            p = next(x["parent"] for x in out if x["branch"] == p)
            hops += 1
    return out


class Drops(list):
    """放棄查核請求清單；form 記清單本體是 array 還是 object（診斷：網址沒帶 with 對方就回 array）。"""
    form = "array"
    suspect = None      # 看板回報的背景測試嫌疑：{"main_sha": ..., "branch": ...}（workflow.loop.triage=code_first 用）


def parse_payload(data):
    """清單本體可以是陣列（舊格式），或物件 {"cards"|"entries"|"items": [...], "drop_checks": [...]}。
    回傳 (清單, 放棄查核請求)。drop_checks 每筆 {card, branch, superseded_by_branch|null}。"""
    drops = Drops()
    if isinstance(data, dict):
        drops.form = "object"
        raw = data.get("cards", data.get("entries", data.get("items")))
        if raw is None:
            raw = []
        dc = data.get("drop_checks") or []
        if not isinstance(dc, list):
            raise SourceError("drop_checks 必須是陣列")
        seen = set()
        for d in dc:
            if not isinstance(d, dict) or not isinstance(d.get("branch"), str) or not d["branch"]:
                raise SourceError("drop_checks 每筆要有 branch（字串）")
            sup = d.get("superseded_by_branch")
            if sup is not None and (not isinstance(sup, str) or not sup):
                raise SourceError(f"{d['branch']} 的 superseded_by_branch 不對")
            card = d.get("card")
            card = str(card) if card is not None else d["branch"]
            if card in seen:
                continue
            seen.add(card)
            item = {"card": card, "branch": d["branch"], "superseded_by_branch": sup}
            if d.get("dropping_at") is not None:       # 看板用來分辨是哪一次放棄，原樣帶回事件
                item["dropping_at"] = d["dropping_at"]
            drops.append(item)
        ls = data.get("loop_suspect")
        if isinstance(ls, dict) and isinstance(ls.get("main_sha"), str) and isinstance(ls.get("branch"), str) \
                and ls["main_sha"] and ls["branch"]:
            drops.suspect = {"main_sha": ls["main_sha"], "branch": ls["branch"]}
        data = raw
    return parse_list(data), drops


NOT_MODIFIED = object()      # 條件式讀取回 304：沿用上次的清單


def http_fetch_full(url: str, token_file: Optional[str], timeout: float, etag: Optional[str] = None,
                    etag_out: Optional[list] = None):
    """拉清單。帶 etag 就送 If-None-Match；對方回 304 傳回 NOT_MODIFIED。
    etag_out 若給一個 list，會把這次 200 回應的 etag 放進去（沒有就清空），讓呼叫端記住。"""
    # 有些代管平台會擋 urllib 預設的 User-Agent（回 403），所以自己報名字
    headers = {"Accept": "application/json", "User-Agent": "relativistic-jet"}
    if etag:
        headers["If-None-Match"] = etag
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
            new_etag = r.headers.get("ETag")
    except urllib.error.HTTPError as e:
        if e.code == 304 and etag:
            return NOT_MODIFIED
        raise SourceError(f"對方回 {e.code}")
    except (urllib.error.URLError, OSError) as e:
        raise SourceError(f"連不上：{getattr(e, 'reason', e)}")
    try:
        res = parse_payload(json.loads(body.decode("utf-8")))
    except ValueError as e:
        raise SourceError(f"不是合法的 JSON：{e}")
    if etag_out is not None:
        etag_out[:] = [new_etag] if new_etag else []
    return res


def http_fetch(url: str, token_file: Optional[str], timeout: float) -> List[dict]:
    return http_fetch_full(url, token_file, timeout)[0]


class SourcePoller:
    """主行程每輪 tick 呼叫 poll()。entries＝最後一次成功的清單（None＝還沒成功過）。"""

    def __init__(self, url: str, token_file: Optional[str] = None, poll_seconds: float = 15,
                 timeout: float = 10.0, fetcher: Optional[Callable[[], object]] = None,
                 clock: Callable[[], float] = time.time, threaded: bool = True):
        check_bearer_url(url) if token_file else None
        self.url, self.token_file, self.poll_s, self.timeout = url, token_file, poll_seconds, timeout
        self._fetcher = fetcher or self._http_conditional
        self._etag: Optional[str] = None         # 上次 200 回應的 etag（看板不支援就一直是 None）
        self._last_payload = None                # 上次成功的 (清單, 放棄查核)，304 時沿用
        self._unconditional = False              # True＝這次不帶 If-None-Match（推 main 前的確認）
        self.clock, self.threaded = clock, threaded
        self.entries: Optional[List[dict]] = None
        self.ok_at: Optional[float] = None       # 最後一次成功的時間
        self.error: Optional[str] = None         # 目前的錯誤（成功就清掉）
        self.drops: List[dict] = []              # 最後一次成功的放棄查核請求
        self.drops_ver = 0                       # 每次成功 +1
        self.new_data = False                    # 有新的成功結果還沒被主行程處理
        self._next = 0.0
        self._thread: Optional[threading.Thread] = None
        self._res = None

    def _http_conditional(self):
        """預設拉法：記住上次的 etag，下次帶 If-None-Match；304 回 NOT_MODIFIED。"""
        out: list = []
        use = None if self._unconditional or self._last_payload is None else self._etag
        res = http_fetch_full(self.url, self.token_file, self.timeout, etag=use, etag_out=out)
        if res is NOT_MODIFIED:
            return res
        self._etag = out[0] if out else None
        self._last_payload = res
        return res

    def _work(self):
        try:
            self._res = (True, self._fetcher())
        except Exception as e:
            self._res = (False, str(e) or type(e).__name__)

    def _take(self) -> Optional[str]:
        """收結果；回傳新出現的錯誤訊息（給主行程發事件），沒有就 None。"""
        ok, val = self._res
        self._res = None
        if ok and val is NOT_MODIFIED:               # 304：沿用上次清單，只更新最後成功時間，不當新資料
            self.ok_at, self.error = self.clock(), None
            return None
        if ok:
            if isinstance(val, tuple):               # (清單, 放棄查核)
                val, self.drops = val[0], val[1]
            else:                                    # 測試或舊 fetcher 直接回清單
                self.drops = []
            self.entries, self.ok_at, self.error, self.new_data = val, self.clock(), None, True
            self.drops_ver += 1
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
        self._unconditional = True                   # 推 main 前那一次維持無條件讀取
        try:
            self._work()
        finally:
            self._unconditional = False
        self._take()
        return self.error

    def age(self) -> Optional[float]:
        return None if self.ok_at is None else self.clock() - self.ok_at
