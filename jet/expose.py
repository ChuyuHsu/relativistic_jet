"""對外開放（選配）：跑一個通道指令，抓到它印出的公開網址，登記到一個固定的轉送點。

核心不認識任何特定的通道服務或轉送服務：指令、網址的正規式、登記網址都由設定檔給。
- 子行程跑 `tunnel_command`，從輸出用 `url_pattern` 抓網址（有群組用第一組，沒有用整段）。
- 抓到就 `PUT register_url`，內容 {"url": "<網址>"}，標頭 Authorization: Bearer <權杖>
  （權杖每次登記時才從檔案讀，不留在記憶體、不進紀錄）。
- 通道行程死掉就重啟、拿到新網址重新登記；每隔 `reregister_s` 秒重新登記一次；
  登記失敗退避重試，第一次失敗與每次換狀態時回報一筆待辦（由主行程寫進資料庫，這裡不碰資料庫）。
- 排隊器結束時 `stop()` 一起收掉整個行程群組。
"""
import json
import os
import queue
import re
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, List, Optional

from . import notify

BACKOFF_BASE_S = 5.0
BACKOFF_MAX_S = 300.0
RESTART_BASE_S = 2.0
RESTART_MAX_S = 60.0


class ExposeError(Exception):
    pass


def check_settings(command: Any, pattern: Any, register_url: Any, token_file: Any, reregister_s: Any) -> None:
    if not isinstance(command, list) or not command or not all(isinstance(x, str) and x for x in command):
        raise ExposeError("expose.tunnel_command 必須是非空字串陣列")
    if not isinstance(pattern, str):
        raise ExposeError("expose.url_pattern 必須是字串")
    try:
        re.compile(pattern)
    except re.error as e:
        raise ExposeError(f"expose.url_pattern 不是合法正規式：{e}")
    try:
        if not notify.check_url(register_url):
            raise ExposeError("expose.register_url 必填")
    except notify.NotifyError:
        raise ExposeError("expose.register_url 只能是 http 或 https 網址")
    if not isinstance(token_file, str) or not token_file:
        raise ExposeError("expose.register_token_file 必填")
    if isinstance(reregister_s, bool) or not isinstance(reregister_s, (int, float)) or reregister_s < 10:
        raise ExposeError("expose.reregister_seconds 必須是 ≥ 10 的數字")


def read_token(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            tok = f.read().strip()
    except OSError as e:
        raise ExposeError(f"讀不到登記權杖檔：{e.strerror or e}")
    if not tok or "\n" in tok or "\r" in tok:
        raise ExposeError("登記權杖檔是空的或格式不對")
    return tok


def put_url(register_url: str, token: str, public_url: str, timeout: float = 10.0) -> None:
    """登記一次；2xx 才算成功，其餘丟 ExposeError（訊息不含權杖）。"""
    notify.check_url(register_url)
    req = urllib.request.Request(register_url, method="PUT",
                                 data=json.dumps({"url": public_url}).encode("utf-8"),
                                 headers={"Content-Type": "application/json; charset=utf-8",
                                          "Authorization": "Bearer " + token})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if not 200 <= r.status < 300:
                raise ExposeError(f"轉送點回 {r.status}")
    except urllib.error.HTTPError as e:
        raise ExposeError(f"轉送點回 {e.code}")
    except (urllib.error.URLError, OSError) as e:
        raise ExposeError(f"登記送不出去：{getattr(e, 'reason', e)}")


class Exposer:
    """背景執行緒管通道與登記；不碰資料庫，要回報的事放進 `events`，由主行程取出去寫。"""

    def __init__(self, command: List[str], url_pattern: str, register_url: str, token_file: str,
                 reregister_s: float = 600.0, popen: Callable[..., Any] = subprocess.Popen,
                 putter: Callable[[str, str, str], None] = put_url,
                 clock: Callable[[], float] = time.monotonic, poll_s: float = 0.2):
        check_settings(command, url_pattern, register_url, token_file, reregister_s)
        self.command, self.re = list(command), re.compile(url_pattern)
        self.register_url, self.token_file, self.reregister_s = register_url, token_file, float(reregister_s)
        self._popen, self._put, self._clock, self._poll_s = popen, putter, clock, poll_s
        self.events: "queue.Queue[str]" = queue.Queue()
        self.url: Optional[str] = None            # 目前通道的網址
        self.registered_url: Optional[str] = None  # 最近一次登記成功的網址
        self.registrations = 0
        self.restarts = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._proc = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------ 對外
    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="jet-expose", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._kill()
        if self._thread:
            self._thread.join(timeout=10)

    def drain_events(self) -> List[str]:
        out = []
        while True:
            try:
                out.append(self.events.get_nowait())
            except queue.Empty:
                return out

    # ------------------------------------------------------------ 內部
    def _kill(self) -> None:
        with self._lock:
            p, self._proc = self._proc, None
        if p is None:
            return
        try:
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except (OSError, AttributeError):
                p.terminate()
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except (OSError, AttributeError):
                    p.kill()
        except Exception:
            pass

    def _spawn(self):
        kw = {"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT, "stdin": subprocess.DEVNULL,
              "universal_newlines": True, "errors": "replace"}
        if self._popen is subprocess.Popen:
            kw["start_new_session"] = True   # 自成行程群組，收的時候整群一起收
        p = self._popen(self.command, **kw)
        with self._lock:
            self._proc = p
        t = threading.Thread(target=self._read, args=(p,), name="jet-expose-read", daemon=True)
        t.start()
        return p

    def _read(self, p) -> None:
        try:
            for line in p.stdout:
                if self._proc is not p:
                    break
                m = self.re.search(line)
                if m and self.url is None:
                    u = m.group(1) if m.groups() else m.group(0)
                    if u.startswith(("http://", "https://")):
                        self.url = u
        except Exception:
            pass

    def _event(self, msg: str) -> None:
        self.events.put(msg[:300])

    def _run(self) -> None:
        restart_delay = RESTART_BASE_S
        while not self._stop.is_set():
            self.url, self.registered_url = None, None
            try:
                p = self._spawn()
            except Exception as e:
                self._event(f"通道指令啟動失敗：{type(e).__name__}")
                if self._stop.wait(restart_delay):
                    return
                restart_delay = min(RESTART_MAX_S, restart_delay * 2)
                continue
            started = self._clock()
            next_try, fails, failing = 0.0, 0, False
            while not self._stop.is_set() and p.poll() is None:
                now = self._clock()
                url = self.url
                if url and now >= next_try and (url != self.registered_url or now >= self._due):
                    try:
                        self._put(self.register_url, read_token(self.token_file), url)
                        self.registered_url, self._due = url, now + self.reregister_s
                        self.registrations += 1
                        next_try, fails = now, 0
                        if failing:
                            failing = False
                            self._event("登記已恢復")
                        restart_delay = RESTART_BASE_S
                    except Exception as e:   # 任何錯都只是這次沒登記成功
                        fails += 1
                        next_try = now + min(BACKOFF_MAX_S, BACKOFF_BASE_S * 2 ** (fails - 1))
                        if not failing:
                            failing = True
                            self._event(f"對外網址登記失敗，會退避重試：{e}")
                self._stop.wait(self._poll_s)
            if self._stop.is_set():
                return
            self.restarts += 1
            self._kill()
            self._event("通道行程結束了，重啟並重新登記")
            if self._clock() - started > 60:
                restart_delay = RESTART_BASE_S
            if self._stop.wait(restart_delay):
                return
            restart_delay = min(RESTART_MAX_S, restart_delay * 2)

    _due = 0.0
