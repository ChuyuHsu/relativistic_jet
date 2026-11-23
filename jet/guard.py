"""自動退版（DESIGN 第 9 節）：換新版後短時間內連續當掉三次，自動退回上一版。

做法（不碰資料庫，所以不是「另一個寫入者」）：
- 版本放 `<root>/releases/<id>/`；`<root>/current` 是指向目前版本的符號連結，`<root>/previous` 指向上一版。
- `deploy` 換版：previous←目前、current←新版本。
- `Supervisor` 守著主行程：它掛了就重啟；從換版（或上次退版）算起，連續當掉達上限（預設 3 次，
  兩次當掉之間不超過 window_s 秒）就把 current 切回 previous，再重啟，並通知。
- 主行程活超過 stable_s 秒就算穩定，當掉計數歸零。乾淨結束（退出碼 0）守護就跟著結束。
- 退版這類事件寫到 `<state_dir>/supervisor_events.jsonl`（守護只寫這個檔）；
  主行程啟動時用 `ingest_events` 讀進 events 表（寫入仍只有主行程）。
"""
import json
import os
import signal
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional

from . import notify

EVENTS_FILE = "supervisor_events.jsonl"
OFFSET_FILE = "supervisor_events.offset"
RESTART_CODE = 75        # 主行程要求重啟（jet restart）：守護不算當掉，用現在的 current 重新拉起
STOP_FILE = "STOP"       # <root>/STOP 存在＝守護下次看到子行程結束就不再拉起（jet stop 放的）
PID_FILE = "supervisor.pid"


class GuardError(Exception):
    pass


# ---------------------------------------------------------------- 版本

def _releases(root: str) -> str:
    return os.path.join(root, "releases")


def _link(root: str, name: str) -> str:
    return os.path.join(root, name)


def _target(root: str, name: str) -> Optional[str]:
    p = _link(root, name)
    if os.path.islink(p):
        return os.path.basename(os.readlink(p).rstrip("/"))
    return None


def _set_link(root: str, name: str, release_id: str) -> None:
    tmp = _link(root, name) + ".tmp"
    if os.path.lexists(tmp):
        os.remove(tmp)
    os.symlink(os.path.join("releases", release_id), tmp)
    os.replace(tmp, _link(root, name))   # 原子換連結


def list_releases(root: str) -> List[str]:
    try:
        return sorted(os.listdir(_releases(root)))
    except FileNotFoundError:
        return []


def deploy(root: str, release_id: str) -> Dict[str, Optional[str]]:
    """換版：previous←目前、current←release_id。版本目錄要先放好。"""
    if not release_id or "/" in release_id or release_id in (".", ".."):
        raise GuardError("版本名稱不合法")
    if not os.path.isdir(os.path.join(_releases(root), release_id)):
        raise GuardError(f"找不到版本目錄 releases/{release_id}")
    old = _target(root, "current")
    if old == release_id:
        return {"current": old, "previous": _target(root, "previous")}
    if old:
        _set_link(root, "previous", old)
    _set_link(root, "current", release_id)
    return {"current": release_id, "previous": old}


def rollback(root: str) -> Dict[str, Optional[str]]:
    """手動或自動退版：current 與 previous 對調（可以再退回來）。"""
    prev, cur = _target(root, "previous"), _target(root, "current")
    if not prev:
        raise GuardError("沒有上一版可以退")
    _set_link(root, "current", prev)
    if cur:
        _set_link(root, "previous", cur)
    return {"current": prev, "previous": cur}


# ---------------------------------------------------------------- 守護

def add_event(state_dir: str, kind: str, detail: str, now: Optional[float] = None) -> None:
    os.makedirs(state_dir, exist_ok=True)
    now = time.time() if now is None else now
    with open(os.path.join(state_dir, EVENTS_FILE), "a", encoding="utf-8") as f:
        f.write(json.dumps({"kind": kind, "detail": detail[:500], "at": now}, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def ingest_events(con, state_dir: str) -> int:
    """主行程啟動時呼叫：把守護寫的事件讀進 events 表。靠 offset 檔避免重複讀。"""
    path = os.path.join(state_dir, EVENTS_FILE)
    off_path = os.path.join(state_dir, OFFSET_FILE)
    try:
        with open(off_path, encoding="utf-8") as f:
            offset = int(f.read().strip() or 0)
    except (OSError, ValueError):
        offset = 0
    try:
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read()
    except FileNotFoundError:
        return 0
    n = 0
    consumed = 0
    for raw in data.splitlines(keepends=True):
        if not raw.endswith(b"\n"):
            break   # 最後一行還沒寫完，下次再讀
        consumed += len(raw)
        try:
            e = json.loads(raw.decode("utf-8"))
        except ValueError:
            continue
        if isinstance(e, dict) and e.get("kind") in ("queue_error",):
            con.execute("INSERT INTO events(kind, branch, detail, at) VALUES(?, NULL, ?, ?)",
                        (e["kind"], str(e.get("detail", ""))[:500], float(e.get("at", time.time()))))
            n += 1
    tmp = off_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(offset + consumed))
    os.replace(tmp, off_path)
    return n


class Supervisor:
    """守著主行程。`cmd_for(release_dir)` 回要執行的指令（argv）；測試時可注入假的指令與時鐘。"""

    def __init__(self, root: str, state_dir: str, cmd_for: Callable[[str], List[str]],
                 max_crashes: int = 3, window_s: float = 600.0, stable_s: float = 300.0,
                 restart_delay_s: float = 2.0, notify_url: Optional[str] = None,
                 clock: Callable[[], float] = time.time, sleeper: Callable[[float], None] = time.sleep,
                 popen: Callable[..., Any] = subprocess.Popen, reexec: Optional[Callable[[], None]] = None):
        self.root, self.state_dir, self.cmd_for = root, state_dir, cmd_for
        self.max_crashes, self.window_s, self.stable_s = max_crashes, window_s, stable_s
        self.restart_delay_s = restart_delay_s
        self.notify_url = notify.check_url(notify_url)
        self.clock, self.sleeper, self.popen = clock, sleeper, popen
        self.reexec = reexec          # 重啟時把守護自己也換成新版（exec 自己）；沒給就只重拉子行程
        self.crashes: List[float] = []
        self._child = None
        self._stop = False

    def stop(self, *_a) -> None:
        self._stop = True
        if self._child is not None and self._child.poll() is None:
            self._child.terminate()

    def _tell(self, kind: str, detail: str) -> None:
        add_event(self.state_dir, kind, detail, self.clock())
        if self.notify_url:
            try:
                notify.post_json(self.notify_url, notify.build_payload(
                    [{"id": 0, "kind": kind, "branch": None, "detail": detail, "at": self.clock()}], self.clock()), 10)
            except Exception:
                pass   # 通知送不出去不能影響守護

    def _current_dir(self) -> str:
        p = _link(self.root, "current")
        if not os.path.isdir(p):
            raise GuardError("沒有 current 版本，先 deploy 一個")
        return os.path.realpath(p)

    def run(self, max_starts: Optional[int] = None) -> int:
        """跑到乾淨結束（回 0）或被要求停止；max_starts 給測試用。"""
        starts = 0
        while not self._stop:
            if max_starts is not None and starts >= max_starts:
                return 1
            cur_dir = self._current_dir()
            t0 = self.clock()
            self._child = self.popen(self.cmd_for(cur_dir), cwd=cur_dir)
            starts += 1
            code = self._child.wait()
            self._child = None
            stopfile = os.path.join(self.root, STOP_FILE)
            if os.path.exists(stopfile):
                os.remove(stopfile)
                return 0
            if code == RESTART_CODE and not self._stop:
                self.crashes = []
                if self.reexec is not None:
                    self.reexec()                  # 正常不會回來（exec）
                continue
            if self._stop or code == 0:
                return 0 if code in (0, -signal.SIGTERM) else code
            now = self.clock()
            if now - t0 >= self.stable_s:
                self.crashes = []          # 活得夠久，之前的當掉不算
            self.crashes = [t for t in self.crashes if now - t <= self.window_s] + [now]
            if len(self.crashes) >= self.max_crashes:
                self._on_repeated_crash(code)
            self.sleeper(self.restart_delay_s)
        return 0

    def _on_repeated_crash(self, code: int) -> None:
        n = len(self.crashes)
        self.crashes = []
        try:
            res = rollback(self.root)
            self._tell("queue_error", f"連續當掉 {n} 次（最後退出碼 {code}），已自動退回上一版 {res['current']}")
        except GuardError:
            self._tell("queue_error", f"連續當掉 {n} 次（最後退出碼 {code}），沒有上一版可退，持續重試")


def default_cmd(config_path: str) -> Callable[[str], List[str]]:
    def build(release_dir: str) -> List[str]:
        return [sys.executable, "-m", "jet", "serve", "--config", config_path]
    return build


def install_signal_handlers(sup: Supervisor) -> None:
    signal.signal(signal.SIGTERM, sup.stop)
    signal.signal(signal.SIGINT, sup.stop)


def pid_alive(root: str) -> Optional[int]:
    """守護還在跑嗎：看 pid 檔，確認那個行程真的是 supervise。回 pid 或 None。"""
    try:
        with open(os.path.join(root, PID_FILE), encoding="utf-8") as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            if b"supervise" not in f.read():
                return None
        return pid
    except (OSError, ValueError):
        return None


def write_pid(root: str) -> None:
    with open(os.path.join(root, PID_FILE), "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))


def start_background(root: str, config_path: str) -> int:
    """把守護叫起來（背景、自成一個工作階段）；輸出寫 <root>/supervisor.log。回 pid。"""
    root, config_path = os.path.abspath(root), os.path.abspath(config_path)
    if pid_alive(root):
        raise GuardError("守護已經在跑了")
    cur = _link(root, "current")
    if not os.path.isdir(cur):
        raise GuardError("沒有 current 版本，先 deploy 一個")
    stopfile = os.path.join(root, STOP_FILE)
    if os.path.exists(stopfile):
        os.remove(stopfile)
    log = open(os.path.join(root, "supervisor.log"), "ab")
    p = subprocess.Popen([sys.executable, "-m", "jet", "supervise", "--root", root, "--config", config_path],
                         cwd=os.path.realpath(cur), stdout=log, stderr=subprocess.STDOUT,
                         stdin=subprocess.DEVNULL, start_new_session=True)
    return p.pid


def make_reexec(root: str, config_path: str) -> Callable[[], None]:
    """守護自己換版：切到現在的 current 目錄，用同一個 pid exec 新版的 supervise。"""
    root, config_path = os.path.abspath(root), os.path.abspath(config_path)

    def go():
        os.chdir(os.path.realpath(_link(root, "current")))
        os.execv(sys.executable, [sys.executable, "-m", "jet", "supervise", "--root", root, "--config", config_path])
    return go
