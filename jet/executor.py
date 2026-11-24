"""執行器：把一個測試工作綁到專屬核上跑（DESIGN 第 5、6 節）。

一個工作＝一個獨立的行程群組（自己的工作階段）、用 sched_setaffinity 綁在專屬核上。
逾時或取消時整群殺掉（先 SIGTERM、寬限後 SIGKILL），正常結束後也會掃一次，不留孤兒。
退出碼：0 全過；1 有測試失敗（逐項看報告）；其他（含 2、被訊號殺掉、找不到指令）都算
infra（不算紅，重跑）。退出碼 0 但報告裡有紅項時以報告為準，判成 fail（紅的絕不放行）。

報告（{report}）缺、壞、沒有項目，或退出碼 1 卻沒有任何紅項：一律判 infra 重跑（寧可重跑，不可誤合）。

只用標準庫、Linux。排隊器主行程可以有別的執行緒：開子行程時 fork 與 exec 之間不跑任何 Python
（不用 preexec_fn）；綁核由小啟動器 jet/_launch.py 在新行程裡做，做完 exec 成真正的指令。本模組不寫資料庫、不碰 Git；呼叫它的是排隊器主行程。
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import IO, Dict, List, Optional, Set

from . import cores as _cores

LAUNCHER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_launch.py")
PLACEHOLDERS = ("{layer}", "{cores}", "{ncores}", "{workdir}", "{report}", "{only}")
RED = ("fail", "timeout")   # 報告裡算紅的項目狀態


@dataclass
class Job:
    layer: str                  # selftest | gate | full
    workdir: str                # 被測提交的 worktree
    cores: List[int]
    timeout_s: int
    report_path: str
    log_path: str
    only: Optional[str] = None  # 插隊跑指定測試項目
    argv: Optional[List[str]] = None   # 指令範本；沒給就用 Executor 建構時的 test_cmd
    env_extra: Optional[dict] = None   # 額外環境變數（例如 ALLOW_STRINGS_REWRITE=1）


@dataclass
class JobResult:
    status: str                 # pass | fail | timeout | infra | cancelled
    exit_code: Optional[int]
    failed_items: List[str] = field(default_factory=list)
    duration_s: float = 0.0
    cpu_s: Optional[float] = None      # 逾時時：整個工作群組用掉的 CPU 秒數
    overloaded: bool = False           # 逾時、而且當時系統超載、工作拿到的 CPU 遠低於配給：不是測試的錯


class ExecutorError(Exception):
    """開不了工作（不是測試紅）：呼叫者稍後重試或回報。"""


class CoreBusy(ExecutorError):
    """要用的核上已經有別人（我方別的工作，或外面的行程）。"""

    def __init__(self, cores: Set[int]):
        super().__init__(f"核被佔用：{sorted(cores)}")
        self.cores = set(cores)


class ResourceLow(ExecutorError):
    """記憶體或磁碟低於水位：暫停派新工作。"""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(eq=False)
class Handle:
    job: Job
    argv: List[str]
    proc: Optional["subprocess.Popen"]    # 接管來的工作沒有（上一代主行程開的，不是我們的子行程）
    pgid: int
    started: float
    deadline: float
    log: Optional[IO] = None
    result: Optional[JobResult] = None
    cpu_s: Optional[float] = None
    cpu_max: float = 0.0          # 跑的途中取樣到的群組 CPU 秒數最大值（結束後孫行程消失，只靠結束當下量會漏）
    cpu_at: float = 0.0           # 上次取樣的時間
    overloaded: bool = False

    @property
    def exit_path(self) -> str:
        return exit_path_for(self.job)


def exit_path_for(job: "Job") -> str:
    return os.path.join(os.path.dirname(os.path.abspath(job.log_path)), "exit.json")


def read_exit(path: str) -> Optional[int]:
    """啟動器寫的 exit.json 裡的退出碼；沒有或壞掉回 None。"""
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        rc = d["rc"]
        if isinstance(d.get("signal"), int) and not isinstance(d.get("signal"), bool):
            rc = -d["signal"]                                       # 被訊號殺掉：跟直接 exec 時的退出碼寫法一致（負的訊號編號）
        return rc if isinstance(rc, int) and not isinstance(rc, bool) else None
    except (OSError, ValueError, KeyError, TypeError):
        return None


def mem_available_mb(meminfo: str = "/proc/meminfo") -> Optional[int]:
    """可用記憶體（MB）；讀不到回 None（不擋工作）。"""
    try:
        with open(meminfo, encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def disk_free_mb(path: str) -> Optional[int]:
    try:
        return shutil.disk_usage(path).free // (1024 * 1024)
    except OSError:
        return None


def fill_argv(template: List[str], job: Job) -> List[str]:
    """把 {layer}{cores}{ncores}{workdir}{report}{only} 換成這個工作的值（逐字取代，
    不用 format，所以指令裡其他的大括號不受影響）。{only} 沒指定時換成空字串。"""
    vals = {
        "{layer}": job.layer,
        "{cores}": ",".join(str(c) for c in job.cores),
        "{ncores}": str(len(job.cores)),
        "{workdir}": job.workdir,
        "{report}": job.report_path,
        "{only}": job.only or "",
    }
    out = []
    for a in template:
        for k, v in vals.items():
            a = a.replace(k, v)
        out.append(a)
    return out


def resolve_command(argv0: str, cwd: str, env_path: Optional[str] = None) -> Optional[str]:
    """指令找得到而且能執行才回路徑（含 / 的相對路徑照工作目錄解）；找不到回 None。
    啟動器會 exec，找不到只會在子行程裡失敗；這裡先擋，讓 start 像以前一樣直接丟 ExecutorError。"""
    if os.sep in argv0:
        p = argv0 if os.path.isabs(argv0) else os.path.join(cwd, argv0)
        return p if os.path.isfile(p) and os.access(p, os.X_OK) else None
    return shutil.which(argv0, path=env_path)


def read_report(path: str) -> Optional[List[str]]:
    """報告（JSON）裡 fail／timeout 的項目 id。報告不存在、JSON 壞、沒有 items、或 items 是空的
    （等於沒測任何東西）回 None＝報告不可信；呼叫者不可把它當成「沒有紅項」。"""
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        items = d["items"]
        if not isinstance(items, list) or not items:
            return None
        return [str(it["id"]) for it in items if it.get("status") in RED]
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None


def read_report_summaries(path: str) -> Dict[str, str]:
    """報告裡紅項的 summary（有寫才有）。壞報告回空 dict。"""
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return {str(it["id"]): str(it["summary"]).strip() for it in d["items"]
                if it.get("status") in RED and str(it.get("summary") or "").strip()}
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return {}


_FAIL_WORDS = ("FAIL", "ERROR", "Error", "Traceback", "失敗", "錯誤", "紅")


def failure_excerpt(log_path: str, item_id: str, lines: int = 15, max_chars: int = 1200,
                    tail_bytes: int = 2_000_000) -> str:
    """從測試記錄（all.log）抽出這個紅項的失敗那幾行：
    找最後一次出現項目名、而且同一行有失敗字眼的位置，從那行起取 lines 行；沒有就取最後一次出現項目名的位置；
    整份記錄都沒提到這個項目名（或項目是「逾時」）就取記錄最後 lines 行（前面加註）。讀不到記錄回空字串。"""
    try:
        with open(log_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - tail_bytes))
            data = f.read().decode("utf-8", "replace")
    except OSError:
        return ""
    ls = [ln.rstrip() for ln in data.splitlines()]
    while ls and not ls[-1]:
        ls.pop()
    if not ls:
        return ""
    hits = [i for i, ln in enumerate(ls) if item_id and item_id in ln]
    pick = [i for i in hits if any(w in ls[i] for w in _FAIL_WORDS)] or hits
    if pick:
        i = pick[-1]
        out = "\n".join(ls[i:i + lines])
    else:
        out = "（記錄沒提到這項，附最後 %d 行）\n" % lines + "\n".join(ls[-lines:])
    return out[:max_chars]


class Executor:
    """start(job)→handle；poll(handle)→JobResult|None；cancel(handle)。
    start 在開不了時丟 CoreBusy／ResourceLow／ExecutorError，不會留下半個行程。"""

    def __init__(self, test_cmd: Optional[List[str]] = None, min_free_mem_mb: int = 0,
                 min_free_disk_mb: int = 0, grace_s: float = 3.0,
                 baseline_cpus: Optional[Set[int]] = None, overload_cpu_ratio: float = 0.6,
                 overload_psi: float = 20.0):
        self.overload_cpu_ratio = overload_cpu_ratio
        self.overload_psi = overload_psi
        self.test_cmd = list(test_cmd) if test_cmd else None
        self.min_free_mem_mb = min_free_mem_mb
        self.min_free_disk_mb = min_free_disk_mb
        self.grace_s = grace_s
        self.baseline_cpus = baseline_cpus   # 測試用：指定「沒綁核」的基準；平常 None＝自動判斷
        self._live: Set[Handle] = set()

    # ------------------------------------------------------------ 查詢
    def live_pgids(self) -> Set[int]:
        return {h.pgid for h in self._live}

    def live_cores(self) -> Set[int]:
        return {c for h in self._live for c in h.job.cores}

    def check_resources(self, path: str) -> Optional[str]:
        """低於水位回原因字串，正常回 None。排隊器派新工作前可先問。"""
        if self.min_free_mem_mb > 0:
            m = mem_available_mb()
            if m is not None and m < self.min_free_mem_mb:
                return f"可用記憶體 {m} MB，低於水位 {self.min_free_mem_mb} MB"
        if self.min_free_disk_mb > 0:
            d = disk_free_mb(path)
            if d is not None and d < self.min_free_disk_mb:
                return f"可用磁碟 {d} MB，低於水位 {self.min_free_disk_mb} MB"
        return None

    # ------------------------------------------------------------ 開跑
    def start(self, job: Job) -> Handle:
        template = job.argv or self.test_cmd
        if not template:
            raise ExecutorError("沒有測試指令（Job.argv 或 Executor.test_cmd）")
        cores = set(job.cores)
        if not cores or len(cores) != len(job.cores):
            raise ExecutorError("工作的核清單不可為空或重複")
        mine = cores & self.live_cores()
        if mine:
            raise CoreBusy(mine)   # 任何時刻不共用同一顆核
        outsider = _cores.foreign_busy(cores, self.live_pgids(), baseline=self.baseline_cpus)
        if outsider:
            raise CoreBusy(outsider)
        why = self.check_resources(job.workdir)
        if why:
            raise ResourceLow(why)
        argv = fill_argv(template, job)
        if resolve_command(argv[0], job.workdir) is None:
            raise ExecutorError("找不到指令：%s" % argv[0])
        known = _cores.online_cpus() | _cores.default_baseline()
        if not cores <= known:
            raise ExecutorError("核不存在：%s" % sorted(cores - known))
        for p in (job.log_path, job.report_path):
            os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
        # 舊報告會讓「沒寫報告的失敗」被誤判成另一輪的結果，先清掉
        for stale in (job.report_path, exit_path_for(job)):
            try:
                os.unlink(stale)
            except FileNotFoundError:
                pass
        log = open(job.log_path, "wb")
        try:
            # fork 與 exec 之間不跑 Python（多執行緒行程裡 preexec_fn 可能卡死）：
            # exec 啟動器，由它在新行程裡綁核再 exec 真正的指令，指令從第一行起就只在專屬核上。
            launch = [sys.executable, "-I", LAUNCHER, "--exit-file", exit_path_for(job),
                      ",".join(str(c) for c in sorted(cores)), "--"] + argv
            env = dict(os.environ)       # 讓測試指令知道自己分到幾顆核，好把 -j、引擎執行緒數壓在這個數字內
            env["JET_CORES"] = ",".join(str(c) for c in sorted(cores))
            env["JET_NCORES"] = str(len(cores))
            if job.env_extra:
                env.update(job.env_extra)
            proc = subprocess.Popen(
                launch, cwd=job.workdir, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True, close_fds=True)
        except (OSError, subprocess.SubprocessError) as e:
            log.close()
            raise ExecutorError(f"開不了工作：{e}")
        now = time.monotonic()
        h = Handle(job=job, argv=argv, proc=proc, pgid=proc.pid, started=now,
                   deadline=now + job.timeout_s, log=log)
        self._live.add(h)
        return h

    def adopt(self, job: Job, pgid: int, remaining_s: float, elapsed_s: float = 0.0) -> Handle:
        """接管上一代主行程開的工作：行程群組還活著，或已經寫了 exit.json。逾時從原本的開跑時間算（remaining_s）。"""
        now = time.monotonic()
        h = Handle(job=job, argv=[], proc=None, pgid=pgid, started=now - max(elapsed_s, 0.0),
                   deadline=now + max(remaining_s, 0.0))
        self._live.add(h)
        return h

    def _poll_adopted(self, handle: Handle) -> Optional[JobResult]:
        rc = read_exit(handle.exit_path)
        if rc is not None:
            self._sweep(handle, signal.SIGKILL, wait_s=5.0)        # 領頭寫完檔走了，留下的孫行程一律清掉
            return self._finish(handle, None, rc=rc)
        if not _cores.group_members(handle.pgid):                  # 群組沒了、又沒留下 exit.json（被 SIGKILL、或舊版啟動器）：不知道結果
            return self._finish(handle, None, rc=-1)
        if time.monotonic() >= handle.deadline:
            handle.cpu_s = _cores.group_cpu_seconds(handle.pgid)
            handle.overloaded = self._was_overloaded(handle, handle.cpu_s)
            self._terminate(handle)
            return self._finish(handle, "timeout")
        return None

    # ------------------------------------------------------------ 收結果
    def poll(self, handle: Handle) -> Optional[JobResult]:
        """還在跑回 None；結束（含逾時被殺）回結果，之後每次都回同一份。"""
        if handle.result is not None:
            return handle.result
        if handle.proc is None:
            return self._poll_adopted(handle)
        try:
            exited = os.waitid(os.P_PID, handle.pgid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None
        except ChildProcessError:
            exited = True
        if exited:
            try:
                handle.cpu_s = max(handle.cpu_max, _cores.group_cpu_seconds(handle.pgid), _cores.proc_cpu_seconds(handle.pgid))   # 領頭的已是殭屍：它自己的加上已回收子行程
            except Exception:
                handle.cpu_s = handle.cpu_max or None
            self._sweep(handle, signal.SIGKILL, wait_s=5.0)   # 領頭的走了，留下的孫行程一律清掉
            return self._finish(handle, None)
        nowm = time.monotonic()
        if nowm - handle.cpu_at >= 10.0:                       # 每 10 秒取樣一次整群組的 CPU 秒數（背景性能量測用）
            handle.cpu_at = nowm
            try:
                handle.cpu_max = max(handle.cpu_max, _cores.group_cpu_seconds(handle.pgid))
            except Exception:
                pass
        if time.monotonic() >= handle.deadline:
            cpu = _cores.group_cpu_seconds(handle.pgid)        # 殺之前先量
            handle.cpu_s = cpu
            handle.overloaded = self._was_overloaded(handle, cpu)
            self._terminate(handle)
            return self._finish(handle, "timeout")
        return None

    def _was_overloaded(self, h: Handle, cpu_s: float) -> bool:
        """逾時是不是機器超載造成的：系統超載，而且這個工作實際拿到的 CPU 遠低於「牆鐘×核數」。
        兩個條件都要成立；單純卡住不動（機器不忙）仍然算測試的錯。"""
        wall = max(time.monotonic() - h.started, 1e-6)
        ratio = cpu_s / (wall * max(len(h.job.cores), 1))
        return ratio < self.overload_cpu_ratio and _cores.system_overloaded(self.overload_psi)

    def cancel(self, handle: Handle) -> JobResult:
        """取消：整群殺掉（已經結束的回原結果）。"""
        if handle.result is not None:
            return handle.result
        self._terminate(handle)
        return self._finish(handle, "cancelled")

    # ------------------------------------------------------------ 內部
    def _finish(self, h: Handle, forced: Optional[str], rc: Optional[int] = None) -> JobResult:
        if h.proc is not None:
            rc = h.proc.wait()   # 回收領頭行程（群組清空後才呼叫，所以不會卡住）
            fromfile = read_exit(h.exit_path)
            if fromfile is not None and forced is None:
                rc = fromfile
        dur = time.monotonic() - h.started
        if h.log is not None:
            h.log.close()
            h.log = None
        rep = read_report(h.job.report_path) if forced != "cancelled" else None
        items = rep or []
        if forced == "cancelled":
            status = "cancelled"
        elif forced == "timeout":
            status = "timeout"
        elif rc not in (0, 1):
            status = "infra"   # 2、被訊號殺掉、找不到指令…都不算紅
        elif rep is None:
            # 報告缺、壞、或沒有項目：沒有可信的逐項結果。判綠會讓沒測完的提交進 main，
            # 判紅則空清單會被批測當成「沒有新紅項」；一律當 infra 重跑（寧可重跑，不可誤合）
            status = "infra"
        elif rc == 0:
            status = "fail" if items else "pass"   # 退出碼 0 但報告有紅項：以報告為準
        else:
            status = "fail" if items else "infra"  # 退出碼 1 卻零個紅項：前後矛盾，重跑
        h.result = JobResult(status=status, exit_code=rc, failed_items=items, duration_s=dur,
                             cpu_s=h.cpu_s, overloaded=h.overloaded and status == "timeout")
        self._live.discard(h)
        return h.result

    def _signal_all(self, h: Handle, sig: int) -> None:
        try:
            os.killpg(h.pgid, sig)
        except (ProcessLookupError, PermissionError):
            pass
        for p in _cores.group_members(h.pgid):   # 自己 setpgid 脫離群組的，靠同一個工作階段抓回來
            if p.pgrp != h.pgid:
                try:
                    os.kill(p.pid, sig)
                except (ProcessLookupError, PermissionError):
                    pass

    def _sweep(self, h: Handle, sig: int, wait_s: float) -> bool:
        """送訊號給整群，等到群組裡沒有活行程；回是否清乾淨。"""
        self._signal_all(h, sig)
        end = time.monotonic() + wait_s
        while time.monotonic() < end:
            if not _cores.group_members(h.pgid):
                return True
            time.sleep(0.02)
        return not _cores.group_members(h.pgid)

    def _terminate(self, h: Handle) -> None:
        """先 SIGTERM 給寬限；沒走乾淨就 SIGKILL。"""
        if not self._sweep(h, signal.SIGTERM, self.grace_s):
            self._sweep(h, signal.SIGKILL, 5.0)
        else:
            self._sweep(h, signal.SIGKILL, 1.0)   # 領頭走了但還有漏網的，補一刀
