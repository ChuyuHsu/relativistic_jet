"""執行器：把一個測試工作綁到專屬核上跑（DESIGN 第 5、6 節）。

一個工作＝一個獨立的行程群組（自己的工作階段）、用 sched_setaffinity 綁在專屬核上。
逾時或取消時整群殺掉（先 SIGTERM、寬限後 SIGKILL），正常結束後也會掃一次，不留孤兒。
退出碼：0 全過；1 有測試失敗（逐項看報告）；其他（含 2、被訊號殺掉、找不到指令）都算
infra（不算紅，重跑）。退出碼 0 但報告裡有紅項時以報告為準，判成 fail（紅的絕不放行）。

報告（{report}）缺、壞、沒有項目，或退出碼 1 卻沒有任何紅項：一律判 infra 重跑（寧可重跑，不可誤合）。

只用標準庫、Linux。前提：呼叫者是單執行緒（排隊器主行程）——start 用 preexec_fn 在子行程裡綁核，
preexec_fn 只在單執行緒下安全；之後若同一行程要開 HTTP 執行緒，這裡要改成先 fork 一個單執行緒的啟動器。本模組不寫資料庫、不碰 Git；呼叫它的是排隊器主行程（單執行緒）。
"""
import json
import os
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass, field
from typing import IO, List, Optional, Set

from . import cores as _cores

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


@dataclass
class JobResult:
    status: str                 # pass | fail | timeout | infra | cancelled
    exit_code: Optional[int]
    failed_items: List[str] = field(default_factory=list)
    duration_s: float = 0.0


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
    proc: "subprocess.Popen"
    pgid: int
    started: float
    deadline: float
    log: Optional[IO] = None
    result: Optional[JobResult] = None


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


class Executor:
    """start(job)→handle；poll(handle)→JobResult|None；cancel(handle)。
    start 在開不了時丟 CoreBusy／ResourceLow／ExecutorError，不會留下半個行程。"""

    def __init__(self, test_cmd: Optional[List[str]] = None, min_free_mem_mb: int = 0,
                 min_free_disk_mb: int = 0, grace_s: float = 3.0,
                 baseline_cpus: Optional[Set[int]] = None):
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
        for p in (job.log_path, job.report_path):
            os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
        # 舊報告會讓「沒寫報告的失敗」被誤判成另一輪的結果，先清掉
        try:
            os.unlink(job.report_path)
        except FileNotFoundError:
            pass
        log = open(job.log_path, "wb")
        try:
            # preexec_fn 在子行程裡綁核，確保指令一開始就只在專屬核上；
            # 主行程是單執行緒，用 preexec_fn 是安全的。
            proc = subprocess.Popen(
                argv, cwd=job.workdir, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True, preexec_fn=lambda: _cores.pin(cores), close_fds=True)
        except (OSError, subprocess.SubprocessError) as e:
            log.close()
            raise ExecutorError(f"開不了工作：{e}")
        now = time.monotonic()
        h = Handle(job=job, argv=argv, proc=proc, pgid=proc.pid, started=now,
                   deadline=now + job.timeout_s, log=log)
        self._live.add(h)
        return h

    # ------------------------------------------------------------ 收結果
    def poll(self, handle: Handle) -> Optional[JobResult]:
        """還在跑回 None；結束（含逾時被殺）回結果，之後每次都回同一份。"""
        if handle.result is not None:
            return handle.result
        try:
            exited = os.waitid(os.P_PID, handle.pgid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None
        except ChildProcessError:
            exited = True
        if exited:
            self._sweep(handle, signal.SIGKILL, wait_s=5.0)   # 領頭的走了，留下的孫行程一律清掉
            return self._finish(handle, None)
        if time.monotonic() >= handle.deadline:
            self._terminate(handle)
            return self._finish(handle, "timeout")
        return None

    def cancel(self, handle: Handle) -> JobResult:
        """取消：整群殺掉（已經結束的回原結果）。"""
        if handle.result is not None:
            return handle.result
        self._terminate(handle)
        return self._finish(handle, "cancelled")

    # ------------------------------------------------------------ 內部
    def _finish(self, h: Handle, forced: Optional[str]) -> JobResult:
        rc = h.proc.wait()   # 回收領頭行程（群組清空後才呼叫，所以不會卡住）
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
        h.result = JobResult(status=status, exit_code=rc, failed_items=items, duration_s=dur)
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
