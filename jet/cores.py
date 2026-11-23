"""核池（DESIGN 第 6 節）：每個工作專屬核，任何時刻不共用。"""
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set


def parse_cores(spec: str) -> List[int]:
    """'8-87' 或 '4-7,88-95' → 核編號清單。"""
    out: List[int] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    if len(set(out)) != len(out):
        raise ValueError(f"核重複：{spec}")
    return out


class CorePool:
    def __init__(self, cores: Iterable[int]):
        self.all: List[int] = sorted(cores)
        self.busy: Set[int] = set()

    def acquire(self, n: int) -> List[int]:
        """拿 n 顆空核；不夠就回空清單（不部分給）。"""
        free = [c for c in self.all if c not in self.busy]
        if len(free) < n:
            return []
        got = free[:n]
        self.busy.update(got)
        return got

    def release(self, cores: Iterable[int]) -> None:
        self.busy.difference_update(cores)

    def free_count(self) -> int:
        return len(self.all) - len(self.busy)


PF_KTHREAD = 0x00200000   # 核心執行緒（每顆核都有自己的 ksoftirqd 之類，不算外人）


@dataclass(frozen=True)
class ProcInfo:
    pid: int
    state: str
    ppid: int
    pgrp: int
    sid: int
    flags: int
    cpu: int            # 最後跑在哪一顆核（/proc stat 第 39 欄）


def _parse_stat(pid: int, text: str) -> Optional[ProcInfo]:
    """解析 /proc/<pid>/stat；行程名可能含空白與括號，所以從最後一個 ')' 之後切。"""
    try:
        rest = text[text.rindex(")") + 2:].split()
        return ProcInfo(pid=pid, state=rest[0], ppid=int(rest[1]), pgrp=int(rest[2]),
                        sid=int(rest[3]), flags=int(rest[6]), cpu=int(rest[36]))
    except (ValueError, IndexError):
        return None


def scan_procs(proc_root: str = "/proc") -> List[ProcInfo]:
    """列出現在所有行程（掃的過程中行程消失就略過）。"""
    out: List[ProcInfo] = []
    try:
        names = os.listdir(proc_root)
    except OSError:
        return out
    for name in names:
        if not name.isdigit():
            continue
        try:
            with open(f"{proc_root}/{name}/stat", encoding="utf-8", errors="replace") as f:
                info = _parse_stat(int(name), f.read())
        except OSError:
            continue
        if info is not None:
            out.append(info)
    return out


def group_members(pgid: int, procs: Optional[List[ProcInfo]] = None) -> List[ProcInfo]:
    """屬於這個行程群組或這個工作階段（sid）的活行程；殭屍不算（已經死了，只差回收）。
    工作階段也算進來：子行程自己 setpgid 脫離群組時還抓得到。"""
    procs = scan_procs() if procs is None else procs
    return [p for p in procs if (p.pgrp == pgid or p.sid == pgid) and p.state not in ("Z", "X")]


def online_cpus() -> Set[int]:
    try:
        with open("/sys/devices/system/cpu/online", encoding="utf-8") as f:
            return set(parse_cores(f.read().strip()))
    except (OSError, ValueError):
        return set(range(os.cpu_count() or 1))


def default_baseline() -> Set[int]:
    """「沒綁核」的行程長什麼樣：容器或 cpuset 會讓所有行程都只准用一部分核，
    所以拿 1 號行程的可用核當基準（讀不到就用全部上線的核）。"""
    try:
        return set(os.sched_getaffinity(1))
    except (OSError, AttributeError):
        return online_cpus()


def foreign_busy(cores: Iterable[int], own_pgids: Set[int], *, baseline: Optional[Set[int]] = None,
                 include_running: bool = False, procs: Optional[List[ProcInfo]] = None) -> Set[int]:
    """掃 /proc，找不屬於我方行程群組、而且綁在這些核上的行程，回傳被別人佔用的核。
    派工前呼叫。「綁」＝行程的可用核是基準的真子集（刻意限制過），而且跟這些核有交集；
    沒限制過的一般行程可以跑在任何核上，不算佔用。include_running=True 時，
    正在這些核上執行（狀態 R）的行程也算。核心執行緒、殭屍、自己（本行程）、
    我方行程群組（含同工作階段）都不算外人。"""
    want = set(cores)
    if not want:
        return set()
    base = default_baseline() if baseline is None else set(baseline)
    me = os.getpid()
    busy: Set[int] = set()
    for p in (scan_procs() if procs is None else procs):
        if p.pid == me or p.state in ("Z", "X") or p.flags & PF_KTHREAD:
            continue
        if p.pgrp in own_pgids or p.sid in own_pgids:
            continue
        try:
            aff = set(os.sched_getaffinity(p.pid))
        except (OSError, AttributeError):
            continue
        if aff and aff < base:
            busy |= aff & want
        if include_running and p.state == "R" and p.cpu in want:
            busy.add(p.cpu)
        if busy >= want:
            break
    return busy


def pin(cores: Iterable[int]) -> None:
    """子行程啟動前呼叫：只准在這幾顆核上跑。"""
    os.sched_setaffinity(0, set(cores))
