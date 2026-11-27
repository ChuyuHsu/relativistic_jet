"""核池（DESIGN 第 6 節）：每個工作專屬核，任何時刻不共用。"""
import os
from dataclasses import dataclass
from typing import Dict, FrozenSet, Iterable, List, Optional, Set, Tuple


def parse_cores(spec: str) -> List[int]:
    """'0-7' 或 '0-3,8-11' → 核編號清單。"""
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

    def reserve(self, cores: Iterable[int]) -> bool:
        """接管上一代的工作：把指定的核記成忙。核不在池裡、或已經有人用就不接（回 False，什麼都不改）。"""
        cores = set(cores)
        if not cores or not cores <= set(self.all) or cores & self.busy:
            return False
        self.busy.update(cores)
        return True

    def free_count(self) -> int:
        return len(self.all) - len(self.busy)


def read_topology(root: str = "/sys/devices/system/cpu") -> Dict[int, FrozenSet[int]]:
    """每顆邏輯核 → 同一個實體核上的兄弟執行緒（含自己）。讀不到的核當作自己一個實體核。"""
    topo: Dict[int, FrozenSet[int]] = {}
    try:
        names = os.listdir(root)
    except OSError:
        return topo
    for n in names:
        if not (n.startswith("cpu") and n[3:].isdigit()):
            continue
        try:
            with open(f"{root}/{n}/topology/thread_siblings_list", encoding="utf-8") as f:
                topo[int(n[3:])] = frozenset(parse_cores(f.read().strip().replace(" ", "")))
        except (OSError, ValueError):
            continue
    return topo


class PhysicalPool(CorePool):
    """以實體核為單位配槽：同一個實體核的兄弟執行緒只會給同一個工作，絕不分給兩個槽。

    - 池裡的邏輯核，只有「兄弟全都在池裡」的實體核才算（保留核的兄弟也視為保留，自動排除）。
    - acquire(n)＝拿 n 個實體核。threads="all"：回傳這 n 個實體核的全部執行緒；
      threads="one"：每個實體核只回傳一條執行緒，另一條空著不給別人（也不算空核）。
    - busy 是邏輯核集合；一個實體核只要有一條執行緒忙（含外人佔用），整個實體核就不算空。
    """

    def __init__(self, cores: Iterable[int], threads: str = "all",
                 topo: Optional[Dict[int, FrozenSet[int]]] = None):
        cores = set(cores)
        topo = read_topology() if topo is None else topo
        seen: Set[int] = set()
        self.units: List[Tuple[int, ...]] = []
        self.excluded: List[int] = []
        for c in sorted(cores):
            if c in seen:
                continue
            sib = topo.get(c, frozenset([c]))
            seen |= sib
            if sib <= cores:
                self.units.append(tuple(sorted(sib)))
            else:
                self.excluded.extend(sorted(sib & cores))
        self.threads = threads
        self._unit_of = {c: u for u in self.units for c in u}
        super().__init__([c for u in self.units for c in u])

    def _free_units(self) -> List[Tuple[int, ...]]:
        return [u for u in self.units if not (set(u) & self.busy)]

    def acquire(self, n: int, threads: Optional[str] = None) -> List[int]:
        free = self._free_units()
        if n < 1 or len(free) < n:
            return []
        th = threads or self.threads                  # 這次要的執行緒模式（沒指定就用池的預設）
        got: List[int] = []
        for u in free[:n]:
            self.busy.update(u)                       # 兄弟執行緒一起占住
            got.extend(u if th == "all" else u[:1])
        return got

    def release(self, cores: Iterable[int]) -> None:
        for c in cores:
            self.busy.difference_update(self._unit_of.get(c, (c,)))

    def reserve(self, cores: Iterable[int]) -> bool:
        cores = set(cores)
        if not cores or not cores <= set(self.all):
            return False
        whole: Set[int] = set()
        for c in cores:
            whole |= set(self._unit_of.get(c, (c,)))          # 兄弟執行緒一起占住
        if whole & self.busy:
            return False
        self.busy.update(whole)
        return True

    def free_count(self) -> int:
        return len(self._free_units())


def proc_cpu_seconds(pid: int, proc_root: str = "/proc") -> float:
    """行程自己加已回收子行程用掉的 CPU 秒數（utime＋stime＋cutime＋cstime）。"""
    try:
        with open(f"{proc_root}/{pid}/stat", encoding="utf-8", errors="replace") as f:
            text = f.read()
        rest = text[text.rindex(")") + 2:].split()
        ticks = sum(int(rest[i]) for i in (11, 12, 13, 14))
        return ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError):
        return 0.0


def group_cpu_seconds(pgid: int, procs: Optional[List["ProcInfo"]] = None) -> float:
    """整個工作群組到現在用掉的 CPU 秒數：活行程自己的加上它們已回收的子行程。
    （已結束的行程時間會在其父行程的 cutime 裡，所以不會漏、也不會重複算。）"""
    return sum(proc_cpu_seconds(p.pid) for p in group_members(pgid, procs))


def cpu_pressure(path: str = "/proc/pressure/cpu") -> Optional[float]:
    """系統 CPU 壓力（PSI some avg10，百分比）；沒有 PSI 就回 None。"""
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.startswith("some"):
                    for kv in line.split():
                        if kv.startswith("avg10="):
                            return float(kv[6:])
    except (OSError, ValueError):
        pass
    return None


def system_overloaded(psi_threshold: float = 20.0, load_factor: float = 0.9) -> bool:
    """系統現在是不是超載：PSI 壓力超過門檻；沒有 PSI 就看 1 分鐘平均負載是否超過上線核數的 load_factor 倍。"""
    p = cpu_pressure()
    if p is not None:
        return p >= psi_threshold
    try:
        return os.getloadavg()[0] > load_factor * len(online_cpus())
    except OSError:
        return False


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


def _is_init(ppid: int, proc_root: str) -> bool:
    """親是 init／systemd（包含使用者層的收養者）：行程的原主人已經不在了。"""
    if ppid <= 1:
        return True
    try:
        with open(f"{proc_root}/{ppid}/comm", encoding="utf-8", errors="replace") as f:
            return f.read().strip() in ("systemd", "init", "tini", "dumb-init")
    except OSError:
        return True            # 親已經不在：也算孤兒


def own_leftovers(root, keep_pgids: Iterable[int] = (), proc_root: str = "/proc", extra_roots: Iterable[str] = (),
                  pool: Iterable[int] = ()) -> List[Tuple[ProcInfo, str]]:
    """上一代留下的自己人：工作目錄在我們的 worktree 底下、而且「原主人已經不在」的活行程。
    現役的測試（祖先鏈上還有排隊器）一律不碰：只收「這一串行程最上面那個的親是 init」的整串。
    領頭已死、子行程還在的群組也認得出來。不是這個目錄的行程一律不碰。keep_pgids＝再額外保護的群組。"""
    roots = tuple(os.path.abspath(r).rstrip(os.sep) + os.sep for r in ([root] if isinstance(root, str) else root))   # 一個或多個目錄
    extra = tuple(os.path.abspath(r).rstrip(os.sep) + os.sep for r in extra_roots)   # 範圍大的目錄（例如整個測試暫存碟）：行程還要「釘在池內的核上」才算，別人自己起的行程不碰
    poolset = set(pool)
    try:
        base = default_baseline()
    except Exception:
        base = set()
    keep = set(keep_pgids)
    me = os.getpid()
    cand: Dict[int, Tuple[ProcInfo, str]] = {}
    procs = scan_procs(proc_root)
    by_pid = {p.pid: p for p in procs}

    def protected(p: ProcInfo) -> bool:
        """自己或任何祖先屬於受保護的群組（被接管的工作）：整串都不碰，包含自己 setsid 脫離群組的子行程。"""
        q, hops = p, 0
        while q is not None and hops < 64:
            if q.pgrp in keep or q.sid in keep:
                return True
            q = by_pid.get(q.ppid)
            hops += 1
        return False

    for p in procs:
        if p.pid == me or p.state in ("Z", "X") or p.flags & PF_KTHREAD or (keep and protected(p)):
            continue
        try:
            cwd = os.readlink(f"{proc_root}/{p.pid}/cwd")
        except OSError:
            continue
        cw = cwd.rstrip(os.sep) + os.sep
        if cw.startswith(roots):
            cand[p.pid] = (p, cwd)
        elif extra and poolset and cw.startswith(extra):
            try:
                aff = set(os.sched_getaffinity(p.pid))
            except (OSError, AttributeError):
                continue
            if aff and aff < base and aff & poolset:      # 刻意釘過核、而且釘在池裡
                cand[p.pid] = (p, cwd)
    out: List[Tuple[ProcInfo, str]] = []
    for pid, (p, cwd) in cand.items():
        top, hops = p, 0
        while top.ppid in cand and hops < 64:      # 往上走到這串候選行程的最上面
            top, hops = cand[top.ppid][0], hops + 1
        if _is_init(top.ppid, proc_root):
            out.append((p, cwd))
    return out


def kill_leftovers(items: List[Tuple[ProcInfo, str]]) -> int:
    import signal
    n = 0
    for p, _ in items:
        try:
            os.kill(p.pid, signal.SIGKILL)
            n += 1
        except OSError:
            pass
    return n


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
    plist = scan_procs() if procs is None else procs
    by_pid = {q.pid: q for q in plist} if own_pgids else {}

    def ours(p: ProcInfo) -> bool:
        """自己或祖先在我方群組：測試行程自己 setsid 脫離群組的子孫（例如引擎）也算我方。"""
        q, hops = p, 0
        while q is not None and hops < 64:
            if q.pgrp in own_pgids or q.sid in own_pgids:
                return True
            q = by_pid.get(q.ppid)
            hops += 1
        return False

    for p in plist:
        if p.pid == me or p.state in ("Z", "X") or p.flags & PF_KTHREAD:
            continue
        if own_pgids and ours(p):
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


def pool_check(cfg, topo=None):
    """核池防呆（doctor 與 check-config 共用）。回傳 [(狀態, 訊息)]；狀態 fail／warn／ok。
    實體核模式：兄弟執行緒不在池裡的實體核整顆排除；可用實體核比單一工作要的少就 fail。"""
    need = max(cfg.selftest_cores, cfg.batch_cores)
    if cfg.core_unit != "physical":
        return [("ok", f"邏輯核模式，池 {len(cfg.pool)} 顆")] if len(cfg.pool) >= need else \
            [("fail", f"池只有 {len(cfg.pool)} 顆，單一工作要 {need} 顆")]
    pp = PhysicalPool(cfg.pool, cfg.threads, topo)
    n, ex = len(pp.units), pp.excluded
    exs = f"；因為兄弟執行緒不在池裡而被排除的核：{ex[:16]}{'…' if len(ex) > 16 else ''}（把兄弟也加進 cores.pool）" if ex else ""
    if n == 0:
        return [("fail", f"實體核模式下可用實體核是 0 顆，排隊器永遠派不出工作{exs}")]
    if n < need:
        return [("fail", f"實體核模式下可用實體核只有 {n} 顆，單一工作要 {need} 顆{exs}")]
    if ex:
        return [("warn", f"實體核模式：可用 {n} 顆實體核，另有 {len(ex)} 條邏輯核被排除{exs}")]
    return [("ok", f"實體核模式：可用 {n} 顆實體核")]
