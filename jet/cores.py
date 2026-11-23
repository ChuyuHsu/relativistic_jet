"""核池（DESIGN 第 6 節）：每個工作專屬核，任何時刻不共用。"""
import os
from typing import Iterable, List, Set


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


def foreign_busy(cores: Iterable[int], own_pgids: Set[int]) -> Set[int]:
    """TODO(執行器串)：掃 /proc，找不屬於我方行程群組、而且綁在這些核上的行程。
    派工前呼叫，回傳被別人佔用的核。骨架先回空集合。"""
    return set()


def pin(cores: Iterable[int]) -> None:
    """子行程啟動前呼叫：只准在這幾顆核上跑。"""
    os.sched_setaffinity(0, set(cores))
