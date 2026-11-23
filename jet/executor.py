"""執行器：把一個測試工作綁到專屬核上跑（DESIGN 第 5、6 節）。骨架：只定介面。"""
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Job:
    layer: str                  # selftest | gate | full
    workdir: str                # 被測提交的 worktree
    cores: List[int]
    timeout_s: int
    report_path: str
    log_path: str
    only: Optional[str] = None  # 插隊跑指定測試項目


@dataclass
class JobResult:
    status: str                 # pass | fail | timeout | infra | cancelled
    exit_code: Optional[int]
    failed_items: List[str] = field(default_factory=list)
    duration_s: float = 0.0


class Executor:
    """start(job)→handle；poll(handle)→JobResult|None；cancel(handle)。
    每個工作自己一個行程群組、sched_setaffinity 綁核；
    逾時或取消殺整個群組；退出碼 2＝infra（不算紅，重跑）。"""

    def start(self, job: Job): raise NotImplementedError
    def poll(self, handle): raise NotImplementedError
    def cancel(self, handle): raise NotImplementedError
