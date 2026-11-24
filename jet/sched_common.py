"""排程器各模組共用的小東西：常數、狀態縮寫、執行中工作與批的資料類別。"""
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from .states import State

log = logging.getLogger("jet")
S = State
MAX_OVERLOAD_RETRY = 2        # 超載造成的逾時最多重排幾次，再逾時就當是測試自己慢
UNKNOWN_GRACE_S = 120         # 掃過遠端後，放行版本（不是現在的頭）還解不出來超過這麼久，才當「內容不同」


@dataclass
class Running:
    handle: object
    ctx: object
    cores: List[int]
    run_id: int
    kind: str                 # selftest | point | probe | baseline
    branch: Optional[str] = None
    head_sha: Optional[str] = None
    size: Optional[int] = None
    batch_id: Optional[int] = None
    units: int = 0            # 占了幾個核單位（邏輯核或實體核，依設定）
    approved_sha: Optional[str] = None   # 開跑時依據的放行版本與重測計數（事件帶這個，不帶送出當下的）
    retest: Optional[int] = None
    workdir: str = ""


class _NoCtx:
    def __enter__(self):
        return None

    def __exit__(self, *a):
        return None


@dataclass
class ActiveBatch:
    id: int
    base_sha: str
    members: List[Tuple[str, str, str]]       # (name, content_id, head_sha)
    prefixes: List[str]
    results: Dict[int, str] = field(default_factory=dict)
    probes: Dict[int, str] = field(default_factory=dict)
    failed: Dict[int, Set[str]] = field(default_factory=dict)   # 大小 → 新增的紅項
    launched: Set[Tuple[str, int]] = field(default_factory=set)
    ctx: Dict[str, Tuple[Optional[str], Optional[int]]] = field(default_factory=dict)   # 成員 → 組批時的 (放行版本, 重測)
