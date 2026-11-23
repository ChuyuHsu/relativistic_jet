"""批測的純邏輯（DESIGN 第 4 節）：測點階梯、判決、找兇手。

不碰 Git、不碰行程；輸入是每個測點的結果，輸出是下一步該做什麼。
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional

GREEN, RED, PENDING = "green", "red", "pending"


def ladder(n: int, points=(2, 4, 8, 16)) -> List[int]:
    """n 條分支要測哪幾個點：階梯中小於 n 的，再加 n 本身。"""
    if n <= 0:
        return []
    pts = sorted({p for p in points if p < n} | {n})
    return pts


@dataclass
class Verdict:
    state: str                      # wait | probe | done
    merge_upto: int = 0             # done：快轉到前 merge_upto 條（0＝都不合）
    culprit: Optional[int] = None   # done：兇手是第幾條（1 起算）
    probe: List[int] = field(default_factory=list)  # probe：要加測的前綴大小


def judge(n: int, results: Dict[int, str], probes: Optional[Dict[int, str]] = None,
          points=(2, 4, 8, 16)) -> Verdict:
    """依四點結果判決。results：前綴大小→green/red/pending。

    - 全綠：整批合入。
    - 有紅：g＝最大綠點、r＝最小紅點；兇手在 g+1..r 之間。
      g+1..r-1 逐條加測（probes），最小的紅前綴 c 是兇手，否則 r 是兇手。
    - 還有 pending 就等（infra 錯誤也算 pending，由呼叫端重跑）。
    """
    pts = ladder(n, points)
    if any(results.get(p, PENDING) == PENDING for p in pts):
        return Verdict("wait")
    reds = [p for p in pts if results[p] == RED]
    greens = [p for p in pts if results[p] == GREEN]
    if not reds:
        return Verdict("done", merge_upto=n)
    r = min(reds)
    g = max([p for p in greens if p < r], default=0)
    cands = list(range(g + 1, r))
    probes = probes or {}
    if cands:
        if any(probes.get(c, PENDING) == PENDING for c in cands):
            return Verdict("probe", probe=cands)
        red_c = [c for c in cands if probes[c] == RED]
        c = min(red_c) if red_c else r
    else:
        c = r
    return Verdict("done", merge_upto=c - 1, culprit=c)
