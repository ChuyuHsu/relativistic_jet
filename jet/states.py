"""分支狀態機（DESIGN 第 3 節）。純函式，不碰資料庫。"""
from enum import Enum


class State(str, Enum):
    PENDING = "pending"            # 待自測
    SELFTESTING = "selftesting"    # 自測中
    SELFTEST_GREEN = "selftest_green"  # 自測綠
    QUEUED = "queued"              # 排隊
    BATCHING = "batching"          # 批測中
    MERGED = "merged"              # 已合入
    REJECTED = "rejected"          # 退回
    HALTED = "halted"              # 批測紅兩次，暫停等人


class Event(str, Enum):
    PUSHED = "pushed"                  # 推了新提交（內容變了）
    SELFTEST_START = "selftest_start"
    SELFTEST_PASS = "selftest_pass"
    SELFTEST_FAIL = "selftest_fail"    # 紅或逾時
    READY = "ready"                    # 自測綠且放行（或設定不需放行）
    APPROVE = "approve"
    REVOKE = "revoke"
    BATCH_START = "batch_start"
    BATCH_MERGED = "batch_merged"
    BATCH_DEFERRED = "batch_deferred"  # 沒輪到／批作廢，回隊伍
    BATCH_CULPRIT = "batch_culprit"    # 是它紅
    CONFLICT_MAIN = "conflict_main"    # 跟 main 撞檔
    DOUBLE_FAIL = "double_fail"        # 批測紅兩次
    PARENT_CHANGED = "parent_changed"  # 父分支被退回或有新提交
    RESUME = "resume"                  # 人工恢復
    RETRY = "retry"                    # 基礎設施錯誤，自測重跑
    DOCS_MERGED = "docs_merged"        # 純文件分支放行後直接合入


class IllegalTransition(Exception):
    pass


S, E = State, Event
_T = {
    (S.PENDING, E.SELFTEST_START): S.SELFTESTING,
    (S.SELFTESTING, E.SELFTEST_PASS): S.SELFTEST_GREEN,
    (S.SELFTESTING, E.SELFTEST_FAIL): S.REJECTED,
    (S.SELFTEST_GREEN, E.READY): S.QUEUED,
    (S.QUEUED, E.BATCH_START): S.BATCHING,
    (S.BATCHING, E.BATCH_MERGED): S.MERGED,
    (S.BATCHING, E.BATCH_DEFERRED): S.QUEUED,
    (S.BATCHING, E.BATCH_CULPRIT): S.REJECTED,
    (S.BATCHING, E.DOUBLE_FAIL): S.HALTED,
    (S.QUEUED, E.CONFLICT_MAIN): S.REJECTED,
    (S.HALTED, E.RESUME): S.PENDING,
    (S.REJECTED, E.RESUME): S.PENDING,          # 重測：清掉退回，從待自測重來
    (S.PENDING, E.DOCS_MERGED): S.MERGED,
    (S.SELFTESTING, E.RETRY): S.PENDING,
    (S.PENDING, E.CONFLICT_MAIN): S.REJECTED,   # 自測前疊不上 main
}
# 新提交：任何非終態都回待自測（已合入除外）
for _s in (S.PENDING, S.SELFTESTING, S.SELFTEST_GREEN, S.QUEUED,
           S.BATCHING, S.REJECTED, S.HALTED):
    _T[(_s, E.PUSHED)] = S.PENDING
# 父分支變動：子分支回待自測（第 3 節「堆疊分支」）
for _s in (S.SELFTESTING, S.SELFTEST_GREEN, S.QUEUED):
    _T[(_s, E.PARENT_CHANGED)] = S.PENDING
# 放行／撤回只改記號，不改狀態（排隊中撤回→回自測綠）
_T[(S.QUEUED, E.REVOKE)] = S.SELFTEST_GREEN
_NOOP = {E.APPROVE, E.REVOKE}

TERMINAL = {S.MERGED}


def transition(state: State, event: Event) -> State:
    """回傳新狀態；不合法就丟 IllegalTransition。"""
    state, event = State(state), Event(event)
    if (state, event) in _T:
        return _T[(state, event)]
    if event in _NOOP and state not in TERMINAL:
        return state
    raise IllegalTransition(f"{state.value} + {event.value}")
