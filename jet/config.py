"""設定檔（JSON）載入與檢查（DESIGN 第 5、6、9 節）。壞的不載。"""
import json
import os
from dataclasses import dataclass, field
from typing import List, Optional

from .cores import parse_cores


class ConfigError(Exception):
    pass


@dataclass
class Config:
    remote: str
    main_branch: str = "main"
    branch_patterns: List[str] = field(default_factory=lambda: ["*"])
    scope: str = "all"              # all＝符合樣式的都管；tracked＝只管登記過的
    fetch_interval_s: float = 15
    git_timeout_s: float = 900
    fetch_timeout_s: float = 3600
    state_dir: str = "state"
    work_dir: str = "work"
    pool: List[int] = field(default_factory=list)
    selftest_cores: int = 1
    batch_cores: int = 1
    reserve_for_selftest: int = 0
    core_unit: str = "logical"        # logical＝以邏輯核計；physical＝以實體核為單位配槽
    threads: str = "all"             # physical 時：all＝實體核的兄弟執行緒都給同一個槽；one＝只給一條，另一條空著
    max_selftest_slots_with_batch: int = 0   # 有批測在跑或在等時，自測最多幾槽；0＝不另外限制
    max_selftest_slots: int = 0       # 同時自測槽數上限；0＝不限（只受核數限制）
    selftest_timeout_s: int = 600
    batch_timeout_s: int = 3600
    batch_max: int = 16
    handover: bool = True              # jet restart 時測試不排空不殺，留給下一代主行程接管
    drain_max_s: float = 300           # 優雅停機／重啟最多等手上的工作多久，超過就立刻收（工作留給啟動復原重排）；0＝一直等
    points: List[int] = field(default_factory=lambda: [2, 4, 8, 16])
    double_fail_halt: int = 2
    baseline_history: int = 5
    min_free_mem_mb: int = 0
    min_free_disk_mb: int = 0
    require_approval: bool = True
    test_cmd: List[str] = field(default_factory=list)
    classify_cmd: Optional[List[str]] = None
    times_file: Optional[str] = None  # 逐項耗時檔（相對工作目錄的 glob，TSV：項目\t開始\t結束，epoch 秒）；有設才記背景 loop 的逐項耗時
    metrics_file: Optional[str] = None  # 逐項畫面量測檔（相對工作目錄的 glob，JSON，格式見 docs/WORKFLOW.md）
    artifacts: Optional[List[str]] = None   # 測試跑完（含逾時）要留下的檔案（相對工作目錄的 glob）
    notify_url: Optional[str] = None
    notify_batch_s: int = 180
    notify_destinations: List[dict] = field(default_factory=list)   # 多個通知目的地（選配）
    snapshot_s: int = 300             # 每幾秒送一份現況快照（state_snapshot）給看板對帳；0＝不送
    listen: str = "127.0.0.1:8080"
    read_key_file: Optional[str] = None
    write_key_file: Optional[str] = None
    flaky_retry: bool = True          # 計時類項目（timing_items）新紅：先在同一個提交上只重跑那幾項，第二次綠就當抖動放過（門檻不放寬）
    gate_registry_dir: str = ""      # 例如 tests/shops：裡面 "gate": false 的店，項目名含該店 id 的紅項不算「main 自己紅」（只記錄）；空＝不啟用
    workflow_mode: str = "gate_batch"   # gate_batch＝批測閘門（預設）｜direct_merge＝自測綠就直接合，背景 loop 測最新 main、紅了自動二分找兇手
    loop_enabled: bool = True         # direct_merge 時是否跑背景 loop
    loop_cores: int = 0               # 背景 loop 每個工作用的核數；0＝同 cores.batch_cores
    loop_triage: str = "bisect"      # 背景測試紅了先怎麼找兇手：bisect＝直接二分；code_first＝先發 loop_triage 事件等看板回嫌疑分支（只驗證一次），逾時才二分
    loop_triage_wait_s: int = 900
    loop_probe_parallel: int = 1      # 背景二分同時開幾個探測（1＝一次一個；2～3 讓二分更快、機器更滿）
    loop_timeout_s: int = 0           # 0＝同 limits 的批測逾時
    nightly_at: str = ""              # 每日完整層時刻 "HH:MM"（本機時間）；空＝不跑。到點對 main 最新版跑 full 層一次，紅了只記錄、不擋 main、不二分
    nightly_cores: int = 0            # 完整層用的核數；0＝同 cores.batch_cores
    nightly_timeout_s: int = 0        # 完整層逾時；0＝同 limits 的批測逾時
    main_red_ignore: tuple = ()       # 項目名符合其中一個正規式的，乾淨 main 上也紅時只記錄、不算「main 自己紅」（不擋批次）；預設空＝全部都算
    timing_items: tuple = ("loadtime", "perf", "timeout", "card_touch", "godot_time_")   # 項目名符合其中一個正規式就算計時類
    relative_ratio: float = 0.0       # >0＝計時類項目（timing_items）改比相對值：超過「最近幾輪 main 基準的中位數」這麼多倍才算紅；0＝不啟用
    relative_min_extra: float = 0.0   # 相對判定的下限：除了超過倍數，還要比基準多至少這麼多（單位同項目數字，例如毫秒），防抖動
    relative_keep: int = 5            # 每個計時項目記最近幾輪 main 基準的數字
    relative_value_re: str = r"ms=(\d+(?:\.\d+)?)"   # 從項目摘要抽出數字的正規式（第 1 組）
    recheck_red: bool = True          # 自測紅時，先把新紅項放在乾淨 main 上單獨重跑；main 也紅就不算這張卡的錯
    order: str = "parallel"           # review_first＝先審後測；parallel＝不分先後
    source: Optional[dict] = None     # 外部來源（選配）：{url, token_file, poll_seconds, max_age_s, timeout_s}


def _nightly_at(v):
    import re
    v = "" if v is None else str(v).strip()
    if v and not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", v):
        raise ConfigError('workflow.nightly.at 必須是 "HH:MM"（24 小時制）或空字串')
    return v


def _snap_s(v):
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
        raise ConfigError("notify.snapshot_seconds 必須是 ≥0 的數字（0＝不送）")
    return int(v)


def _need(d, k, typ):
    if k not in d:
        raise ConfigError(f"缺少欄位：{k}")
    if not isinstance(d[k], typ):
        raise ConfigError(f"欄位型別錯：{k}")
    return d[k]


def parse(d: dict) -> Config:
    if not isinstance(d, dict):
        raise ConfigError("設定檔最外層必須是物件")
    git = _need(d, "git", dict)
    paths = d.get("paths", {})
    cores = _need(d, "cores", dict)
    lim = d.get("limits", {})
    pol = d.get("policy", {})
    cmds = _need(d, "commands", dict)
    notify = d.get("notify", {})
    api = d.get("api", {})
    try:
        pool = parse_cores(_need(cores, "pool", str))
    except ValueError as e:
        raise ConfigError(str(e))
    cfg = Config(
        remote=_need(git, "remote", str),
        main_branch=git.get("main", "main"),
        branch_patterns=git.get("branch_patterns", ["*"]),
        scope=git.get("scope", "all"),
        fetch_interval_s=git.get("fetch_interval_s", 15),
        git_timeout_s=git.get("timeout_s", 900),
        fetch_timeout_s=git.get("fetch_timeout_s", 3600),
        state_dir=paths.get("state_dir", "state"),
        work_dir=paths.get("work_dir", "work"),
        pool=pool,
        selftest_cores=cores.get("selftest_cores", 1),
        batch_cores=cores.get("batch_cores", 1),
        reserve_for_selftest=cores.get("reserve_for_selftest", 0),
        core_unit=cores.get("unit", "logical"),
        threads=cores.get("threads", "all"),
        max_selftest_slots=cores.get("max_selftest_slots", 0),
        max_selftest_slots_with_batch=cores.get("max_selftest_slots_with_batch", 0),
        selftest_timeout_s=lim.get("selftest_timeout_s", 600),
        batch_timeout_s=lim.get("batch_timeout_s", 3600),
        batch_max=lim.get("batch_max", 16),
        drain_max_s=lim.get("drain_max_s", 300),
        handover=lim.get("handover", True),
        points=lim.get("points", [2, 4, 8, 16]),
        double_fail_halt=lim.get("double_fail_halt", 2),
        baseline_history=lim.get("baseline_history", 5),
        min_free_mem_mb=lim.get("min_free_mem_mb", 0),
        min_free_disk_mb=lim.get("min_free_disk_mb", 0),
        require_approval=pol.get("require_approval", True),
        test_cmd=_need(cmds, "test", list),
        classify_cmd=cmds.get("classify"),
        artifacts=cmds.get("artifacts"),
        times_file=cmds.get("times_file"),
        metrics_file=cmds.get("metrics_file"),
        notify_url=notify.get("url"),
        notify_batch_s=notify.get("batch_seconds", 60),
        notify_destinations=_parse_destinations(notify.get("destinations")),
        snapshot_s=_snap_s(notify.get("snapshot_seconds", 300)),
        listen=api.get("listen", "127.0.0.1:8080"),
        read_key_file=api.get("read_key_file"),
        write_key_file=api.get("write_key_file"),
        order=pol.get("order", "parallel"),
        recheck_red=pol.get("recheck_red", True),
        flaky_retry=pol.get("flaky_retry", True),
        main_red_ignore=tuple(pol.get("main_red_ignore", ())),
        workflow_mode=str((d.get("workflow") or {}).get("mode", "gate_batch")),
        loop_enabled=bool(((d.get("workflow") or {}).get("loop") or {}).get("enabled", True)),
        loop_cores=int(((d.get("workflow") or {}).get("loop") or {}).get("cores", 0)),
        loop_triage=str(((d.get("workflow") or {}).get("loop") or {}).get("triage", "bisect")),
        loop_triage_wait_s=int(((d.get("workflow") or {}).get("loop") or {}).get("triage_wait_s", 900)),
        loop_probe_parallel=int(((d.get("workflow") or {}).get("loop") or {}).get("probe_parallel", 1)),
        loop_timeout_s=int(((d.get("workflow") or {}).get("loop") or {}).get("timeout_s", 0)),
        nightly_at=_nightly_at((((d.get("workflow") or {}).get("nightly") or {}).get("at", ""))),
        nightly_cores=int(((d.get("workflow") or {}).get("nightly") or {}).get("cores", 0)),
        nightly_timeout_s=int(((d.get("workflow") or {}).get("nightly") or {}).get("timeout_s", 0)),
        gate_registry_dir=str(pol.get("gate_registry_dir", "")),
        timing_items=tuple(pol.get("timing_items", ("loadtime", "perf", "timeout", "card_touch", "godot_time_"))),
        relative_ratio=float(pol.get("relative_ratio", 0.0)),
        relative_min_extra=float(pol.get("relative_min_extra", 0.0)),
        relative_keep=int(pol.get("relative_keep", 5)),
        relative_value_re=str(pol.get("relative_value_re", r"ms=(\d+(?:\.\d+)?)")),
        source=_parse_source(d.get("source")),
    )
    _validate(cfg)
    return cfg


def _parse_destinations(d):
    from . import notify as _n
    try:
        return _n.check_destinations(d)
    except _n.NotifyError as ex:
        raise ConfigError(f"notify.destinations：{ex}")


def _parse_source(e):
    if e is None:
        return None
    if not isinstance(e, dict):
        raise ConfigError("source 必須是物件")
    from . import notify as _n
    out = {"url": e.get("url"), "token_file": e.get("token_file"), "poll_seconds": e.get("poll_seconds", 15),
           "max_age_s": e.get("max_age_s", 600), "timeout_s": e.get("timeout_s", 10)}
    try:
        _n.check_bearer_url(out["url"]) if out["token_file"] else _n.check_url(out["url"] or None)
    except _n.NotifyError as ex:
        raise ConfigError(f"source.url：{ex}")
    if not out["url"]:
        raise ConfigError("source.url 必填")
    if out["token_file"] is not None and (not isinstance(out["token_file"], str) or not out["token_file"]):
        raise ConfigError("source.token_file 必須是檔案路徑字串")
    for k in ("poll_seconds", "max_age_s", "timeout_s"):
        v = out[k]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0:
            raise ConfigError(f"source.{k} 必須 > 0")
    return out


def _validate(c: Config) -> None:
    if not c.test_cmd or not all(isinstance(x, str) for x in c.test_cmd):
        raise ConfigError("commands.test 必須是非空字串陣列")
    if c.artifacts is not None and not all(isinstance(x, str) and x and not os.path.isabs(x) and ".." not in x.split("/")
                                           for x in c.artifacts):
        raise ConfigError("commands.artifacts 必須是相對路徑的 glob 字串陣列（不能用絕對路徑或 ..）")
    if c.classify_cmd is not None and not all(isinstance(x, str) for x in c.classify_cmd):
        raise ConfigError("commands.classify 必須是字串陣列")
    if min(c.fetch_interval_s, c.git_timeout_s, c.fetch_timeout_s) <= 0:
        raise ConfigError("git.fetch_interval_s／timeout_s／fetch_timeout_s 必須 > 0")
    if not c.branch_patterns or not all(isinstance(x, str) for x in c.branch_patterns):
        raise ConfigError("git.branch_patterns 必須是非空字串陣列")
    if c.scope not in ("all", "tracked"):
        raise ConfigError('git.scope 必須是 "all" 或 "tracked"')
    if not 1 <= c.loop_probe_parallel <= 4:
        raise ConfigError("workflow.loop.probe_parallel 要是 1～4 的整數")
    if c.relative_ratio < 0 or (c.relative_ratio and c.relative_ratio < 1) or c.relative_min_extra < 0 or c.relative_keep < 1:
        raise ConfigError("policy.relative_ratio 要是 0（不啟用）或 ≥1；relative_min_extra ≥0；relative_keep ≥1")
    try:
        import re as _re
        if _re.compile(c.relative_value_re).groups < 1:
            raise ConfigError("policy.relative_value_re 要有一個括號組（抽數字）")
    except _re.error:
        raise ConfigError("policy.relative_value_re 不是合法的正規式")
    if not isinstance(c.flaky_retry, bool):
        raise ConfigError("policy.flaky_retry 必須是 true 或 false")
    if c.workflow_mode not in ("gate_batch", "direct_merge"):
        raise ConfigError("workflow.mode 只能是 gate_batch 或 direct_merge")
    if c.loop_triage not in ("bisect", "code_first"):
        raise ConfigError("workflow.loop.triage 只能是 bisect 或 code_first")
    import re as _re
    for pat in c.main_red_ignore:
        try:
            _re.compile(pat)
        except (_re.error, TypeError):
            raise ConfigError("policy.main_red_ignore 每個都要是正規式字串：%r" % (pat,))
    for pat in c.timing_items:
        try:
            _re.compile(pat)
        except (TypeError, _re.error):
            raise ConfigError("policy.timing_items 每個都要是正規式字串：%r" % (pat,))
    if not isinstance(c.recheck_red, bool):
        raise ConfigError("policy.recheck_red 必須是 true 或 false")
    if c.order not in ("parallel", "review_first"):
        raise ConfigError('policy.order 必須是 "parallel" 或 "review_first"')
    if c.order == "review_first" and not c.require_approval:
        raise ConfigError("policy.order=review_first 需要 policy.require_approval=true")
    if c.source is not None and c.order != "review_first":
        raise ConfigError('有設 source 時 policy.order 必須是 "review_first"（清單搭先審後測）')
    if c.source is not None and c.scope != "tracked":
        raise ConfigError('有設 source 時 git.scope 必須是 "tracked"（清單裡有的才管）')
    if c.core_unit not in ("logical", "physical") or c.threads not in ("all", "one"):
        raise ConfigError('cores.unit 要是 "logical" 或 "physical"，cores.threads 要是 "all" 或 "one"')
    if not isinstance(c.max_selftest_slots_with_batch, int) or c.max_selftest_slots_with_batch < 0:
        raise ConfigError("cores.max_selftest_slots_with_batch 不能是負的")
    if c.max_selftest_slots < 0:
        raise ConfigError("cores.max_selftest_slots 不能是負的")
    if c.baseline_history < 1:
        raise ConfigError("limits.baseline_history 必須 ≥ 1")
    if isinstance(c.drain_max_s, bool) or not isinstance(c.drain_max_s, (int, float)) or c.drain_max_s < 0:
        raise ConfigError("limits.drain_max_s 必須 ≥ 0（0＝一直等）")
    if not isinstance(c.handover, bool):
        raise ConfigError("limits.handover 必須是 true 或 false")
    if c.batch_max < 1:
        raise ConfigError("limits.batch_max 必須 ≥ 1")
    if sorted(c.points) != c.points or any(p < 1 for p in c.points):
        raise ConfigError("limits.points 必須由小到大、且 ≥ 1")
    if c.selftest_cores < 1 or c.batch_cores < 1:
        raise ConfigError("每個工作至少 1 顆核")
    need = c.batch_cores * len([p for p in set(c.points) | {c.batch_max} if p <= c.batch_max])
    if len(c.pool) < c.selftest_cores or len(c.pool) < c.batch_cores:
        raise ConfigError("核池比單一工作還小")
    if c.selftest_timeout_s <= 0 or c.batch_timeout_s <= 0:
        raise ConfigError("逾時必須 > 0")
    del need


def load(path: str) -> Config:
    try:
        with open(path, encoding="utf-8") as f:
            return parse(json.load(f))
    except json.JSONDecodeError as e:
        raise ConfigError(f"JSON 格式錯：{e}")


def safe_reload(path: str, old: Optional[Config]) -> Config:
    """換設定：新的壞了就沿用舊的。"""
    try:
        return load(path)
    except (ConfigError, OSError):
        if old is None:
            raise
        return old
