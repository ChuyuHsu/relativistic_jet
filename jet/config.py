"""設定檔（JSON）載入與檢查（DESIGN 第 5、6、9 節）。壞的不載。"""
import json
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
    state_dir: str = "state"
    work_dir: str = "work"
    pool: List[int] = field(default_factory=list)
    selftest_cores: int = 1
    batch_cores: int = 1
    reserve_for_selftest: int = 0
    selftest_timeout_s: int = 600
    batch_timeout_s: int = 3600
    batch_max: int = 16
    points: List[int] = field(default_factory=lambda: [2, 4, 8, 16])
    double_fail_halt: int = 2
    min_free_mem_mb: int = 0
    min_free_disk_mb: int = 0
    require_approval: bool = True
    test_cmd: List[str] = field(default_factory=list)
    classify_cmd: Optional[List[str]] = None
    notify_url: Optional[str] = None
    notify_batch_s: int = 180
    listen: str = "127.0.0.1:8080"
    read_key_file: Optional[str] = None
    write_key_file: Optional[str] = None


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
        state_dir=paths.get("state_dir", "state"),
        work_dir=paths.get("work_dir", "work"),
        pool=pool,
        selftest_cores=cores.get("selftest_cores", 1),
        batch_cores=cores.get("batch_cores", 1),
        reserve_for_selftest=cores.get("reserve_for_selftest", 0),
        selftest_timeout_s=lim.get("selftest_timeout_s", 600),
        batch_timeout_s=lim.get("batch_timeout_s", 3600),
        batch_max=lim.get("batch_max", 16),
        points=lim.get("points", [2, 4, 8, 16]),
        double_fail_halt=lim.get("double_fail_halt", 2),
        min_free_mem_mb=lim.get("min_free_mem_mb", 0),
        min_free_disk_mb=lim.get("min_free_disk_mb", 0),
        require_approval=pol.get("require_approval", True),
        test_cmd=_need(cmds, "test", list),
        classify_cmd=cmds.get("classify"),
        notify_url=notify.get("url"),
        notify_batch_s=notify.get("batch_seconds", 180),
        listen=api.get("listen", "127.0.0.1:8080"),
        read_key_file=api.get("read_key_file"),
        write_key_file=api.get("write_key_file"),
    )
    _validate(cfg)
    return cfg


def _validate(c: Config) -> None:
    if not c.test_cmd or not all(isinstance(x, str) for x in c.test_cmd):
        raise ConfigError("commands.test 必須是非空字串陣列")
    if c.classify_cmd is not None and not all(isinstance(x, str) for x in c.classify_cmd):
        raise ConfigError("commands.classify 必須是字串陣列")
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
