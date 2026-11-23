"""jet doctor：上線前後「一條指令看哪裡不對」。只讀、不寫資料庫。
每項回 (狀態, 說明)；狀態 ok／warn／fail。"""
import fnmatch
import os
import re
import shutil
import subprocess
import sys
import time
from typing import Callable, List, Optional, Tuple

from . import config
from .cores import foreign_busy
from .gitops import MIN_GIT

Check = Tuple[str, str, str]   # (狀態, 名稱, 說明)


def _git_version() -> Optional[Tuple[int, int]]:
    try:
        out = subprocess.run(["git", "--version"], capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    m = re.search(r"(\d+)\.(\d+)", out)
    return (int(m.group(1)), int(m.group(2))) if m else None


def _ls_remote(remote: str, timeout: float) -> Tuple[Optional[List[str]], str, float]:
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0", LC_ALL="C")
    env.setdefault("GIT_SSH_COMMAND", "ssh -o BatchMode=yes -o ConnectTimeout=15")
    t0 = time.time()
    try:
        p = subprocess.Popen(["git", "ls-remote", "--heads", remote], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, env=env, start_new_session=True)
        try:
            out, err = p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, 9)
            p.communicate()
            return None, f"逾時（{timeout:.0f} 秒）：網路、ssh 金鑰或主機金鑰確認可能卡住", time.time() - t0
    except OSError as e:
        return None, str(e), time.time() - t0
    if p.returncode != 0:
        return None, err.strip()[:300], time.time() - t0
    names = [l.split("refs/heads/", 1)[1] for l in out.splitlines() if "refs/heads/" in l]
    return names, "", time.time() - t0


def run_checks(cfg: config.Config, remote_timeout: float = 60,
               ls_remote: Callable = _ls_remote) -> List[Check]:
    r: List[Check] = []
    add = lambda st, name, msg: r.append((st, name, msg))

    add("ok", "python", sys.version.split()[0])
    gv = _git_version()
    if gv is None:
        add("fail", "git", "找不到 git")
    elif gv < MIN_GIT:
        add("fail", "git", f"版本 {gv[0]}.{gv[1]} 太舊，需要 ≥ {MIN_GIT[0]}.{MIN_GIT[1]}")
    else:
        add("ok", "git", f"{gv[0]}.{gv[1]}")

    for label, d in (("state_dir", cfg.state_dir), ("work_dir", cfg.work_dir)):
        try:
            os.makedirs(d, exist_ok=True)
            probe = os.path.join(d, ".doctor_probe")
            open(probe, "w").close()
            os.unlink(probe)
            free = shutil.disk_usage(d).free // (1 << 20)
            st = "ok" if free >= cfg.min_free_disk_mb else "warn"
            add(st, label, f"{os.path.abspath(d)} 可寫，剩 {free} MB" +
                ("" if st == "ok" else f"（低於水位 {cfg.min_free_disk_mb} MB，排隊器會暫停派工）"))
        except OSError as e:
            add("fail", label, f"{d} 不能寫：{e}")

    add("ok", "設定檔",
        f"remote={cfg.remote}　main={cfg.main_branch}　分支樣式={cfg.branch_patterns}　fetch 間隔 {cfg.fetch_interval_s:g}s／逾時 {cfg.fetch_timeout_s:g}s")
    names, err, dt = ls_remote(cfg.remote, remote_timeout)
    if names is None:
        add("fail", "remote 連線", err)
    else:
        add("ok", "remote 連線", f"{dt:.1f} 秒，{len(names)} 條分支")
        if cfg.main_branch not in names:
            add("fail", "main", f"遠端沒有 {cfg.main_branch} 分支")
        match = [n for n in names if n != cfg.main_branch and any(fnmatch.fnmatchcase(n, p) for p in cfg.branch_patterns)]
        if match:
            add("ok", "分支樣式", f"遠端 {len(names)} 條中，符合樣式的 {len(match)} 條（例：{', '.join(sorted(match)[:3])}）")
        else:
            add("fail", "分支樣式", f"遠端 {len(names)} 條，沒有任何一條符合 {cfg.branch_patterns}"
                f"（遠端分支例：{', '.join(sorted(names)[:3])}）")

    mirror = os.path.join(cfg.work_dir, "mirror.git")
    if os.path.isdir(mirror):
        refs = subprocess.run(["git", "--git-dir", mirror, "for-each-ref", "--format=x", "refs/remotes/origin"],
                              capture_output=True, text=True).stdout.split()
        if refs:
            add("ok", "鏡像", f"{mirror} 有 {len(refs)} 個遠端分支（refs/remotes/origin/*）")
        else:
            add("warn", "鏡像", f"{mirror} 存在但沒有任何遠端分支：第一次 fetch 還在跑或失敗過（看 jet status --verbose）")
    else:
        add("warn", "鏡像", f"{mirror} 還不存在：排隊器第一次啟動時會建立並 fetch")

    cmd = cfg.test_cmd
    exe = cmd[0]
    if shutil.which(exe) or (os.path.isabs(exe) and os.access(exe, os.X_OK)):
        add("ok", "測試指令", f"{exe} 找得到；其餘參數（相對路徑的腳本）在每個 worktree 裡才找得到，這裡不驗")
    else:
        add("fail", "測試指令", f"找不到可執行檔 {exe}")
    for a in cmd[1:]:
        if os.path.isabs(a) and "{" not in a and not os.path.exists(a):
            add("fail", "測試指令", f"指令裡的絕對路徑不存在：{a}")
    if cfg.classify_cmd and not shutil.which(cfg.classify_cmd[0]):
        add("warn", "分類指令", f"找不到 {cfg.classify_cmd[0]}（找不到時一律當要測）")

    try:
        avail = os.sched_getaffinity(0)
        missing = [c for c in cfg.pool if c not in avail]
        if missing:
            add("fail", "核池", f"池裡有這台機器上不能用的核：{missing[:8]}")
        else:
            busy = sorted(foreign_busy(cfg.pool, set()))
            if busy:
                add("warn", "核池", f"{len(cfg.pool)} 顆核都存在，但現在有外人行程綁在 {busy[:8]}…（共 {len(busy)} 顆），派工會避開")
            else:
                add("ok", "核池", f"{len(cfg.pool)} 顆核可用，沒有外人占用")
        need = max(cfg.selftest_cores, cfg.batch_cores)
        if need > len(cfg.pool):
            add("fail", "核池", f"單一工作要 {need} 顆，池只有 {len(cfg.pool)} 顆")
    except (OSError, AttributeError) as e:
        add("warn", "核池", f"無法檢查：{e}")

    for label, f in (("讀鑰匙", cfg.read_key_file), ("寫鑰匙", cfg.write_key_file)):
        if not f:
            add("warn", label, "設定檔沒寫 api 鑰匙檔")
        elif not os.path.exists(f):
            add("fail", label, f"{f} 不存在（jet init-keys）")
        else:
            mode = os.stat(f).st_mode & 0o777
            add("ok" if mode & 0o077 == 0 else "warn", label, f"{f} 權限 {oct(mode)}")
    return r


def format_checks(checks: List[Check]) -> str:
    sym = {"ok": "✓", "warn": "!", "fail": "✗"}
    return "\n".join(f"{sym[s]} {n}：{m}" for s, n, m in checks)
