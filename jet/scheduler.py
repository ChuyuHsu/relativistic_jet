"""主迴圈（DESIGN 第 3、4 節）。單執行緒；狀態寫入都在這裡。

一輪 tick：
 1. 掃 Git：新分支／新內容／消失的分支。
 2. 父分支被退回的子孫回待自測。
 3. 收完成的工作（自測、批測點、基準）。
 4. 放行＋自測綠 → 排隊；純文件分支放行後直接合入。
 5. （沒暫停、水位夠、基準在）推進批測；派自測。
"""
import json
import logging
import os
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple

from . import executor, store
from .batch import GREEN, RED, PENDING, ladder, judge
from .cores import CorePool, PhysicalPool, foreign_busy as _foreign_busy, own_leftovers, kill_leftovers
from .executor import Job
from .gitops import MainMoved
from .states import Event, State

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


def make_pool(cfg):
    """依設定建核池：unit=physical 時以實體核為單位（兄弟執行緒不分給兩個槽）。"""
    if cfg.core_unit == "physical":
        return PhysicalPool(cfg.pool, cfg.threads)
    return CorePool(cfg.pool)


def default_resources_ok(cfg) -> bool:
    try:
        if cfg.min_free_mem_mb:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        if int(line.split()[1]) // 1024 < cfg.min_free_mem_mb:
                            return False
        if cfg.min_free_disk_mb:
            os.makedirs(cfg.state_dir, exist_ok=True)
            if shutil.disk_usage(cfg.state_dir).free // (1 << 20) < cfg.min_free_disk_mb:
                return False
    except OSError:
        return True
    return True


class Scheduler:
    def __init__(self, cfg, con, repo, executor, clock: Callable[[], float] = time.time,
                 resources_ok: Optional[Callable[[], bool]] = None,
                 foreign_busy=_foreign_busy, async_fetch: bool = False, source=None):
        self.cfg, self.con, self.repo, self.executor = cfg, con, repo, executor
        self.clock = clock
        self.resources_ok = resources_ok or (lambda: default_resources_ok(cfg))
        self.foreign_busy = foreign_busy
        self.pool = make_pool(cfg)
        self.jobs: Dict[int, Running] = {}
        self.batch: Optional[ActiveBatch] = None
        self.main: str = ""
        self._bad: Optional[Set[int]] = None
        self._start_fail = 0
        self._infra_streak = 0
        self._overload: Dict[str, int] = {}
        if source is None and cfg.source:
            from .source import SourcePoller
            sc = cfg.source
            source = SourcePoller(sc["url"], sc["token_file"], sc["poll_seconds"], sc["timeout_s"])
        self.source = source                # SourcePoller 或 None（外部來源）
        self._drop_sig = None
        self._rc: Dict[str, dict] = {}                     # 複驗中的卡 → 原本的自測結果
        self._rc_wait: Dict[str, dict] = {}                # 複驗還沒排到核
        self._retried: Set[Tuple[str, str]] = set()        # 已做過計時類重跑的（分支, 提交）：同一版本只重跑一次
        self._pretry: Dict[int, Tuple[str, int]] = {}      # 批測點的計時類重跑：run id → (kind, size)
        self._pretried: Set[Tuple[int, str, int]] = set()  # (batch id, kind, size)
        self._entries: Optional[Dict[str, dict]] = None    # 清單最後一次成功的內容（依分支名）
        self._src_err_last: Optional[str] = None
        self._unres_since: Dict[Tuple[str, str], float] = {}
        self._tips: Optional[Dict[str, str]] = None        # 本程序最近一次成功掃到的遠端分支頭（沒掃過是 None）
        self._unres: Dict[Tuple[str, str], float] = {}     # 解不出來的放行版本：稍後再試
        self._repulled = False                             # 這一輪是不是已經重拉過清單
        self._repull_ok = False
        self._repull_retry_at = 0.0
        self._anc_cache: Dict[Tuple[str, str], bool] = {}
        self._cls_ctx = self._cls_dir = self._cls_sha = None   # 分類指令用的 main 檢出（換 main 才換）
        self._wait_proven: Set[str] = set()                # 因父卡還沒綠而被重排的子分支：父卡綠了才再測
        self._blocked: Dict[str, str] = {}                 # 被父卡擋住的分支 → 原因
        self._stop = False
        self.draining = False               # 停機中：不再起新的批與自測，只把手上的做完
        self.async_fetch = async_fetch      # 正式跑用背景執行緒 fetch，第一次抓一個大 repo 不會卡住主迴圈
        self._fetch_thread: Optional[threading.Thread] = None
        self._fetch_started = 0.0
        self._fetch_res = None
        self._fetch_done_once = False
        self._last_err_msg: Optional[str] = None
        self.recover()

    # ---------- 啟動復原 ----------
    def recover(self):
        """啟動復原：上一代主行程留下的測試（行程群組還活著、或已經寫了 exit.json）接管下來繼續收結果；
        接不了的才殺掉、作廢、狀態退回上一步。
        例外：已經推進 main 的批（status=pushing 而且終點已是 main 的祖先）照合入收尾。"""
        c = self.con
        adopted = self._adopt_runs() if hasattr(self.executor, "adopt") else {}
        keep_pg = {j.handle.pgid for j in adopted.values()}
        self._kill_leftovers(keep_pg)
        cl = getattr(self.repo, "cleanup_worktrees", None)
        if cl:
            try:
                cl(keep=[j.workdir for j in adopted.values() if j.workdir])
            except TypeError:
                cl()
        c.execute("UPDATE runs SET status='cancelled', finished_at=? WHERE status IN ('running','queued')"
                  + (" AND id NOT IN (%s)" % ",".join(str(r) for r in adopted) if adopted else ""),
                  (self.clock(),))
        for b in c.execute("SELECT * FROM batches WHERE status='pushing'").fetchall():
            self._recover_pushing(b)
        keep_batch = self.batch.id if self.batch is not None else None
        c.execute("UPDATE batches SET status='aborted', finished_at=? WHERE status IN ('testing','probing','pushing')"
                  + (" AND id!=%d" % keep_batch if keep_batch is not None else ""), (self.clock(),))
        live = {j.branch for j in adopted.values() if j.kind == "selftest"}
        for r in store.list_branches(c, S.SELFTESTING.value):
            if r["name"] not in live:
                store.apply_event(c, r["name"], Event.RETRY)
        members = {n for n, _, _ in self.batch.members} if self.batch is not None else set()
        for r in store.list_branches(c, S.BATCHING.value):
            if r["name"] not in members:
                store.apply_event(c, r["name"], Event.BATCH_DEFERRED)

    # ---------- 換版接管 ----------
    def _timeout_for(self, kind: str) -> float:
        return self.cfg.selftest_timeout_s if kind == "selftest" else self.cfg.batch_timeout_s

    def _adopt_runs(self) -> Dict[int, Running]:
        """資料庫裡 status=running 的工作，行程群組還活著（或 exit.json 已寫好）就接管：恢復核池記帳、
        逾時從原本的開跑時間算、之後跟自己開的一樣輪詢收結果。回傳 run id → Running。"""
        from . import cores as _cores
        from .executor import exit_path_for
        c = self.con
        out: Dict[int, Running] = {}
        restored: Dict[int, Optional[ActiveBatch]] = {}
        exists = getattr(self.repo, "worktree_exists", lambda p: os.path.isdir(p))
        for r in c.execute("SELECT * FROM runs WHERE status='running' AND pgid IS NOT NULL ORDER BY id").fetchall():
            try:
                cores = [int(x) for x in (r["cores"] or "").split(",") if x]
                wd = r["workdir"]
                if not cores or not wd or not exists(wd):
                    continue
                job = Job(layer=r["layer"], workdir=wd, cores=cores,
                          timeout_s=int(r["timeout_s"] or self._timeout_for(r["kind"])),
                          report_path=r["report_path"], log_path=r["log_path"])
                alive = os.path.exists(exit_path_for(job)) or bool(_cores.group_members(r["pgid"]))
                if not alive:
                    continue
                kind, branch, head = r["kind"], r["branch"], r["commit_sha"]
                if kind == "selftest":
                    row = store.get_branch(c, branch) if branch else None
                    if row is None or row["state"] != S.SELFTESTING.value:
                        continue
                    head = row["head_sha"]
                elif kind in ("point", "probe"):
                    if r["batch_id"] not in restored:
                        restored[r["batch_id"]] = self._restore_batch(r["batch_id"])
                    if restored[r["batch_id"]] is None:
                        continue
                elif kind != "baseline":
                    continue
                if not self.pool.reserve(cores):
                    continue
                started = r["started_at"] or self.clock()
                elapsed = max(self.clock() - started, 0.0)
                h = self.executor.adopt(job, r["pgid"], job.timeout_s - elapsed, elapsed)
                out[r["id"]] = Running(h, self.repo.adopt_worktree(wd) if hasattr(self.repo, "adopt_worktree") else _NoCtx(),
                                       cores, r["id"], kind, branch, head, r["size"], r["batch_id"],
                                       r["units"] or len(cores), r["approved_sha"], r["retest"], wd)
            except Exception as e:                 # 接不起來的當沒接管，走舊辦法（殺掉重排）
                log.warning("接管 run %s 失敗：%r", r["id"], e)
        self.jobs.update(out)
        bids = {j.batch_id for j in out.values() if j.batch_id is not None}
        self.batch = None
        for bid, ab in restored.items():
            if ab is not None and bid in bids:
                for j in out.values():
                    if j.batch_id == bid:
                        ab.launched.add((j.kind, j.size))
                self.batch = ab
        for rid in [rid for rid, j in out.items() if j.batch_id is not None and (self.batch is None or j.batch_id != self.batch.id)]:
            j = self.jobs.pop(rid)
            out.pop(rid)
            self.pool.release(j.cores)                      # 只接一個批；多的當沒接管（下面殺掉）
        if out:
            log.info("換版接管：%d 個測試繼續跑（%s）", len(out), "、".join(f"#{rid} {j.kind}" for rid, j in out.items()))
        return out

    def _restore_batch(self, bid: int) -> Optional["ActiveBatch"]:
        """用資料庫重建進行中的批；成員內容跟組批時不同（分支又被推了）就不接。已經跑完的測點不記，下一輪重跑。"""
        c = self.con
        b = c.execute("SELECT * FROM batches WHERE id=? AND status IN ('testing','probing')", (bid,)).fetchone()
        if b is None:
            return None
        rows = c.execute("SELECT * FROM batch_members WHERE batch_id=? ORDER BY pos", (bid,)).fetchall()
        if not rows:
            return None
        members, prefixes, ctx = [], [], {}
        for m in rows:
            br = store.get_branch(c, m["branch"])
            if br is None or br["state"] != S.BATCHING.value or br["content_id"] != m["content_id"] or not m["squash_sha"]:
                return None
            members.append((m["branch"], m["content_id"], br["head_sha"]))
            prefixes.append(m["squash_sha"])
            ctx[m["branch"]] = (br["approved_sha"], br["retest"])
        return ActiveBatch(bid, b["base_sha"], members, prefixes, ctx=ctx)

    def handover(self):
        """換版（jet restart）：把測試留給下一代主行程，自己什麼都不殺、不等。只放掉自己的暫時資源。"""
        self._cls_close()
        log.info("換版交接：%d 個測試留給下一代接管", len(self.jobs))

    def _kill_leftovers(self, keep_pgids=()):
        import signal
        root = os.path.abspath(self.cfg.work_dir)
        keep_pgids = set(keep_pgids)
        for r in self.con.execute("SELECT pgid FROM runs WHERE status='running' AND pgid IS NOT NULL"):
            pg = r[0]
            if pg in keep_pgids:
                continue                                  # 被接管的工作不殺
            try:
                # 保險：只殺工作目錄在我們 worktree 底下的群組（避免重開機後行程號被別人用走）
                if pg > 1 and os.readlink(f"/proc/{pg}/cwd").startswith(root):
                    os.killpg(pg, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass
        # 領頭已死、子行程（例如遊戲引擎）還活著的群組，上面靠 /proc/<領頭> 認不到：直接用「工作目錄在我們底下」認
        left = own_leftovers(root, keep_pgids)
        if left:
            log.warning("殺掉上一代遺留的測試行程 %d 個", kill_leftovers(left))

    def _recover_pushing(self, b):
        c = self.con
        final, m = b["merged_sha"], b["merged_count"] or 0
        main = self.repo.main_sha()
        if not final or m <= 0 or not self.repo.is_ancestor(final, main):
            return
        names = [r["branch"] for r in c.execute(
            "SELECT branch FROM batch_members WHERE batch_id=? ORDER BY pos", (b["id"],))][:m]
        done = []
        for n in names:
            row = store.get_branch(c, n)
            if row is not None and row["state"] == S.BATCHING.value:
                store.apply_event(c, n, Event.BATCH_MERGED, merged_sha=final)
                done.append(n)
        c.execute("UPDATE batches SET status='merged', finished_at=? WHERE id=?", (self.clock(), b["id"]))
        self.main = main
        if done:
            self._after_merge(done, final, None, b["base_sha"])

    def update_config(self, cfg):
        """設定檔熱重載（已檢查過格式）。核池變了就重建，保留還在用的核。"""
        if (cfg.pool, cfg.core_unit, cfg.threads) != (self.cfg.pool, self.cfg.core_unit, self.cfg.threads):
            old = self.pool
            self.pool = make_pool(cfg)
            self.pool.busy = {c for c in old.busy if c in set(self.pool.all)}
        self.cfg = cfg
        t = getattr(self.executor, "test_cmd", None)
        if t is not None:
            self.executor.test_cmd = list(cfg.test_cmd)

    # ---------- 一輪 ----------
    # ---------- 可觀測性：狀態寫進 meta，/status 與 jet doctor 讀得到 ----------
    def _put(self, key: str, d: dict):
        store.set_meta(self.con, f"obs_{key}", json.dumps(d, ensure_ascii=False))

    def _note_error(self, where: str, msg: str):
        """錯誤一律寫日誌；同一個訊息只發一次待辦事件，不洗版。"""
        self._put("last_error", {"at": self.clock(), "where": where, "message": msg})
        if msg != self._last_err_msg:
            self._last_err_msg = msg
            log.error("%s：%s", where, msg)
            store.add_event(self.con, "queue_error", None, f"{where}：{msg}"[:500])

    def _check_pool(self):
        """核池小到永遠派不出工作（例如實體核模式但池裡沒有兄弟執行緒）：不能靜悄悄，寫進最近錯誤。"""
        if hasattr(self.pool, "units"):
            n = len(self.pool.units)
            ex = getattr(self.pool, "excluded", [])
        else:
            n, ex = len(self.pool.all), []
        need = max(self.cfg.selftest_cores, self.cfg.batch_cores)
        if n < need:
            self._note_error("核池", f"可用{'實體' if hasattr(self.pool, 'units') else ''}核只有 {n} 顆，單一工作要 {need} 顆，派不出工作"
                             + (f"（兄弟執行緒不在池裡而被排除的核：{ex[:16]}）" if ex else ""))

    def _fetch_sync(self):
        t0 = time.time()
        self._put("fetch", {"state": "running", "started_at": t0})
        try:
            self.repo.fetch()
        except Exception as e:
            self._put("fetch", {"state": "error", "started_at": t0, "finished_at": time.time(), "error": str(e)})
            self._note_error("fetch 失敗", str(e))
            raise
        dt = time.time() - t0
        self._put("fetch", {"state": "ok", "started_at": t0, "finished_at": time.time(), "seconds": round(dt, 2)})
        self._fetch_done_once = True
        if dt > 30:
            log.info("fetch 完成（%.0f 秒）", dt)

    def _fetch_worker(self, t0):
        """背景執行緒只做 git fetch，不碰資料庫（sqlite 連線不跨執行緒）；結果交給主執行緒記錄。"""
        try:
            self.repo.fetch()
            self._fetch_res = (True, None, t0, time.time())
        except Exception as e:
            self._fetch_res = (False, str(e), t0, time.time())

    def _harvest_fetch(self):
        if self._fetch_thread is None or self._fetch_thread.is_alive():
            return
        self._fetch_thread = None
        ok, err, t0, t1 = self._fetch_res
        if ok:
            self._put("fetch", {"state": "ok", "started_at": t0, "finished_at": t1, "seconds": round(t1 - t0, 2)})
            self._fetch_done_once = True
            self._last_err_msg = None
            if t1 - t0 > 30:
                log.info("fetch 完成（%.0f 秒）", t1 - t0)
        else:
            self._put("fetch", {"state": "error", "started_at": t0, "finished_at": t1, "error": err})
            self._note_error("fetch 失敗", err)

    def _maybe_fetch(self) -> bool:
        """回傳這一輪能不能繼續（鏡像裡有東西可掃）。"""
        if not self.async_fetch:
            self._fetch_sync()
            return True
        self._harvest_fetch()
        if self._fetch_thread is None and time.time() - self._fetch_started >= self.cfg.fetch_interval_s:
            t0 = self._fetch_started = time.time()
            self._put("fetch", {"state": "running", "started_at": t0})
            if not self._fetch_done_once:
                log.info("開始第一次 fetch（大的 repo 要幾分鐘；進度看 jet status --verbose）")
            self._fetch_thread = threading.Thread(target=self._fetch_worker, args=(t0,), daemon=True)
            self._fetch_thread.start()
        if self._fetch_done_once:
            return True
        try:                                  # 重啟後鏡像本來就有東西，不用乾等
            self.repo.main_sha()
            return True
        except Exception:
            self._put("scan", {"at": self.clock(), "waiting": "第一次 fetch 還沒完成，鏡像是空的"})
            return False

    def tick(self) -> None:
        self._bad = None
        phys = hasattr(self.pool, "units")
        self._put("tick", {"at": self.clock(), "jobs": len(self.jobs), "pool_unit": "physical" if phys else "logical",
                           "pool_busy": (len(self.pool.units) - self.pool.free_count()) if phys else len(self.pool.busy),
                           "pool_total": len(self.pool.units) if phys else len(self.pool.all)})
        self._check_pool()
        if not self._maybe_fetch():
            return
        self.main = self.repo.main_sha()
        self._repulled = False
        self._source_step()
        self._scan()
        if self.source is not None:
            self._reconcile_parents()
        self._sweep_parents()
        if self.source is not None:
            self._apply_source_approvals()
        self._collect()
        self._start_rechecks()
        self._reap_orphans()
        self._enforce_review_first()
        self._put("guard", self._guard_info())
        self._promote()
        self._snapshot_step()
        if store.is_paused(self.con) or not self.resources_ok():
            return
        if not self.draining:
            self._nightly_step()
        if self.cfg.workflow_mode != "direct_merge":
            self._wf_status("idle", main_sha=self.main)        # 批測閘門模式：沒有背景 loop，只回報目前模式
        if self.cfg.workflow_mode == "direct_merge":
            if not self.draining:
                self._loop_step()
                self._direct_merge_step()
                self._dispatch_selftests()
            return
        if self.draining:
            if self.batch is not None:
                self._batch_step()
            return
        if not self._ensure_baseline():
            return
        self._batch_step()
        self._dispatch_selftests()

    def run_forever(self, interval: float = 2.0) -> None:
        while not self._stop:
            try:
                self.tick()
            except Exception as e:  # 排隊器不能因一輪出錯就掛
                log.exception("tick 出錯")
                store.add_event(self.con, "queue_error", None, repr(e))
            time.sleep(interval)

    def stop(self):
        self._stop = True

    def drain_waiting(self) -> List[dict]:
        """停機等待中還在跑的工作（給 status 看：還在等哪個 run）。"""
        now = self.clock()
        out = [{"run": j.run_id, "kind": j.kind, "branch": j.branch, "size": j.size} for j in self.jobs.values()]
        if self.batch is not None and not out:
            out.append({"run": None, "kind": "batch", "branch": None, "size": None})
        return out

    def idle(self) -> bool:
        return not self.jobs and self.batch is None

    def shutdown(self):
        """立刻停機前：殺掉所有測試行程群組、釋放核。批與狀態留給下次啟動的 recover 接續。"""
        self._cls_close()
        for rid, j in list(self.jobs.items()):
            try:
                self.executor.cancel(j.handle)
            finally:
                self._release_job(rid, "cancelled")

    # ---------- 掃 Git ----------
    def _scan(self):
        remote = self.repo.list_branches()
        self._tips = dict(remote)
        outside = 0
        if self.cfg.scope == "tracked":                      # 登記制：沒登記的完全不理
            tr = store.tracked_names(self.con)
            outside = sum(1 for n in remote if n not in tr)
            remote = {n: s for n, s in remote.items() if n in tr}
        before = {r["name"]: r["head_sha"] for r in store.list_branches(self.con)}
        stats = {"at": self.clock(), "scope": self.cfg.scope, "outside_scope": outside,
                 "remote": len(remote), "new": 0, "changed": 0, "dropped": 0,
                 "main": self.main[:12]}
        for name, sha in self._parents_first(remote):
            if name not in before:
                stats["new"] += 1
            elif before[name] != sha:
                stats["changed"] += 1
            row = store.get_branch(self.con, name)
            if row is not None and row["state"] == S.MERGED.value and row["head_sha"] != sha:
                self.con.execute("DELETE FROM branches WHERE name=?", (name,))
                row = None
            if row is None:
                self._insert(name, sha, remote)
            elif row["head_sha"] != sha:
                self._updated(row, sha)
        for row in store.list_branches(self.con):
            if row["name"] not in remote and row["state"] != S.MERGED.value:
                stats["dropped"] += 1
                self._drop(row)
        if self.cfg.scope == "tracked" and self.source is None:
            self._apply_tracked_parents()
        stats["tracked"] = self.con.execute("SELECT count(*) FROM branches").fetchone()[0]
        prev = store.get_meta(self.con, "obs_scan")
        self._put("scan", stats)
        if stats["new"] or stats["changed"] or stats["dropped"] or prev is None:
            log.info("掃描：遠端 %d 條（新 %d、更新 %d、消失 %d），追蹤中 %d 條，main %s",
                     stats["remote"], stats["new"], stats["changed"], stats["dropped"],
                     stats["tracked"], stats["main"])
        if stats["remote"] == 0 and stats["tracked"] == 0 and self.cfg.scope == "all":
            self._note_error("掃描", "遠端沒有符合 git.branch_patterns 的分支（樣式設定對嗎？用 jet doctor 檢查）")

    def _apply_tracked_parents(self):
        """登記時帶的父分支：等父分支也被掃到之後套用一次（之後的父變動走 /set-parent）。"""
        for t in self.con.execute("SELECT branch, parent FROM tracked WHERE parent_applied=0").fetchall():
            row = store.get_branch(self.con, t["branch"])
            if row is None:
                continue
            if t["parent"] is None:
                self.con.execute("UPDATE tracked SET parent_applied=1 WHERE branch=?", (t["branch"],))
                continue
            p = store.get_branch(self.con, t["parent"])
            if p is None:
                continue                                        # 父還沒被掃到，下一輪再來
            if p["state"] != S.MERGED.value:
                try:
                    store.set_parent(self.con, t["branch"], t["parent"], "tracked")
                except ValueError:                              # 跟人工設的父分支成環：以人工的為準
                    pass
            self.con.execute("UPDATE tracked SET parent_applied=1 WHERE branch=?", (t["branch"],))

    def _base_for(self, row) -> str:
        if row is not None and row["parent"]:
            p = store.get_branch(self.con, row["parent"])
            if p is not None:
                return p["head_sha"]
        return self.main

    def _parents_first(self, remote: Dict[str, str]):
        """父分支排在子分支前面處理：子分支的內容指紋是相對父分支算的，父分支要先更新。"""
        par = {t["branch"]: t["parent"] for t in self.con.execute("SELECT branch,parent FROM tracked")}
        def depth(n):
            d, seen = 0, set()
            while par.get(n) and par[n] not in seen and par[n] in remote:
                seen.add(n)
                n = par[n]
                d += 1
            return d
        return sorted(remote.items(), key=lambda kv: (depth(kv[0]), kv[0]))

    def _insert(self, name, sha, remote=None):
        base = self.main
        t = self.con.execute("SELECT parent FROM tracked WHERE branch=?", (name,)).fetchone()
        if t is not None and t["parent"] and remote and t["parent"] in remote:
            pr = store.get_branch(self.con, t["parent"])
            if pr is None or pr["state"] != S.MERGED.value:
                base = remote[t["parent"]]               # 登記時帶了父分支：內容指紋相對父分支算
        cid = self.repo.content_id(sha, base)
        store.note_sha(self.con, name, sha, cid)
        docs = self._classify_branch(sha)
        self.con.execute(
            "INSERT INTO branches(name,head_sha,content_id,state,seen_at,docs_only,updated_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (name, sha, cid, S.PENDING.value, self.clock(), docs, self.clock()))

    def _updated(self, row, sha):
        name = row["name"]
        cid = self.repo.content_id(sha, self._base_for(row))
        store.note_sha(self.con, name, sha, cid)
        same = cid == row["content_id"]
        st = row["state"]
        if same and st != S.REJECTED.value:
            self.con.execute("UPDATE branches SET head_sha=? WHERE name=?", (sha, name))
            return
        # 內容變了（或退回後重疊）：停掉在跑的工作，回待自測
        self._cancel_jobs(branch=name)
        if st == S.BATCHING.value:
            self._truncate_batch(name, "成員有新提交")
        fields = {"head_sha": sha}
        if not same:
            fields.update(content_id=cid, fail_count=0, queued_at=None, reject_reason=None,
                          docs_only=self._classify_branch(sha))
        else:
            fields.update(reject_reason=None)
        store.apply_event(self.con, name, Event.PUSHED, **fields)
        if not same:
            for ch in store.children(self.con, name):
                self._parent_changed(ch)

    def _drop(self, row):
        name = row["name"]
        self._cancel_jobs(branch=name)
        if row["state"] == S.BATCHING.value:
            self._truncate_batch(name, "成員消失")
        for ch in store.children(self.con, name):
            self.con.execute("UPDATE branches SET parent=NULL WHERE name=?", (ch["name"],))   # 外鍵：先脫鉤；來源模式的父卡以清單為準（_src_parent），父卡回來會重新套上
            self._parent_changed(ch)
        self.con.execute("DELETE FROM branches WHERE name=?", (name,))

    def _parent_changed(self, ch):
        ch = store.get_branch(self.con, ch["name"])
        if ch["state"] in (S.SELFTESTING.value, S.SELFTEST_GREEN.value, S.QUEUED.value):
            self._cancel_jobs(branch=ch["name"])
            store.apply_event(self.con, ch["name"], Event.PARENT_CHANGED)
            for g in store.children(self.con, ch["name"]):
                self._parent_changed(g)

    def _sweep_parents(self):
        for r in store.list_branches(self.con):
            if not r["parent"]:
                continue
            p = store.get_branch(self.con, r["parent"])
            if p is None:
                self.con.execute("UPDATE branches SET parent=NULL WHERE name=?", (r["name"],))   # 來源模式下清單仍說有父卡，_src_parent 會擋住
            elif p["state"] in (S.REJECTED.value, S.HALTED.value):
                self._parent_changed(r)
            elif r["state"] == S.QUEUED.value and p["state"] in (S.PENDING.value, S.SELFTESTING.value,
                                                                 S.SELFTEST_GREEN.value):
                self._parent_changed(r)                  # 父卡不在隊伍裡了（放行被撤回、重測中）：子卡回待自測

    # ---------- 分類 ----------
    def _classify_dir(self) -> Optional[str]:
        """分類指令的工作目錄：被測專案最新 main 的檢出（相對路徑以專案根為準）。main 換了就換一份檢出。"""
        if self._cls_sha != self.main:
            self._cls_close()
            try:
                ctx = self.repo.worktree(self.main)
                self._cls_dir = ctx.__enter__()
                self._cls_ctx, self._cls_sha = ctx, self.main
            except Exception as e:
                self._cls_dir, self._cls_sha = None, self.main
                self._note_error("分類指令", f"取不到 main 的檢出：{e}")
        return self._cls_dir if self._cls_dir and os.path.isdir(self._cls_dir) else None

    def _cls_close(self):
        if self._cls_ctx is not None:
            try:
                self._cls_ctx.__exit__(None, None, None)
            except Exception:
                pass
        self._cls_ctx, self._cls_dir, self._cls_sha = None, None, None

    def _is_docs(self, files: List[str]) -> bool:
        cmd = self.cfg.classify_cmd
        if not cmd or not files:
            return False
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
            f.write("\n".join(files) + "\n")
            path = f.name
        try:
            argv = [a.replace("{files}", path) for a in cmd]
            out = subprocess.run(argv, capture_output=True, text=True, timeout=60, cwd=self._classify_dir())
            if out.returncode != 0:                      # 指令本身壞了（找不到、路徑不對）：當要測，但要讓人看得到
                self._note_error("分類指令", f"回傳碼 {out.returncode}（一律當要測）：{(out.stderr or out.stdout).strip()[:200]}")
                return False
            return json.loads(out.stdout).get("verdict") == "docs_only"
        except Exception as e:
            self._note_error("分類指令", f"執行失敗（一律當要測）：{type(e).__name__}: {str(e)[:200]}")
            return False
        finally:
            os.unlink(path)

    def _classify_branch(self, sha) -> Optional[int]:
        if not self.cfg.classify_cmd:
            return None
        return 1 if self._is_docs(self.repo.changed_files(sha, self.main)) else 0

    # ---------- 核 ----------
    def _own_pgids(self):
        return {getattr(r.handle, "pgid", None) for r in self.jobs.values()} - {None}

    def _acquire(self, n: int) -> List[int]:
        if self._bad is None:
            self._bad = set(self.foreign_busy(self.pool.all, self._own_pgids()))
        added = self._bad - self.pool.busy
        self.pool.busy |= added
        try:
            return self.pool.acquire(n)
        finally:
            self.pool.busy -= added

    # ---------- 派工與收工 ----------
    def _launch(self, kind, layer, sha, ncores, timeout, branch=None, size=None, batch_id=None,
                base_sha=None, head_sha=None, only=None) -> bool:
        cores = self._acquire(ncores)
        if not cores:
            return False
        ctx = self.repo.worktree(sha)
        wd = ctx.__enter__()
        cur = self.con.execute(
            "INSERT INTO runs(kind,branch,batch_id,size,layer,commit_sha,base_sha,status,cores,started_at,"
            "workdir,timeout_s,units) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (kind, branch, batch_id, size, layer, sha, base_sha, "running",
             ",".join(map(str, cores)), self.clock(), wd, timeout, ncores))
        rid = cur.lastrowid
        d = os.path.join(self.cfg.state_dir, "runs", str(rid))
        os.makedirs(d, exist_ok=True)
        log_p, rep_p = os.path.join(d, "output.log"), os.path.join(d, "report.json")
        self.con.execute("UPDATE runs SET log_path=?, report_path=? WHERE id=?", (log_p, rep_p, rid))
        env_extra = None
        try:                              # 提交訊息有 `Allow-Strings-Rewrite: <理由>` 尾行 → 這次測試帶 ALLOW_STRINGS_REWRITE=1（strings.csv 有意改既有行）
            import re as _re
            heads = [head_sha] if head_sha else []
            if kind in ("point", "probe", "pretry") and self.batch is not None and size:
                heads = [m[2] for m in self.batch.members[:size]]       # 批測：看每個成員自己的提交（疊成一個提交後訊息會不見）
            base = self.repo.main_sha()
            for h in heads or [sha]:
                if h and base and h != base and _re.search(r"^Allow-Strings-Rewrite:[ \t]*\S",
                                                          self.repo.commit_messages(base, h), _re.M | _re.I):
                    env_extra = {"ALLOW_STRINGS_REWRITE": "1"}
                    break
        except Exception:
            env_extra = None
        try:
            handle = self.executor.start(Job(layer=layer, workdir=wd, cores=cores, timeout_s=timeout,
                                             report_path=rep_p, log_path=log_p, only=only, env_extra=env_extra))
        except Exception as e:
            ctx.__exit__(None, None, None)
            self.pool.release(cores)
            self.con.execute("UPDATE runs SET status='infra', finished_at=? WHERE id=?", (self.clock(), rid))
            self._start_fail += 1
            if self._start_fail == 3:
                store.add_event(self.con, "queue_error", branch, f"工作開不起來：{e!r}")
            return False
        self._start_fail = 0
        self.con.execute("UPDATE runs SET pgid=? WHERE id=?", (getattr(handle, "pgid", None), rid))
        row = store.get_branch(self.con, branch) if branch else None
        if row is not None:      # 換版接管時要知道這次測的是哪個放行版本
            self.con.execute("UPDATE runs SET approved_sha=?, retest=? WHERE id=?", (row["approved_sha"], row["retest"], rid))
        self.jobs[rid] = Running(handle, ctx, cores, rid, kind, branch, head_sha or sha, size, batch_id, ncores,
                                 row["approved_sha"] if row else None, row["retest"] if row else None, wd)
        return True

    def _release_job(self, rid, status):
        j = self.jobs.pop(rid)
        try:
            self._keep_artifacts(j)
        except Exception as e:                               # 留檔失敗不能影響測試結論
            log.warning("留存測試產物失敗：%r", e)
        if j.kind in ("baseline", "loopprobe"):
            try:
                self._record_item_times(j)
                self._record_item_metrics(j)
            except Exception as e:
                log.warning("記錄逐項耗時失敗：%r", e)
        try:
            j.ctx.__exit__(None, None, None)
        finally:
            self.pool.release(j.cores)
        self.con.execute("UPDATE runs SET status=?, finished_at=? WHERE id=?", (status, self.clock(), rid))
        return j

    METRIC_COLS = ("secs", "draw_calls_avg", "draw_calls_max", "fps_avg", "fps_min", "objects", "mem_mb")

    def _record_item_metrics(self, j):
        """解析逐項畫面量測檔（JSON：{"schema":1,"items":{項目:{secs,draw_calls_avg,draw_calls_max,fps_avg,fps_min,objects,mem_mb}}}）。"""
        pat = self.cfg.metrics_file
        if not pat or not j.workdir:
            return
        import glob
        root = os.path.realpath(j.workdir)
        for src in sorted(glob.glob(os.path.join(j.workdir, pat), recursive=True)):
            real = os.path.realpath(src)
            if not os.path.isfile(real) or not real.startswith(root + os.sep) or os.path.getsize(real) > (20 << 20):
                continue
            try:
                with open(real, encoding="utf-8") as f:
                    items = (json.load(f) or {}).get("items") or {}
            except (OSError, ValueError):
                continue
            if not isinstance(items, dict):
                continue
            for name, m in items.items():
                if not isinstance(m, dict):
                    continue
                vals = []
                for k in self.METRIC_COLS:
                    v = m.get(k)
                    vals.append(float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None)
                self.con.execute("INSERT OR REPLACE INTO item_metrics(run_id,item,secs,draw_calls_avg,draw_calls_max,fps_avg,"
                                 "fps_min,objects,mem_mb) VALUES(?,?,?,?,?,?,?,?,?)", (j.run_id, str(name), *vals))

    def _record_item_times(self, j):
        """解析逐項耗時檔，存進 item_times（背景 loop 的性能量測）。"""
        pat = self.cfg.times_file
        if not pat or not j.workdir:
            return
        import glob
        root = os.path.realpath(j.workdir)
        rows = {}
        for src in sorted(glob.glob(os.path.join(j.workdir, pat), recursive=True)):
            real = os.path.realpath(src)
            if not os.path.isfile(real) or not real.startswith(root + os.sep):
                continue
            with open(real, encoding="utf-8", errors="replace") as f:
                for line in f:
                    p = line.rstrip("\n").split("\t")
                    if len(p) < 3:
                        continue
                    try:
                        a, b = float(p[1]), float(p[2])
                    except ValueError:
                        continue
                    if b >= a:
                        rows[p[0]] = (round(b - a, 2), a, b)
        for item, (secs, a, b) in rows.items():
            self.con.execute("INSERT OR REPLACE INTO item_times(run_id,item,secs,start_ts,end_ts) VALUES(?,?,?,?,?)",
                             (j.run_id, item, secs, a, b))

    def _record_loop_run(self, j, res):
        """背景 loop 一次測試的整體量測，並把摘要送看板（loop_perf）。"""
        c = self.con
        r = c.execute("SELECT started_at, finished_at FROM runs WHERE id=?", (j.run_id,)).fetchone()
        wall = (r["finished_at"] - r["started_at"]) if r and r["finished_at"] and r["started_at"] else res.duration_s
        n = c.execute("SELECT COUNT(*) FROM item_times WHERE run_id=?", (j.run_id,)).fetchone()[0]
        c.execute("INSERT OR REPLACE INTO loop_runs(run_id,kind,main_sha,started_at,finished_at,wall_s,cores_n,cpu_s,items_n,status)"
                  " VALUES(?,?,?,?,?,?,?,?,?,?)",
                  (j.run_id, j.kind, j.head_sha, r["started_at"] if r else None, r["finished_at"] if r else None,
                   wall, len(j.cores) or j.units, res.cpu_s, n, res.status))
        top = [{"item": x[0], "secs": x[1]} for x in c.execute(
            "SELECT item, secs FROM item_times WHERE run_id=? ORDER BY secs DESC LIMIT 15", (j.run_id,))]
        mx = lambda col, order, k=10: [{"item": x[0], "value": x[1]} for x in c.execute(
            f"SELECT item, {col} FROM item_metrics WHERE run_id=? AND {col} IS NOT NULL ORDER BY {col} {order} LIMIT {k}", (j.run_id,))]
        agg = c.execute("SELECT COUNT(*), AVG(fps_avg), MIN(fps_min), AVG(draw_calls_avg), MAX(draw_calls_max) FROM item_metrics WHERE run_id=?",
                        (j.run_id,)).fetchone()
        metrics = None
        if agg and agg[0]:
            metrics = {"items": agg[0], "fps_avg": round(agg[1], 1) if agg[1] is not None else None, "fps_min": agg[2],
                       "draw_calls_avg": round(agg[3], 1) if agg[3] is not None else None, "draw_calls_max": agg[4],
                       "lowest_fps": mx("fps_min", "ASC"), "most_draw_calls": mx("draw_calls_max", "DESC")}
        util = None
        if res.cpu_s and wall and (len(j.cores) or j.units):
            util = round(res.cpu_s / (wall * (len(j.cores) or j.units)), 2)
        store.emit(c, "loop_perf", None, {"main_sha": j.head_sha, "perf": {
            "kind": j.kind, "wall_s": round(wall or 0, 1), "cores": len(j.cores) or j.units, "cpu_util": util,
            "items": n, "top": top, "metrics": metrics}, "detail": f"背景測試耗時 {round(wall or 0)} 秒，{n} 項"}, force=True)

    ARTIFACT_MAX = 5 << 20           # 單檔上限；大的只留尾巴

    def _keep_artifacts(self, j):
        """工作目錄清掉之前，把設定的檔案（例如逐項耗時紀錄）複製到這次 run 的目錄，逾時也留得下來。"""
        pats = self.cfg.artifacts
        if not pats or not j.workdir:
            return
        import glob
        dest = os.path.join(self.cfg.state_dir, "runs", str(j.run_id), "artifacts")
        root = os.path.realpath(j.workdir)
        n = 0
        for pat in pats:
            for src in sorted(glob.glob(os.path.join(j.workdir, pat), recursive=True)):
                real = os.path.realpath(src)
                if not os.path.isfile(real) or not real.startswith(root + os.sep) or n >= 50:
                    continue
                rel = os.path.relpath(real, root)
                out = os.path.join(dest, rel)
                os.makedirs(os.path.dirname(out), exist_ok=True)
                size = os.path.getsize(real)
                with open(real, "rb") as f, open(out, "wb") as g:
                    if size > self.ARTIFACT_MAX:
                        f.seek(size - self.ARTIFACT_MAX)
                    g.write(f.read())
                n += 1

    def _cancel_jobs(self, branch=None, batch=False):
        if branch:
            self._rc.pop(branch, None)
            self._rc_wait.pop(branch, None)
        for rid, j in list(self.jobs.items()):
            if (branch and j.branch == branch and j.kind in ("selftest", "recheck")) or \
               (batch and j.kind in ("point", "probe", "pretry")):
                try:
                    self.executor.cancel(j.handle)
                finally:
                    self._release_job(rid, "cancelled")

    def _collect(self):
        for rid, j in list(self.jobs.items()):
            res = self.executor.poll(j.handle)
            if res is None:
                continue
            self.con.execute("UPDATE runs SET exit_code=? WHERE id=?", (res.exit_code, rid))
            self._release_job(rid, res.status)
            if res.status not in ("infra", "cancelled"):
                self._infra_streak = 0
            if j.kind in ("baseline", "loopprobe") and res.status not in ("cancelled",):
                try:
                    self._record_loop_run(j, res)
                except Exception as e:
                    log.warning("記錄背景測試量測失敗：%r", e)
            if j.kind == "selftest":
                self._on_selftest(j, res)
            elif j.kind == "recheck":
                self._on_recheck(j, res)
            elif j.kind == "pretry":
                self._on_pretry(j, res)
            elif j.kind == "baseline":
                self._on_baseline(j, res)
            elif j.kind == "loopprobe":
                self._on_loopprobe(j, res)
            elif j.kind == "nightly":
                self._on_nightly(j, res)
            else:
                self._on_point(j, res)

    def _store_items(self, rid, res, base: Set[str]):
        """紅項寫進 run_items（通知與看板要看）；本來就紅的（已知紅）標成 known，不算這條分支的。"""
        c = self.con
        rr0 = c.execute("SELECT log_path FROM runs WHERE id=?", (rid,)).fetchone()
        if res.status == "timeout":
            c.execute("INSERT OR REPLACE INTO run_items(run_id,item_id,status,summary) VALUES(?,?,?,?)",
                      (rid, "逾時", "timeout", ("測試逾時\n" + (executor.failure_excerpt(rr0["log_path"], "") if rr0 and rr0["log_path"] else "")).strip()))
        rr = c.execute("SELECT log_path, report_path FROM runs WHERE id=?", (rid,)).fetchone()
        sums = executor.read_report_summaries(rr["report_path"]) if rr and rr["report_path"] else {}
        for it in res.failed_items:
            st = "known" if it in base else "fail"
            summ = sums.get(it)
            if not summ and st == "fail" and rr and rr["log_path"]:
                summ = executor.failure_excerpt(rr["log_path"], it) or None   # 報告沒寫原因：從記錄抽失敗那幾行
            c.execute("INSERT OR REPLACE INTO run_items(run_id,item_id,status,summary) VALUES(?,?,?,?)",
                      (rid, it, st, summ[:1200] if summ else None))

    @staticmethod
    def _red(res, baseline: Set[str]) -> bool:
        if res.status == "pass":
            return False
        if res.status == "timeout":
            return True
        items = set(res.failed_items)
        return not items or not items <= baseline

    def _infra_retry(self, key) -> bool:
        """基礎設施錯誤（測試根本沒跑起來）：重跑；連續 3 次就暫停整個排隊器並發待辦事件。"""
        self._infra_streak += 1
        if self._infra_streak >= 3 and not store.is_paused(self.con):
            why = f"{key} 連續基礎設施錯誤 {self._infra_streak} 次（測試根本沒跑起來），排隊器自己暫停；查完用 jet resume 恢復"
            store.set_paused(self.con, True, "scheduler", why)
            store.add_event(self.con, "queue_error", None, why)
        return True

    # ---------- 基準 ----------
    def _ensure_baseline(self) -> bool:
        main = self.main
        if store.get_baseline(self.con, main, "gate") is not None:
            store.set_meta(self.con, "last_main", main)
            return True
        if any(j.kind == "baseline" for j in self.jobs.values()):
            return False
        last = store.get_meta(self.con, "last_main")
        if last and last != main and store.get_baseline(self.con, last, "gate") is not None:
            if self._is_docs(self.repo.changed_files(main, last)):
                self._copy_baseline(last, main)
                return True
        self._launch("baseline", "gate", main, self.cfg.batch_cores, self.cfg.batch_timeout_s,
                     base_sha=main)
        return False

    def _copy_baseline(self, src, dst):
        for layer in ("gate", "selftest"):
            b = store.get_baseline(self.con, src, layer)
            if b is not None:
                store.set_baseline(self.con, dst, layer, b, copied=True)
        store.set_meta(self.con, "last_main", dst)

    def _set_baseline_all(self, sha, failed):
        self._check_docs_guard(sha, set(failed))
        for layer in ("gate", "selftest"):
            store.set_baseline(self.con, sha, layer, failed)
        store.set_meta(self.con, "last_main", sha)
        store.set_meta(self.con, "baseline_history", self.cfg.baseline_history)
        self._check_flaky()

    def _known_red(self, layer, main_sha) -> Set[str]:
        """判新紅用的「已知紅」：這個 main 的基準加最近 N 次 main 紅過的項目。"""
        return (store.get_baseline(self.con, main_sha, layer) or set()) | \
            store.known_red(self.con, layer, self.cfg.baseline_history)

    def _check_flaky(self):
        """基準裡時紅時綠的項目：發一筆待辦事件（同一組只發一次），讓人去修測試。"""
        fl = sorted(store.flaky_items(self.con, "gate", self.cfg.baseline_history))
        key = ",".join(fl)
        if fl and store.get_meta(self.con, "flaky_notified") != key:
            store.set_meta(self.con, "flaky_notified", key)
            store.add_event(self.con, "flaky", None, "main 上時紅時綠：" + key[:400])
        elif not fl:
            store.set_meta(self.con, "flaky_notified", "")

    def _on_baseline(self, j, res):
        if res.status in ("infra", "timeout", "cancelled"):
            self._infra_retry("baseline")
            return
        if self.cfg.workflow_mode == "direct_merge":
            try:
                rp = self.con.execute("SELECT report_path FROM runs WHERE id=?", (j.run_id,)).fetchone()
                summ = executor.read_report_summaries(rp[0]) if rp and rp[0] else {}
            except Exception:
                summ = {}
            self._loop_result(j.head_sha, res, summ)
        self._set_baseline_all(j.head_sha, set(res.failed_items))

    # ---------- direct_merge 工作流：自測綠就直接合，背景 loop 測最新 main、紅了二分找兇手 ----------
    def _direct_merge_step(self):
        c = self.con
        rows = [r for r in store.list_branches(c, S.QUEUED.value) if not self._direct_docs(r)]
        rows.sort(key=lambda r: (-r["prio_override"], self._board_prio(r["name"]), r["queued_at"] or 0, r["name"]))
        if not rows or not self._repull():
            return
        for r in rows:
            r = store.get_branch(c, r["name"])
            if r is None or r["state"] != S.QUEUED.value or r["parent"] is not None:
                continue                                   # 父卡還沒合：等它合完（_after_merge 會把子卡脫離）
            if not self._approved(r) or not self._tracked_ok(r["name"]) or r["name"] in self._blocked \
                    or self._blocked_by_main_red(r):
                continue
            main = self.repo.main_sha()
            res = self.repo.stack(main, [(r["name"], r["head_sha"])])
            if len(res.kept) != 1:
                store.apply_event(c, r["name"], Event.CONFLICT_MAIN, reject_reason="疊不上 main（撞檔）")
                store.add_event(c, "conflict", r["name"], "直接合入前疊不上 main")
                continue
            new = res.prefixes[0]
            store.apply_event(c, r["name"], Event.BATCH_START)
            try:
                self.repo.fast_forward_main(new, main)
            except MainMoved:
                store.apply_event(c, r["name"], Event.BATCH_DEFERRED)
                return
            store.apply_event(c, r["name"], Event.BATCH_MERGED, merged_sha=new)
            self._after_merge([r["name"]], new, None, main)
            self.main = new

    SNAPSHOT_MAX = 500
    NIGHTLY_MAX_ITEMS = 100
    NIGHTLY_INFRA_TRIES = 3
    NIGHTLY_RETRY_S = 600

    def _nightly_step(self):
        """每日完整層（NIGHT-01）：到設定的時刻，對 main 最新版跑 full 層一次，結果送 full_result。
        一天只開一次（開跑時就記日期，換版接管時由 _adopt_runs 接手收結果）；基礎設施錯誤最多重試 NIGHTLY_INFRA_TRIES 次。
        紅了只記錄：不擋 main、不二分、不動任何分支。"""
        at = self.cfg.nightly_at
        if not at or any(j.kind == "nightly" for j in self.jobs.values()):
            return
        now = self.clock()
        lt = time.localtime(now)
        today = time.strftime("%Y-%m-%d", lt)
        hh, mm = int(at[:2]), int(at[3:])
        if (lt.tm_hour, lt.tm_min) < (hh, mm) or store.get_meta(self.con, "nightly_day") == today:
            return
        if now < float(store.get_meta(self.con, "nightly_retry_at", "0") or 0):
            return
        cores = self.cfg.nightly_cores or self.cfg.batch_cores
        to = self.cfg.nightly_timeout_s or self.cfg.batch_timeout_s
        main = self.main
        if self._launch("nightly", "full", main, cores, to, base_sha=main, head_sha=main):
            store.set_meta(self.con, "nightly_day", today)

    def _on_nightly(self, j, res):
        c = self.con
        if res.status in ("infra", "cancelled"):
            n = int(store.get_meta(c, "nightly_infra", "0") or 0) + 1
            store.set_meta(c, "nightly_infra", str(n))
            if n < self.NIGHTLY_INFRA_TRIES and res.status == "infra":
                store.set_meta(c, "nightly_day", "")                       # 今天再試
                store.set_meta(c, "nightly_retry_at", str(self.clock() + self.NIGHTLY_RETRY_S))
                return
            if res.status == "cancelled":
                return
        store.set_meta(c, "nightly_infra", "0")
        r = c.execute("SELECT started_at, finished_at, report_path, log_path FROM runs WHERE id=?", (j.run_id,)).fetchone()
        total, red, timeouts = 0, [], []
        try:
            with open(r["report_path"], encoding="utf-8") as f:
                items = json.load(f)["items"]
            total = len(items)
            sums = executor.read_report_summaries(r["report_path"])
            for it in items:
                name = str(it.get("id"))
                ent = {"item": name, "summary": sums.get(name, "")[:800]}
                if it.get("status") == "fail" and len(red) < self.NIGHTLY_MAX_ITEMS:
                    red.append(ent)
                elif it.get("status") == "timeout" and len(timeouts) < self.NIGHTLY_MAX_ITEMS:
                    timeouts.append(ent)
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            pass
        if res.status == "timeout" and not timeouts:      # 整輪逾時、報告也沒寫哪一項：帶名字記一筆
            ex = executor.failure_excerpt(r["log_path"], "") if r and r["log_path"] else ""
            timeouts.append({"item": "完整層整體逾時", "summary": ex[:800]})
        if res.status == "infra":
            red.append({"item": "基礎設施錯誤", "summary": "完整層測試沒跑起來（重試仍失敗）"})
        if res.status == "fail" and not red and not timeouts:
            red.append({"item": "未知紅項", "summary": "測試失敗但報告沒列出紅項"})
        store.emit(c, "full_result", None, {"main_sha": j.head_sha, "started_at": r["started_at"], "finished_at": r["finished_at"] or self.clock(),
                                            "total": total, "red": red, "timeouts": timeouts}, force=True)

    def _snapshot_step(self):
        """定期送現況快照（BD-06 規格，CARD_FLOW §7 state_snapshot）：排隊器管理中的每條分支的狀態、提交、放行版本、重測次數。
        跟一般事件走同一張有序的事件表，所以一定排在先前的 merged 等事件之後。送不出去不影響調度。"""
        every = self.cfg.snapshot_s
        if not every or every <= 0:
            return
        now = self.clock()
        last = getattr(self, "_snap_at", None)
        if last is not None and now - last < every:
            return
        self._snap_at = now
        try:
            rows = [r for r in store.list_branches(self.con) if r["state"] != S.MERGED.value]
            branches = [{"branch": r["name"], "state": r["state"], "commit": r["head_sha"],
                         "approved_sha": r["approved_sha"], "retest": r["retest"] or 0}
                        for r in rows[:self.SNAPSHOT_MAX]]
            # 舊的快照一律刪掉只留最新一筆：看板連不上時不會一直堆，恢復後也只送一筆（已送出的不會再被讀）
            self.con.execute("DELETE FROM events WHERE kind='state_snapshot'")
            store.emit(self.con, "state_snapshot", None, {"at": now, "branches": branches}, force=True)
        except Exception as e:                      # 快照出錯不能影響調度
            log.warning("現況快照失敗：%r", e)

    def _wf_status(self, state, **kw):
        """workflow_status 事件（整份取代）：狀態變了才送。"""
        loop = {"state": state, "last_run_at": kw.get("last_run_at"), "last_result": kw.get("last_result"),
                "main_sha": kw.get("main_sha", self.main), "failed_items": kw.get("failed_items", []),
                "next_run_at": kw.get("next_run_at")}
        d = {"mode": self.cfg.workflow_mode, "loop": loop}
        key = json.dumps(d, sort_keys=True, ensure_ascii=False)
        if store.get_meta(self.con, "wf_status") != key:
            store.set_meta(self.con, "wf_status", key)
            store.emit(self.con, "workflow_status", None, d, force=True)

    def _loop_cfg(self):
        cores = self.cfg.loop_cores or self.cfg.batch_cores
        return cores, (self.cfg.loop_timeout_s or self.cfg.batch_timeout_s)

    def _loop_step(self):
        """背景 loop：一次只跑一個；先處理二分，沒有二分就測最新 main（測過的 main 不重測）。"""
        if not self.cfg.loop_enabled:
            return
        if any(j.kind in ("baseline", "loopprobe") for j in self.jobs.values()):
            self._wf_status("bisecting" if store.get_meta(self.con, "loop_bisect") else "running",
                            last_run_at=self.clock(), main_sha=self.main)
            return
        cores, to = self._loop_cfg()
        raw = store.get_meta(self.con, "loop_bisect")
        if raw:
            st = json.loads(raw)
            if st.get("phase") == "triage":
                idx = self._loop_suspect_idx(st)
                if idx is not None:           # 看板回了嫌疑：只驗證一次（嫌疑提交與它前一個各跑一次紅項）
                    st["queue"] = [st["cands"][idx]] + ([st["cands"][idx - 1]] if idx > 0 else [])
                    st["phase"] = "verify"
                    store.set_meta(self.con, "loop_bisect", json.dumps(st))
                elif self.clock() >= st.get("deadline", 0):
                    st["phase"] = "bisect"    # 逾時：改二分
                    store.set_meta(self.con, "loop_bisect", json.dumps(st))
                else:
                    self._wf_status("triage", main_sha=st.get("red_sha"), last_result="red", failed_items=st["items"])
                    return
            if st.get("queue"):
                nxt = st["queue"][0]
                if self._launch("loopprobe", "gate", nxt, cores, to, base_sha=nxt, head_sha=nxt):
                    st["queue"].pop(0)
                    store.set_meta(self.con, "loop_bisect", json.dumps(st))
                return
            if st["hi"] - st["lo"] <= 1:
                self._loop_conclude(st)
                return
            mid = (st["lo"] + st["hi"]) // 2
            self._launch("loopprobe", "gate", st["cands"][mid], cores, to, base_sha=st["cands"][mid], head_sha=st["cands"][mid])
            return
        main = self.main
        if store.get_meta(self.con, "loop_tested") != main:
            self._launch("baseline", "gate", main, cores, to, base_sha=main)
        else:
            self._wf_status("green" if store.get_meta(self.con, "loop_green") == main else "idle", main_sha=main)

    def _loop_result(self, sha, res, summ=None):
        """對最新 main 的一次 gate 結果：沒有新紅就記為綠；有新紅就從上次綠的 main 到這個 sha 二分找兇手。"""
        c = self.con
        store.set_meta(c, "loop_tested", sha)
        new_red = set(res.failed_items) - self._known_red("gate", sha) if res.status != "pass" else set()
        if res.status == "pass" or not new_red:
            store.set_meta(c, "loop_green", sha)
            store.set_meta(c, "loop_bisect", "")
            store.emit(c, "loop_green", None, {"main_sha": sha, "detail": "背景測試：主線沒有新紅項"}, force=True)
            self._wf_status("green", main_sha=sha, last_run_at=self.clock(), last_result="green")
            return
        green = store.get_meta(c, "loop_green")
        cands = self.repo.commits_between(green, sha) if green and hasattr(self.repo, "commits_between") else []
        store.emit(c, "loop_red", None, {"main_sha": sha, "failed_items": sorted(new_red), "detail": f"{len(cands)} 個提交待二分"}, force=True)
        self._wf_status("bisecting" if len(cands) > 1 else "red", main_sha=sha, last_run_at=self.clock(),
                        last_result="red", failed_items=sorted(new_red))
        if len(cands) > 1:
            st = {"items": sorted(new_red), "cands": cands, "lo": -1, "hi": len(cands) - 1, "phase": "bisect",
                  "queue": [], "red_sha": sha}
            if self.cfg.loop_triage == "code_first":
                st["phase"] = "triage"
                st["deadline"] = self.clock() + self.cfg.loop_triage_wait_s
                commits = []
                for cs in cands:
                    row = c.execute("SELECT name FROM branches WHERE merged_sha=? LIMIT 1", (cs,)).fetchone()
                    commits.append({"sha": cs, "branch": row["name"] if row else None,
                                    "subject": self.repo.commit_subject(cs) if hasattr(self.repo, "commit_subject") else ""})
                store.emit(c, "loop_triage", None, {
                    "main_sha": sha, "green_sha": green, "commits": commits, "deadline_at": st["deadline"],
                    "failed_items": [{"item_id": i, "summary": ((summ or {}).get(i) or "")[:1200]} for i in sorted(new_red)],
                    "detail": f"請先看代碼找嫌疑分支（{len(cands)} 個提交），{self.cfg.loop_triage_wait_s} 秒內回報，逾時改二分"}, force=True)
            store.set_meta(c, "loop_bisect", json.dumps(st))
        elif len(cands) == 1:
            self._loop_conclude({"items": sorted(new_red), "cands": cands, "lo": -1, "hi": 0})

    def _loop_suspect_idx(self, st):
        """看板在清單裡回報的嫌疑分支（要對上這次紅的 main）→ 它在候選提交裡的位置；沒有或對不上回 None。"""
        d = getattr(getattr(self.source, "drops", None), "suspect", None) if self.source is not None else None
        if not d or not str(st.get("red_sha", "")).startswith(d["main_sha"][:12]):
            return None
        row = self.con.execute("SELECT merged_sha FROM branches WHERE name=?", (d["branch"],)).fetchone()
        if row and row["merged_sha"] in st["cands"]:
            return st["cands"].index(row["merged_sha"])
        return None

    def _on_loopprobe(self, j, res):
        if res.status in ("infra", "timeout", "cancelled"):
            self._infra_retry("loopprobe")
            return
        raw = store.get_meta(self.con, "loop_bisect")
        if not raw:
            return
        st = json.loads(raw)
        if j.head_sha not in st["cands"]:
            return
        idx = st["cands"].index(j.head_sha)
        red = bool(set(res.failed_items) & set(st["items"])) if res.status != "pass" else False
        if red:
            st["hi"] = min(st["hi"], idx)
        else:
            st["lo"] = max(st["lo"], idx)
        store.set_meta(self.con, "loop_bisect", json.dumps(st))

    def _loop_conclude(self, st):
        c = self.con
        sha = st["cands"][st["hi"]]
        row = c.execute("SELECT name FROM branches WHERE merged_sha=? LIMIT 1", (sha,)).fetchone()
        who = row["name"] if row else None
        store.emit(c, "loop_culprit", who, {"culprit_branch": who, "culprit_commit": sha, "failed_items": st["items"],
                                            "main_sha": self.main, "hotfix_id": "HOTFIX-" + sha[:8],
                                            "detail": f"背景測試找到兇手：提交 {sha[:9]}（{who or '不明分支'}）讓 {','.join(st['items'])[:300]} 變紅"}, force=True)
        self._wf_status("red", main_sha=self.main, last_result="red", failed_items=st["items"])
        store.set_meta(c, "loop_bisect", "")

    # ---------- 自測 ----------
    def _chain(self, name) -> List:
        rows, r = [], store.get_branch(self.con, name)
        while r is not None and (not rows or r["state"] != S.MERGED.value):
            rows.append(r)
            r = store.get_branch(self.con, r["parent"]) if r["parent"] else None
        return rows[::-1]

    def _timing_only(self, items) -> bool:
        """這批新紅項全是計時類（項目名符合 policy.timing_items）：負載下偶發紅，先在同一個提交上單獨重跑。"""
        import re
        if not self.cfg.flaky_retry or not items or not self.cfg.timing_items:
            return False
        return all(any(re.search(p, i) for p in self.cfg.timing_items) for i in items)

    def _board_prio(self, name) -> float:
        """看板清單上這張卡的優先序（數字小的先）。跟手動的 jet priority 共存：手動值（大的先）優先比，同值才看這個；
        沒有來源、清單沒帶就排最後（同為沒帶的照原順序）。"""
        e = (self._entries or {}).get(name)
        p = e.get("priority") if e else None
        return float(p) if isinstance(p, (int, float)) and not isinstance(p, bool) else float("inf")

    def _dispatch_selftests(self):
        c = self.con
        rows = [r for r in store.list_branches(c, S.PENDING.value) if not self._direct_docs(r)]
        rows.sort(key=lambda r: (-r["prio_override"], self._board_prio(r["name"]), r["seen_at"], r["name"]))
        in_use = sum(j.units for j in self.jobs.values() if j.kind == "selftest")
        slots = sum(1 for j in self.jobs.values() if j.kind == "selftest")
        wanted = self.batch is not None or bool(store.list_branches(c, S.QUEUED.value))
        for r in rows:
            if self.cfg.order == "review_first" and not self._approved(r):
                continue                                      # 先審後測：沒放行的版本不花測試資源
            if r["name"] in self._blocked or self._parent_waiting(r):
                continue
            if self.cfg.max_selftest_slots and slots >= self.cfg.max_selftest_slots:
                break                                         # 同時自測槽數上限（硬上限，不論有沒有批測）
            if wanted and in_use + self.cfg.selftest_cores > self.cfg.reserve_for_selftest:
                break
            if wanted and self.cfg.max_selftest_slots_with_batch and slots >= self.cfg.max_selftest_slots_with_batch:
                break                                         # 有批測在等：自測槽數的硬上限
            if r["parent"]:
                if any(x["state"] in (S.REJECTED.value, S.HALTED.value) for x in self._chain(r["name"])[:-1]):
                    continue                                  # 祖先被退回：等它好了再測
                if r["name"] in self._wait_proven:
                    if self._ancestor_unproven(r["name"]):
                        continue
                    self._wait_proven.discard(r["name"])
            if self._stale_parent(r):
                self._reject_stale(r)
                continue
            chain = self._chain(r["name"])
            res = self.repo.stack(self.main, [(x["name"], x["head_sha"]) for x in chain])
            if len(res.kept) != len(chain):
                why = "疊不上 main（撞檔）" if len(chain) == 1 else "疊不上 main 加父卡（撞檔）"
                store.apply_event(c, r["name"], Event.CONFLICT_MAIN, reject_reason=why)
                store.add_event(c, "conflict", r["name"], "自測前" + why)
                continue
            if not self._launch("selftest", "selftest", res.prefixes[-1], self.cfg.selftest_cores,
                                self.cfg.selftest_timeout_s, branch=r["name"],
                                base_sha=self.main, head_sha=r["head_sha"]):
                break
            store.apply_event(c, r["name"], Event.SELFTEST_START)
            jr = next((j for j in self.jobs.values() if j.branch == r["name"] and j.kind == "selftest"), None)
            if jr is not None:
                store.emit(c, "selftest_started", r["name"], {"run_id": jr.run_id, "cores": ",".join(map(str, jr.cores))},
                           commit_sha=r["head_sha"])
            in_use += self.cfg.selftest_cores
            slots += 1

    def _on_selftest(self, j, res):
        with store.event_ctx(j.branch, j.approved_sha, j.retest):
            self._on_selftest_inner(j, res)

    def _on_selftest_inner(self, j, res):
        c = self.con
        row = store.get_branch(c, j.branch)
        if row is None or row["state"] != S.SELFTESTING.value:
            return
        if res.status in ("infra", "cancelled"):
            self._infra_retry("selftest:" + j.branch)
            store.apply_event(c, j.branch, Event.RETRY)
            return
        if res.status == "timeout" and getattr(res, "overloaded", False) \
                and self._overload.get(j.branch, 0) < MAX_OVERLOAD_RETRY:
            # 機器超載造成的逾時不是分支的錯：重排，不退回（連續幾次還逾時才算它的）
            self._overload[j.branch] = self._overload.get(j.branch, 0) + 1
            store.add_info_event(c, "overload", j.branch, f"自測逾時但系統超載（工作只拿到 {res.cpu_s or 0:.0f} 秒 CPU），重排")
            store.apply_event(c, j.branch, Event.RETRY)
            return
        if self.source is not None and (j.approved_sha, j.retest) != (row["approved_sha"], row["retest"]):
            self._overload.pop(j.branch, None)               # 這是照舊放行（或舊重測計數）測的結果：丟掉，照現在的放行重測
            store.apply_event(c, j.branch, Event.RETRY)
            return
        self._overload.pop(j.branch, None)
        base = self._known_red("selftest", self.main)
        self._store_items(j.run_id, res, base)
        if self._red(res, base) and self._ancestor_unproven(j.branch):
            self._wait_proven.add(j.branch)
            store.apply_event(c, j.branch, Event.RETRY)       # 父卡的錯不算子卡的：父卡過了再測一次
            return
        if self._red(res, base):
            new_red = set(res.failed_items) - base
            if res.status == "fail" and new_red and self._timing_only(new_red) \
                    and (j.branch, row["head_sha"]) not in self._retried and j.branch not in self._rc:
                self._retried.add((j.branch, row["head_sha"]))
                self._rc_wait[j.branch] = {"new_red": new_red, "items": list(res.failed_items), "run_id": j.run_id,
                                           "approved": (j.approved_sha, j.retest), "mode": "retry"}
                return                                         # 計時類新紅：先在同一個提交上只重跑那幾項（單獨一個工作），第二次綠就當抖動
            if self.cfg.recheck_red and res.status == "fail" and new_red and j.branch not in self._rc:
                self._rc_wait[j.branch] = {"new_red": new_red, "items": list(res.failed_items), "run_id": j.run_id,
                                           "approved": (j.approved_sha, j.retest)}
                return                                         # 先不判：新紅項放到乾淨 main 上單獨重跑，main 也紅就不是這張卡的錯
            why = "自測逾時" if res.status == "timeout" else \
                "自測紅：" + ",".join(sorted(set(res.failed_items) - base)) 
            store.apply_event(c, j.branch, Event.SELFTEST_FAIL, reject_reason=why)
            store.add_event(c, "rejected", j.branch, why)
        else:
            mr = self._main_red()
            if mr and not (set(res.failed_items) & set(mr["items"])):     # main 自己紅著，這張卡的自測沒有這些紅項：它能修
                self._mark_fixer(j.branch, row["head_sha"])
            store.apply_event(c, j.branch, Event.SELFTEST_PASS)

    # ---------- 紅項複驗、main 自己紅、純文件保險 ----------
    def _start_rechecks(self):
        for name, rec in list(self._rc_wait.items()):
            row = store.get_branch(self.con, name)
            if row is None or row["state"] != S.SELFTESTING.value:
                self._rc_wait.pop(name, None)
                continue
            sha = self.main
            if rec.get("mode") == "retry":
                chain = self._chain(name)
                st = self.repo.stack(self.main, [(x["name"], x["head_sha"]) for x in chain])
                if len(st.kept) != len(chain):
                    self._rc_wait.pop(name, None)
                    store.apply_event(self.con, name, Event.RETRY)   # 疊不上了：回待自測，照一般流程處理
                    continue
                sha = st.prefixes[-1]
            if self._launch("recheck", "selftest", sha, self.cfg.selftest_cores, self.cfg.selftest_timeout_s,
                            branch=name, base_sha=self.main, only=",".join(sorted(rec["new_red"]))):
                self._rc[name] = self._rc_wait.pop(name)

    def _on_recheck(self, j, res):
        c = self.con
        rec = self._rc.pop(j.branch, None)
        row = store.get_branch(c, j.branch)
        if rec is None or row is None or row["state"] != S.SELFTESTING.value:
            return
        ctx = rec["approved"]
        with store.event_ctx(j.branch, ctx[0], ctx[1]):
            if res.status in ("infra", "cancelled"):
                self._infra_retry("recheck:" + j.branch)
                store.apply_event(c, j.branch, Event.RETRY)
                return
            if self.source is not None and tuple(ctx) != (row["approved_sha"], row["retest"]):
                store.apply_event(c, j.branch, Event.RETRY)        # 照舊放行測的結果：丟掉，照現在的放行重測
                return
            if rec.get("mode") == "retry":
                still = set(res.failed_items) & rec["new_red"] if res.status in ("fail", "pass") else set(rec["new_red"])
                if res.status == "pass" or not still:
                    store.add_info_event(c, "flaky_retry", j.branch,
                                         "計時類項目第一次紅、同一個提交單獨重跑綠，算抖動放過：" + ",".join(sorted(rec["new_red"])))
                    store.apply_event(c, j.branch, Event.SELFTEST_PASS)
                else:
                    rec2 = dict(rec, new_red=still, mode="main")   # 還是紅：接著照原本流程（在乾淨 main 上確認是不是 main 自己紅）
                    if self.cfg.recheck_red:
                        self._rc_wait[j.branch] = rec2
                    else:
                        why = "自測紅：" + ",".join(sorted(still))
                        store.apply_event(c, j.branch, Event.SELFTEST_FAIL, reject_reason=why)
                        store.add_event(c, "rejected", j.branch, why)
                return
            main_red = (set(res.failed_items) & rec["new_red"]) if res.status in ("fail", "pass") else set()
            import re
            pats = list(self.cfg.main_red_ignore)
            if self.cfg.gate_registry_dir:         # 登記檔標 gate:false 的店（施工中、每晚才跑）：項目名含店 id 的不擋
                try:
                    pats += [r"(?<![a-z0-9])" + re.escape(x) + r"(?![a-z0-9])"
                             for x in self.repo.nongate_ids(self.main, self.cfg.gate_registry_dir)]
                except Exception:
                    pass
            gate_red = {i for i in main_red if not any(re.search(p, i) for p in pats)}
            if gate_red:                       # 只有範圍內（沒被 policy.main_red_ignore 排除）的項目才算「main 自己紅」並擋批次
                self._declare_main_red(gate_red, j.branch)
            ignored = main_red - gate_red
            if ignored:                        # 範圍外的：寫進這個 main 的基準（批測才不會當成新紅），但不宣告 main 紅、不擋批次
                for layer in ("gate", "selftest"):
                    b = store.get_baseline(c, self.main, layer) or set()
                    store.set_baseline(c, self.main, layer, b | ignored, copied=True)
                store.add_info_event(c, "main_red_ignored", j.branch, "乾淨 main 上也紅但在 main_red_ignore 範圍外，只記錄、不擋：" + ",".join(sorted(ignored)))
            remaining = rec["new_red"] - main_red
            if remaining:
                why = "自測紅：" + ",".join(sorted(remaining))
                store.apply_event(c, j.branch, Event.SELFTEST_FAIL, reject_reason=why)
                store.add_event(c, "rejected", j.branch, why)
            else:
                store.add_info_event(c, "recheck_cleared", j.branch,
                                     "自測的紅項在乾淨 main 上也紅（" + ",".join(sorted(main_red)) + "），不算這張卡的錯")
                store.apply_event(c, j.branch, Event.SELFTEST_PASS)

    def _recent_merges(self, n=8) -> List[str]:
        rows = self.con.execute("SELECT name, docs_only FROM branches WHERE state='merged' ORDER BY updated_at DESC LIMIT ?",
                                (n,)).fetchall()
        return [r["name"] + ("（純文件）" if r["docs_only"] == 1 else "") for r in rows]

    def _main_red(self) -> Optional[dict]:
        """目前 main 自己有沒有被發現紅（跟 main 的版本綁在一起；main 一動就失效；人可以 ack）。"""
        raw = store.get_meta(self.con, "main_red")
        if not raw:
            return None
        try:
            d = json.loads(raw)
        except ValueError:
            return None
        return d if d.get("main") == self.main and not d.get("acked") else None

    def _declare_main_red(self, items: Set[str], trigger: str):
        mr = self._main_red() or {"main": self.main, "items": [], "fixers": {}, "at": self.clock()}
        new = sorted(set(items) - set(mr["items"]))
        mr["items"] = sorted(set(mr["items"]) | set(items))
        store.set_meta(self.con, "main_red", json.dumps(mr, ensure_ascii=False))
        for layer in ("gate", "selftest"):                    # 寫進這個 main 的基準（不算進歷史；main 一動就失效）
            b = store.get_baseline(self.con, self.main, layer) or set()
            store.set_baseline(self.con, self.main, layer, b | set(items), copied=True)
        self._maybe_pause_docs(set(items))
        if new:
            msg = (f"main 自己紅了：{'、'.join(mr['items'])}（{trigger} 的自測發現，在乾淨 main 上單獨重跑也紅）；"
                   f"最近合入：{'、'.join(self._recent_merges()) or '無'}。"
                   "這些紅項修好之前不會推任何批次進 main（能讓它們轉綠的卡除外）")
            log.error(msg)
            store.add_event(self.con, "queue_error", None, msg[:900])

    def _mark_fixer(self, name: str, head_sha: str):
        mr = self._main_red()
        if mr:
            mr["fixers"][name] = head_sha
            store.set_meta(self.con, "main_red", json.dumps(mr, ensure_ascii=False))

    def _blocked_by_main_red(self, r) -> bool:
        """main 自己紅著時，只有「自測時這些紅項是綠的」那個版本的卡可以進批。"""
        mr = self._main_red()
        return bool(mr) and mr["fixers"].get(r["name"]) != r["head_sha"]

    def _docs_paused(self) -> Optional[dict]:
        raw = store.get_meta(self.con, "docs_direct_paused")
        try:
            return json.loads(raw) if raw else None
        except ValueError:
            return None

    def _direct_docs(self, r) -> bool:
        return r["docs_only"] == 1 and not self._docs_paused()

    def _note_docs_merge(self, name, sha, main_before):
        raw = store.get_meta(self.con, "docs_merges")
        try:
            lst = json.loads(raw) if raw else []
        except ValueError:
            lst = []
        lst.append({"name": name, "sha": sha, "at": self.clock()})
        store.set_meta(self.con, "docs_merges", json.dumps(lst[-50:], ensure_ascii=False))

    def _maybe_pause_docs(self, new: Set[str]):
        """有新紅項、而且上一次實測後有純文件直接合入：點名那幾筆、暫停直接合入。"""
        raw = store.get_meta(self.con, "docs_merges")
        try:
            merges = json.loads(raw) if raw else []
        except ValueError:
            merges = []
        if new and merges and not self._docs_paused():
            names = [m["name"] for m in merges][-10:]
            msg = (f"main 出現新紅項 {'、'.join(sorted(new))}，而上一次實測後有 {len(merges)} 筆純文件直接合入"
                   f"（最近：{'、'.join(names)}）：疑似分類誤判。已暫停「純文件直接合入」，之後純文件卡照一般卡測；"
                   "查完用 jet resume-docs 恢復")
            store.set_meta(self.con, "docs_direct_paused", json.dumps({"at": self.clock(), "items": sorted(new), "merges": names},
                                                                       ensure_ascii=False))
            log.error(msg)
            store.add_event(self.con, "queue_error", None, msg[:900])

    def _check_docs_guard(self, sha, failed: Set[str]):
        """真的量過一次基準（不是沿用）：跟上一次真基準比，出現新紅、而且這段期間有純文件直接合入 → 點名並暫停直接合入。"""
        raw = store.get_meta(self.con, "last_real_baseline")
        try:
            prev = set(json.loads(raw).get("failed", [])) if raw else None
        except ValueError:
            prev = None
        raw = store.get_meta(self.con, "docs_merges")
        try:
            merges = json.loads(raw) if raw else []
        except ValueError:
            merges = []
        new = set(failed) - prev if prev is not None else set()
        self._maybe_pause_docs(new)
        store.set_meta(self.con, "last_real_baseline", json.dumps({"main": sha, "failed": sorted(failed)}))
        store.set_meta(self.con, "docs_merges", "[]")

    def _guard_info(self) -> dict:
        mr = self._main_red()
        return {"main_red": ({"items": mr["items"], "fixers": sorted(mr["fixers"]), "since": mr["at"]} if mr else None),
                "docs_paused": self._docs_paused()}

    def _ancestor_unproven(self, name) -> bool:
        """疊在它下面的父卡還沒自測綠：這次紅可能是父卡造成的，不能算在這條分支頭上。"""
        ok = (S.SELFTEST_GREEN.value, S.QUEUED.value, S.BATCHING.value)
        return any(x["state"] not in ok for x in self._chain(name)[:-1])

    def _reap_orphans(self):
        """保險：狀態說在跑、其實沒有對應工作的，退回上一步（不讓分支卡死）。"""
        live = {j.branch for j in self.jobs.values() if j.kind in ("selftest", "recheck")} | set(self._rc_wait)
        for r in store.list_branches(self.con, S.SELFTESTING.value):
            if r["name"] not in live:
                store.apply_event(self.con, r["name"], Event.RETRY)
        members = {n for n, _, _ in self.batch.members} if self.batch else set()
        for r in store.list_branches(self.con, S.BATCHING.value):
            if r["name"] not in members:
                store.apply_event(self.con, r["name"], Event.BATCH_DEFERRED)

    # ---------- 放行→排隊 ----------
    def _approved(self, r) -> bool:
        return (not self.cfg.require_approval) or store.is_approved(self.con, r["name"], r["content_id"])

    def _parent_ok(self, r) -> bool:
        sp = self._src_parent(r["name"])
        if sp and (store.get_branch(self.con, sp) is None or r["parent"] != sp):
            return False                                  # 清單說有父卡，但父卡的分支還沒到或還沒套上：不能排隊
        if not r["parent"]:
            return True
        p = store.get_branch(self.con, r["parent"])
        return p is None or p["state"] in (S.QUEUED.value, S.BATCHING.value, S.MERGED.value)

    def _promote(self):
        c = self.con
        changed = True
        while changed:
            changed = False
            for r in store.list_branches(c, S.SELFTEST_GREEN.value):
                if self._approved(r) and self._parent_ok(r) and r["name"] not in self._blocked:
                    store.apply_event(c, r["name"], Event.READY,
                                      queued_at=r["queued_at"] if r["queued_at"] else self.clock())
                    changed = True
        for r in store.list_branches(c, S.QUEUED.value):
            if not self._approved(r):
                store.apply_event(c, r["name"], Event.REVOKE)
            elif r["name"] in self._blocked or self._parent_waiting(r):
                store.apply_event(c, r["name"], Event.PARENT_CHANGED)   # 父卡不在了／還沒接上：排隊中的也回待自測等
        for r in store.list_branches(c, S.PENDING.value):
            if self._direct_docs(r) and not r["parent"] and not self._src_parent(r["name"]) \
                    and self._approved(r) and r["name"] not in self._blocked and not self._main_red():
                self._merge_docs_only(r)

    def _merge_docs_only(self, r):
        c, main = self.con, self.repo.main_sha()
        if not self._repull():
            return
        r = store.get_branch(c, r["name"])
        if r is None or r["state"] != S.PENDING.value or not self._approved(r) or not self._tracked_ok(r["name"]):
            return
        res = self.repo.stack(main, [(r["name"], r["head_sha"])])
        if len(res.kept) != 1:
            store.apply_event(c, r["name"], Event.CONFLICT_MAIN, reject_reason="疊不上 main（撞檔）")
            store.add_event(c, "conflict", r["name"], "純文件疊不上 main")
            return
        try:
            self.repo.fast_forward_main(res.prefixes[0], main)
        except MainMoved:
            return
        new = res.prefixes[0]
        self._note_docs_merge(r["name"], new, main)
        store.apply_event(c, r["name"], Event.DOCS_MERGED, merged_sha=new)
        self._after_merge([r["name"]], new, None, main_before=main)

    def _after_merge(self, names, new_main, failed: Optional[Set[str]], main_before: str):
        """合入後：基準、刪分支、子分支脫離、撞檔檢查。"""
        c = self.con
        for n in names:
            row = store.get_branch(c, n)
            c.execute("UPDATE branches SET parent=NULL WHERE parent=?", (n,))
            try:
                self.repo.delete_branch(n, row["head_sha"])
            except Exception as e:
                store.add_event(c, "queue_error", n, f"刪分支失敗：{e!r}")
        if failed is not None:
            self._set_baseline_all(new_main, failed)
        else:
            self._copy_baseline(main_before, new_main)
        self.main = new_main
        for r in store.list_branches(c, S.QUEUED.value):
            if r["parent"] is None and self.repo.conflicts_with(r["head_sha"], new_main):
                store.apply_event(c, r["name"], Event.CONFLICT_MAIN, reject_reason="跟 main 撞檔")
                store.add_event(c, "conflict", r["name"], "合入後跟新 main 撞檔")

    # ---------- 外部來源（清單是唯一依據；CARD_FLOW 第 5、6、9 節）----------
    def _source_step(self):
        if self.source is None:
            return
        err = self.source.poll()
        if err:
            self._source_error(err)
        if self.source.new_data:
            self.source.new_data = False
            self._apply_entries(self.source.entries)
        info = {"url_ok": self.source.error is None, "age_s": self.source.age(), "error": self.source.error,
                "entries": None if self._entries is None else len(self._entries)}
        self._put("source", info)
        self._drop_step()

    def _drop_step(self):
        """放棄防呆：清單帶來的 drop_checks 逐筆查（同一組 提交／目標／main／dropping_at 只算一次），
        結果變了或換了一輪放棄（dropping_at 變）才發一筆 drop_checked 事件。只讀 git，不動任何登記與測試。
        事件沒真的寫出去（沒有目的地要機器事件）時不記「已查過」，之後補上設定就會補發。"""
        src = self.source
        if src.drops_ver == 0:
            return
        form = getattr(src.drops, "form", "array")
        sig = (src.drops_ver, self.main)
        if sig == self._drop_sig:
            return
        self._drop_sig = sig
        checked = emitted = failed = 0
        for d in src.drops[:100]:
            card, br, sup = d["card"], d["branch"], d.get("superseded_by_branch")
            try:
                r = self.repo.drop_check(br, sup)
            except Exception as e:           # 查不出來就當沒查過，下一輪再試（不發「已包含」）
                log.warning("放棄查核 %s 失敗：%r", card, e)
                self._drop_sig = None
                failed += 1
                continue
            checked += 1
            res = {"unmerged": bool(r["unmerged"]), "covered": bool(r["covered"]), "target": r["target"],
                   "missing_count": int(r["missing_count"]), "missing_files": list(r["missing_files"])[:20]}
            if d.get("dropping_at") is not None:
                res["dropping_at"] = d["dropping_at"]
            key = json.dumps([r.get("branch_sha"), r.get("target_sha"), r.get("main_sha"), sup, d.get("dropping_at")],
                             sort_keys=True)
            sigr = json.dumps(["v2", card, key, res], sort_keys=True)     # v2：改版後所有現行的待查卡重發一次
            if store.get_meta(self.con, f"dcr:{card}", None) == sigr:
                continue                       # 同一輪、同樣的內容、結論沒變：不重發
            if store.emit(self.con, "drop_checked", br, dict(res, card=card)):
                store.set_meta(self.con, f"dcr:{card}", sigr)
                emitted += 1
        self._put("drop", {"at": self.clock(), "requested": len(src.drops), "checked": checked,
                           "emitted": emitted, "failed": failed, "form": form})

    def _source_error(self, msg: str):
        """拉不到、格式不對：記錄＋一筆 source_error 事件（同樣的錯只發一次），絕不動登記與放行。"""
        if msg != self._src_err_last:
            self._src_err_last = msg
            log.warning("外部來源：%s", msg)
            store.add_info_event(self.con, "source_error", None, msg[:500])

    def _apply_entries(self, entries):
        self._src_err_last = None
        self._entries = {e["branch"]: e for e in entries}
        store.sync_tracked(self.con, entries)

    def _repull(self) -> bool:
        """推 main 前重拉一次清單（每輪最多一次）。一定要成功才能推：失敗就這輪不推，過一會兒再試。"""
        if self.source is None:
            return True
        if self._repulled:
            return self._repull_ok
        if self.clock() < self._repull_retry_at:
            return False
        self._repulled = True
        err = self.source.refresh_now()
        self._repull_ok = err is None
        if err:
            self._repull_retry_at = self.clock() + 15
            self._source_error(err)
            self._note_error("外部來源", "推 main 前重拉清單失敗，這批只測不推（來源恢復後自動繼續）")
            return False
        if self._last_err_msg and self._last_err_msg.startswith("外部來源"):
            self._last_err_msg = None
        if self.source.new_data:
            self.source.new_data = False
            self._apply_entries(self.source.entries)
            self._reconcile_parents()
            self._apply_source_approvals()
        return True

    def _may_push(self) -> bool:
        """清單比設定的年齡還舊（或根本沒拉成功過）就只測不合。"""
        if self.source is None:
            return True
        age = self.source.age()
        if age is None:
            return False                              # 重啟後第一次拉取還沒完成：先不開批、不推，但不算「過期」，不發事件
        if age > self.cfg.source["max_age_s"]:
            self._note_error("外部來源", f"清單超過 {int(self.cfg.source['max_age_s'])} 秒沒更新成功，只測不合（來源恢復後自動繼續）")
            return False
        if self._last_err_msg and self._last_err_msg.startswith("外部來源："):
            self._last_err_msg = None
        return True

    def _tracked_ok(self, name) -> bool:
        return self.source is None or (self._entries is not None and name in self._entries)

    def _reconcile_parents(self):
        """清單上的父分支（含被改、被拿掉）套到分支上：放行保留（內容指紋不重算），測試從待自測重來。"""
        if self._entries is None:
            return
        for name, e in self._entries.items():
            row = store.get_branch(self.con, name)
            if row is None or row["state"] == S.MERGED.value:
                continue
            want = e["parent"]
            if want is not None:
                pr = store.get_branch(self.con, want)
                if pr is None:
                    continue                                  # 父還沒被掃到（或不在清單上：由擋住的檢查處理）
                if pr["state"] == S.MERGED.value:
                    want = None
            if row["parent"] != want:
                self._reparent(row, want)

    def _reparent(self, row, parent):
        name = row["name"]
        self._cancel_jobs(branch=name)
        if row["state"] == S.BATCHING.value:
            self._abort_batch("父分支變動")
        try:
            store.set_parent(self.con, name, parent, "source")
        except ValueError:
            return
        for ch in store.children(self.con, name):
            self._parent_changed(ch)

    def _judge_approved(self, row, ap: str) -> str:
        """清單上的放行版本對這條分支算不算數：yes／no（內容不同）／unknown（解不出來，等 fetch）。"""
        name = row["name"]
        if ap == row["head_sha"]:
            return "yes"
        if 7 <= len(ap) < 40 and all(ch in "0123456789abcdefABCDEF" for ch in ap) \
                and row["head_sha"].lower().startswith(ap.lower()):
            return "yes"                       # 放行版本寫成縮短的提交碼（前 7 碼以上），對得上目前的頭就算
        cid = store.known_content_id(self.con, name, ap)
        if cid is None:
            now = self.clock()
            last = self._unres.get((name, ap))
            if last is None or now - last >= 30:
                try:
                    cid = self.repo.content_id(ap, self._base_for(row))
                    store.note_sha(self.con, name, ap, cid)
                    self._unres.pop((name, ap), None)
                    self._unres_since.pop((name, ap), None)
                except Exception:
                    self._unres[(name, ap)] = now
            if cid is None:
                # 這些情況一律 unknown（不撤放行、不發 superseded）：①本程序還沒成功掃過遠端（重啟後物件、清單都還沒到）；
                # ②放行版本就是遠端現在的頭（只是資料庫裡的頭還沒跟上，內容 id 暫時算不出來）。
                # 只有「掃過遠端、放行版本不是現在的頭、又一直拿不到」才算內容不同（被新提交蓋掉）。
                tip = (self._tips or {}).get(name)
                if self._tips is not None and tip and len(ap) < 40 and tip.lower().startswith(ap.lower()):
                    return "yes" if row["head_sha"].lower().startswith(ap.lower()) else "unknown"
                if len(ap) < 40 and store.get_meta(self.con, f"short_ap:{name}") != ap:
                    store.set_meta(self.con, f"short_ap:{name}", ap)
                    store.add_info_event(self.con, "approval_unresolved", name,
                                         f"放行版本 {ap} 不是完整 40 碼、解析不出對應提交；請用完整提交碼重新放行")
                if self._tips is None or self._tips.get(name) == ap:
                    self._unres_since.pop((name, ap), None)
                    return "unknown"
                since = self._unres_since.setdefault((name, ap), now)
                return "no" if now - since >= UNKNOWN_GRACE_S else "unknown"
        return "yes" if cid == row["content_id"] else "no"

    def _apply_source_approvals(self):
        """清單 → 每條分支的放行記號、重測計數、父卡擋住。清單是唯一依據；每輪都對一次。"""
        if self._entries is None:
            return
        c = self.con
        self._blocked = {}
        for row in store.list_branches(c):
            name = row["name"]
            e = self._entries.get(name)
            if e is None or row["state"] == S.MERGED.value:
                continue
            ap, rt = e["approved_sha"], e["retest"]
            if row["approved_sha"] != ap or row["retest"] is None:
                c.execute("UPDATE branches SET approved_sha=?, retest=COALESCE(retest, ?) WHERE name=?", (ap, rt, name))
            if row["retest"] is not None and rt > row["retest"]:
                self._retest(row, rt)
                row = store.get_branch(c, name)
            self._check_blocked(row, e)
            active = store.is_approved(c, name, row["content_id"])
            if ap is None:
                if active:
                    store.revoke(c, name, "source")
                continue
            verdict = self._judge_approved(row, ap)
            if verdict == "yes":
                if not active:
                    store.approve(c, name, "source")
            else:
                if active:
                    store.revoke(c, name, "source")
                if verdict == "no" and store.get_meta(c, f"sup:{name}") != row["head_sha"]:
                    store.set_meta(c, f"sup:{name}", row["head_sha"])
                    st = row["state"]
                    store.emit(c, "superseded", name,
                               {"from": st, "to": st, "reason": f"放行版本 {ap[:8]} 的內容跟目前 {row['head_sha'][:8]} 不同，放行失效"},
                               row["head_sha"])

    def _src_parent(self, name) -> Optional[str]:
        """來源模式：清單上這條分支還沒合併的父卡（沒有來源或沒有父卡回 None）。"""
        if self.source is None or self._entries is None:
            return None
        e = self._entries.get(name)
        par = e["parent"] if e else None
        if not par:
            return None
        pr = store.get_branch(self.con, par)
        return None if (pr is not None and pr["state"] == S.MERGED.value) else par

    def _check_blocked(self, row, e):
        """父卡被放棄（清單說 parent_status=dropped）：擋住這條分支，發 parent_blocked（同一條只發一次）。
        父卡只是還沒推、還沒人領、被還卡（open）：只是等，不發事件。"""
        name, par = row["name"], e["parent"]
        key = f"pb:{name}"
        blocked = None
        if par and par not in self._entries and e.get("parent_status") == "dropped":
            pr = store.get_branch(self.con, par)
            if pr is None or pr["state"] != S.MERGED.value:
                blocked = par
        if blocked is None:
            if store.get_meta(self.con, key):
                store.set_meta(self.con, key, "")
            return
        self._blocked[name] = f"父卡 {blocked} 被放棄"
        if store.get_meta(self.con, key) != blocked:
            store.set_meta(self.con, key, blocked)
            st = row["state"]
            store.emit(self.con, "parent_blocked", name,
                       {"from": st, "to": st, "reason": f"父卡 {blocked} 被放棄，請改父卡"}, row["head_sha"])

    def _retest(self, row, rt: int):
        """清單上的重測計數變大：清掉退回／暫停，從待自測重來（放行保留）。"""
        name = row["name"]
        self.con.execute("UPDATE branches SET retest=? WHERE name=?", (rt, name))
        if row["state"] in (S.REJECTED.value, S.HALTED.value):
            self._overload.pop(name, None)
            store.apply_event(self.con, name, Event.RESUME, fail_count=0, reject_reason=None)

    def _parent_waiting(self, r) -> bool:
        """這條分支現在不該測：清單上有父卡但父卡的分支還沒到、不在清單上（還沒領／被還卡）、或還沒套上；
        先審後測時父卡還要在隊伍裡（已放行）。"""
        sp = self._src_parent(r["name"])
        if sp:
            pr = store.get_branch(self.con, sp)
            if pr is None or sp not in (self._entries or {}) or r["parent"] != sp:
                return True
        if self.cfg.order == "review_first" and r["parent"]:
            p = store.get_branch(self.con, r["parent"])
            if p is not None and p["state"] != S.MERGED.value:
                if p["state"] in (S.SELFTESTING.value, S.QUEUED.value, S.BATCHING.value):
                    return False
                if p["state"] == S.SELFTEST_GREEN.value and self._approved(p):
                    return False
                return True                                   # 父卡還沒放行進隊伍（待自測、退回、暫停、放行被撤）：等
        return False

    def _stale_parent(self, r) -> bool:
        """父卡還沒合併、而且父卡現在的版本不是子卡的祖先（父卡改版了，子卡還沒重新接上）。"""
        if not r["parent"]:
            return False
        p = store.get_branch(self.con, r["parent"])
        if p is None or p["state"] == S.MERGED.value:
            return False
        key = (p["head_sha"], r["head_sha"])
        if key not in self._anc_cache:
            try:
                self._anc_cache[key] = bool(self.repo.is_ancestor(p["head_sha"], r["head_sha"]))
            except Exception:
                self._anc_cache[key] = True               # 查不出來就不擋（不誤殺）；真正的疊不上由 stack 擋
            if len(self._anc_cache) > 5000:
                self._anc_cache.clear()
        return not self._anc_cache[key]

    def _reject_stale(self, r):
        why = "父卡已更新，請重新接到父卡上"
        store.apply_event(self.con, r["name"], Event.CONFLICT_MAIN, reject_reason=why)
        store.add_event(self.con, "conflict", r["name"], why)

    def _enforce_review_first(self):
        """先審後測：放行沒了（撤回、內容變了）的版本，正在跑的自測停掉、回待自測。"""
        if self.cfg.order != "review_first":
            return
        for r in store.list_branches(self.con, S.SELFTESTING.value):
            if not self._approved(r):
                self._cancel_jobs(branch=r["name"])
                store.apply_event(self.con, r["name"], Event.RETRY)

    def _valid_prefix(self, b, m: int) -> int:
        """批測綠到第 m 條，推 main 前再確認：只能推「全部成員都還有效」的最大已測綠點。"""
        c = self.con
        first_bad = None
        for i, (name, cid, _) in enumerate(b.members[:m]):
            r = store.get_branch(c, name)
            sp = self._src_parent(name)
            before = {x[0] for x in b.members[:i]}
            ok = (r is not None and r["state"] == S.BATCHING.value and r["content_id"] == cid
                  and self._approved(r) and self._tracked_ok(name) and name not in self._blocked
                  and not self._stale_parent(r) and (not sp or sp in before)
                  and (not r["parent"] or r["parent"] in before or
                       (store.get_branch(c, r["parent"]) or {"state": "merged"})["state"] == S.MERGED.value))
            if not ok:
                first_bad = i
                break
        if first_bad is None:
            return m
        greens = [sz for d in (b.results, b.probes) for sz, g in d.items() if g == GREEN and sz <= first_bad]
        return max(greens, default=0)

    # ---------- 批測 ----------
    def _truncate_batch(self, name: str, why: str):
        """成員 name 有新提交／消失：只把它和排在它後面的踢出批，前面的測點與結果照用、照合。
        它是第一條（前面沒有東西可救）時等於整批作廢。"""
        b = self.batch
        if b is None:
            return
        k = next((i for i, m in enumerate(b.members) if m[0] == name), None)
        if k is None:
            return
        if k == 0:
            self._abort_batch(why)
            return
        for rid, j in list(self.jobs.items()):                      # 含被踢那條的測點作廢（大小 > k）
            if j.kind in ("point", "probe", "pretry") and j.batch_id == b.id and (j.size or 0) > k:
                try:
                    self.executor.cancel(j.handle)
                finally:
                    self._release_job(rid, "cancelled")
        for n, _, _ in b.members[k:]:
            if n == name:
                continue                                           # 它自己由呼叫端走 PUSHED／刪除
            r = store.get_branch(self.con, n)
            if r is not None and r["state"] == S.BATCHING.value:
                store.apply_event(self.con, n, Event.BATCH_DEFERRED)
        self.con.execute("DELETE FROM batch_members WHERE batch_id=? AND pos>?", (b.id, k))
        b.members = b.members[:k]
        b.prefixes = b.prefixes[:k]
        for d in (b.results, b.probes, b.failed):
            for sz in [z for z in d if z > k]:
                del d[sz]
        b.launched = {(kd, sz) for kd, sz in b.launched if sz <= k}
        for n in list(b.ctx):
            if n not in {m[0] for m in b.members}:
                del b.ctx[n]
        log.info("批縮短到前 %d 條：%s（%s）", k, name, why)

    def _abort_batch(self, why: str):
        b = self.batch
        if b is None:
            return
        self._cancel_jobs(batch=True)
        for name, _, _ in b.members:
            r = store.get_branch(self.con, name)
            if r is not None and r["state"] == S.BATCHING.value:
                store.apply_event(self.con, name, Event.BATCH_DEFERRED)
        self.con.execute("UPDATE batches SET status='aborted', finished_at=? WHERE id=?", (self.clock(), b.id))
        log.info("批作廢：%s", why)
        self.batch = None

    def _select(self):
        """依順序挑最多 N 條；子一定在父後面、而且父也被選上。"""
        rows = [r for r in store.list_branches(self.con, S.QUEUED.value) if not self._direct_docs(r)]
        rows = [r for r in rows if not self._blocked_by_main_red(r)]       # main 自己紅著：只有能修它的卡可以進批
        rows.sort(key=lambda r: (-r["prio_override"], self._board_prio(r["name"]), r["queued_at"] or 0, r["name"]))
        chosen: List = []
        names: Set[str] = set()
        pend = rows
        progress = True
        while progress and len(chosen) < self.cfg.batch_max:
            progress = False
            rest = []
            for r in pend:
                if len(chosen) >= self.cfg.batch_max:
                    rest.append(r)
                    continue
                if r["parent"] is None or r["parent"] in names:
                    chosen.append(r)
                    names.add(r["name"])
                    progress = True
                else:
                    rest.append(r)
            pend = rest
        return chosen

    def _start_batch(self):
        c, main = self.con, self.main
        if not self._may_push():
            return                                        # 清單太舊：不開新批測（只做自測）
        cand = self._select()
        for r in list(cand):                              # 父卡改版、子卡沒重接：退回，不進批
            if self._stale_parent(r) or (self._src_parent(r["name"]) and
                                         (store.get_branch(c, self._src_parent(r["name"])) is None or r["parent"] != self._src_parent(r["name"]))):
                if self._stale_parent(r):
                    self._reject_stale(r)
                cand.remove(r)
        keep = True
        while keep:                                        # 父卡不在這批裡的，子孫一起留下
            keep = False
            names0 = {r["name"] for r in cand}
            for r in list(cand):
                if r["parent"] and r["parent"] not in names0:
                    pr = store.get_branch(c, r["parent"])
                    if pr is not None and pr["state"] != S.MERGED.value:
                        cand.remove(r)
                        keep = True
        # 撞 main 的先退回
        ok = []
        for r in cand:
            if r["parent"] is None and self.repo.conflicts_with(r["head_sha"], main):
                store.apply_event(c, r["name"], Event.CONFLICT_MAIN, reject_reason="跟 main 撞檔")
                store.add_event(c, "conflict", r["name"], "組批時跟 main 撞檔")
            else:
                ok.append(r)
        # 被退回的父分支，子孫一起留下（否則父的改動會經由子分支進 main）
        gone = {r["name"] for r in cand if r not in ok}
        grow = True
        while grow:
            grow = False
            for r in ok:
                if r["name"] not in gone and r["parent"] in gone:
                    gone.add(r["name"])
                    grow = True
        cand = [r for r in ok if r["name"] not in gone]
        res = None
        while cand:
            res = self.repo.stack(main, [(r["name"], r["head_sha"]) for r in cand])
            kept = set(res.kept)
            skipped = {cand[i]["name"] for i in range(len(cand)) if i not in kept}
            if not skipped:
                break
            # 略過的（跟同批撞檔）留下一批，連同它的子孫
            drop = set(skipped)
            grow = True
            while grow:
                grow = False
                for r in cand:
                    if r["name"] not in drop and r["parent"] in drop:
                        drop.add(r["name"])
                        grow = True
            cand = [r for r in cand if r["name"] not in drop]
        if not cand or res is None:
            return
        members = [(r["name"], r["content_id"], r["head_sha"]) for r in cand]
        cur = c.execute("INSERT INTO batches(base_sha,status,started_at) VALUES(?,?,?)",
                        (main, "testing", self.clock()))
        bid = cur.lastrowid
        for i, (n, cid, _) in enumerate(members):
            c.execute("INSERT INTO batch_members(batch_id,pos,branch,content_id,squash_sha) VALUES(?,?,?,?,?)",
                      (bid, i + 1, n, cid, res.prefixes[i]))
            store.apply_event(c, n, Event.BATCH_START)
            store.emit(c, "batch_started", n, {"batch_id": bid, "pos": i + 1, "batch_size": len(members)},
                       commit_sha=members[i][2])
        self.batch = ActiveBatch(bid, main, members, list(res.prefixes),
                                 ctx={r["name"]: (r["approved_sha"], r["retest"]) for r in cand})

    def _batch_step(self):
        if self.batch is None:
            self._start_batch()
        b = self.batch
        if b is None:
            return
        n = len(b.members)
        v = judge(n, b.results, b.probes, self.cfg.points)
        if v.state == "done":
            self._finish_batch(v)
            return
        if v.state == "wait":
            need = [("point", p) for p in ladder(n, self.cfg.points)]
        else:
            need = [("probe", p) for p in v.probe]
        for kind, size in need:
            if (kind, size) in b.launched:
                continue
            if not self._launch(kind, "gate", b.prefixes[size - 1], self.cfg.batch_cores,
                                self.cfg.batch_timeout_s, size=size, batch_id=b.id, base_sha=b.base_sha):
                break
            b.launched.add((kind, size))

    def _on_point(self, j, res):
        b = self.batch
        if b is None or b.id != j.batch_id:
            return
        if res.status in ("infra", "cancelled"):
            self._infra_retry(f"batch:{j.kind}:{j.size}")
            b.launched.discard((j.kind, j.size))
            return
        base = self._known_red("gate", b.base_sha)
        if res.status == "timeout" and getattr(res, "overloaded", False) \
                and self._overload.get(f"b{j.size}", 0) < MAX_OVERLOAD_RETRY:
            self._overload[f"b{j.size}"] = self._overload.get(f"b{j.size}", 0) + 1
            store.add_info_event(self.con, "overload", None, f"批測點 {j.size} 逾時但系統超載，重跑")
            b.launched.discard((j.kind, j.size))
            return
        self._overload.pop(f"b{j.size}", None)
        self._store_items(j.run_id, res, base)
        red = self._red(res, base)       # 逾時＝這個測點紅，照一般流程找兇手
        nr = set(res.failed_items) - base
        if red and res.status == "fail" and self._timing_only(nr) and (b.id, j.kind, j.size) not in self._pretried:
            self._pretried.add((b.id, j.kind, j.size))        # 計時類新紅：同一個測點只重跑那幾項（單獨一個工作），綠就當抖動
            if self._launch("pretry", "gate", b.prefixes[j.size - 1], self.cfg.batch_cores, self.cfg.batch_timeout_s,
                            size=j.size, batch_id=b.id, base_sha=b.base_sha, only=",".join(sorted(nr))):
                self._pretry[max(self.jobs)] = (j.kind, j.size, nr)
                return
        target = b.results if j.kind == "point" else b.probes
        target[j.size] = RED if red else GREEN
        if res.status == "timeout":
            b.failed[j.size] = {"逾時"}
        else:
            b.failed[j.size] = set(res.failed_items) - base if red else set(res.failed_items)

    def _on_pretry(self, j, res):
        b = self.batch
        kind, size, nr = self._pretry.pop(j.run_id, (None, None, set()))
        if b is None or b.id != j.batch_id or kind is None:
            return
        if res.status in ("infra", "cancelled"):
            self._infra_retry(f"batch:{kind}:{size}")
            b.launched.discard((kind, size))              # 重跑沒跑成：整個測點重來（已記過重跑，不會再重跑一次）
            return
        base = self._known_red("gate", b.base_sha)
        still = set(res.failed_items) - base
        target = b.results if kind == "point" else b.probes
        if res.status == "pass" or (res.status == "fail" and not (still & nr)):
            store.add_info_event(self.con, "flaky_retry", None,
                                 f"批測點 {size} 的計時類項目第一次紅、單獨重跑綠，算抖動放過：" + ",".join(sorted(nr)))
            target[size] = GREEN
            b.failed[size] = set()
        else:
            self._store_items(j.run_id, res, base)
            target[size] = RED
            b.failed[size] = set(still) or {"逾時"}

    def _finish_batch(self, v):
        import contextlib
        with contextlib.ExitStack() as es:
            for n, (ap, rt) in self.batch.ctx.items():
                es.enter_context(store.event_ctx(n, ap, rt))
            self._finish_batch_inner(v)

    def _finish_batch_inner(self, v):
        b, c = self.batch, self.con
        m = v.merge_upto
        if m > 0:
            if not self._repull():
                return                                   # 推 main 前一定要重拉成功：拉不到就只測不推，等下一輪
            m2 = self._valid_prefix(b, m)
            if m2 == 0:
                self._abort_batch("放行被撤回／分支不在清單／內容變了：整批重排")
                return
            if m2 < m:
                log.info("推 main 前重拉清單：只推前 %d 條（原本 %d 條）", m2, m)
            m = m2
            if m > 0 and self._main_red() and any(self._blocked_by_main_red(store.get_branch(self.con, n) or {"name": n, "head_sha": h})
                                                  for n, _, h in b.members[:m]):
                self._abort_batch("測批途中發現 main 自己紅了：這批先不推（只有能修它的卡可以進）")
                return
        final = b.prefixes[m - 1] if m > 0 else None
        if m > 0:
            cur = self.repo.main_sha()
            expect = b.base_sha
            if cur != b.base_sha:
                files = self.repo.changed_files(cur, b.base_sha)
                if not self._is_docs(files):
                    self._abort_batch("批測途中 main 被動過")
                    return
                res = self.repo.stack(cur, [(n, h) for n, _, h in b.members[:m]])
                if len(res.kept) != m:
                    self._abort_batch("純文件更動後重疊失敗")
                    return
                final, expect = res.prefixes[-1], cur
            # 先記「要推了」再推：中途被砍時，復原能靠它判斷有沒有已經進 main
            c.execute("UPDATE batches SET status='pushing', merged_sha=?, merged_count=? WHERE id=?",
                      (final, m, b.id))
            try:
                self.repo.fast_forward_main(final, expect)
            except MainMoved:
                c.execute("UPDATE batches SET status='testing', merged_sha=NULL, merged_count=NULL WHERE id=?",
                          (b.id,))
                self._abort_batch("推 main 時 main 已動")
                return
            for n, _, _ in b.members[:m]:
                store.apply_event(c, n, Event.BATCH_MERGED, merged_sha=final)
        crow = store.get_branch(c, b.members[v.culprit - 1][0]) if v.culprit else None
        if crow is not None and self.source is not None and \
                b.ctx.get(crow["name"], (crow["approved_sha"], crow["retest"])) != (crow["approved_sha"], crow["retest"]):
            pass                                              # 兇手是照舊放行測出來的：不退回，下面會把它留回隊伍重測
        elif v.culprit:
            name = b.members[v.culprit - 1][0]
            row = store.get_branch(c, name)
            items = ",".join(sorted(b.failed.get(v.culprit, set())))
            fc = row["fail_count"] + 1
            if fc >= self.cfg.double_fail_halt:
                store.apply_event(c, name, Event.DOUBLE_FAIL, fail_count=fc, reject_reason=f"批測紅 {fc} 次：{items}")
                store.add_event(c, "halted", name, f"批測紅 {fc} 次，暫停")
            else:
                store.apply_event(c, name, Event.BATCH_CULPRIT, fail_count=fc, reject_reason=f"批測紅：{items}")
                store.add_event(c, "rejected", name, f"批測紅：{items}")
        for n, _, _ in b.members[m:]:                      # 沒合入又不是兇手的，回隊伍最前面
            r = store.get_branch(c, n)
            if r is not None and r["state"] == S.BATCHING.value:
                store.apply_event(c, n, Event.BATCH_DEFERRED)
        c.execute("UPDATE batches SET status=?, finished_at=?, merged_sha=?, merged_count=? WHERE id=?",
                  ("merged" if m else "aborted", self.clock(), final, m, b.id))
        self.batch = None
        if m > 0:
            self._after_merge([n for n, _, _ in b.members[:m]], final, b.failed.get(m, set()), b.base_sha)
