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

from .sched_common import S, log
from .sched_common import (  # noqa: F401  （搬到共用模組；這裡保留名字，舊的 import 路徑照用）
    MAX_OVERLOAD_RETRY, UNKNOWN_GRACE_S, Running, _NoCtx, ActiveBatch)
from .sched_source import SourceMixin
from .sched_drop import DropMixin
from .sched_selftest import SelftestMixin
from .sched_loop import LoopMixin
from .sched_docs import DocsMixin
from .sched_batch import BatchMixin
from .sched_relative import RelativeMixin


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


class Scheduler(SourceMixin, DropMixin, SelftestMixin, LoopMixin, DocsMixin, BatchMixin, RelativeMixin):
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
        lost = c.execute("SELECT id, commit_sha, started_at FROM runs WHERE kind='nightly' AND status IN ('running','queued')"
                         + (" AND id NOT IN (%s)" % ",".join(str(r) for r in adopted) if adopted else "")).fetchall()
        if lost:                    # 每日完整層沒接管到（換版或停機時被殺）：當天重排，次數照 _on_nightly 的規則算
            n = int(store.get_meta(c, "nightly_infra", "0") or 0) + 1
            store.set_meta(c, "nightly_infra", str(n))
            if n < self.NIGHTLY_INFRA_TRIES:
                store.set_meta(c, "nightly_day", "")
                store.set_meta(c, "nightly_retry_at", "0")
            else:                   # 次數用完：送一筆紅項、計數歸零（今天不再排，明天照常）
                store.set_meta(c, "nightly_infra", "0")
                store.emit(c, "full_result", None, {
                    "main_sha": lost[-1]["commit_sha"], "started_at": lost[-1]["started_at"], "finished_at": self.clock(),
                    "total": 0, "red": [{"item": "完整層被取消", "summary": "完整層測試多次被取消（換版或停機），沒有完整結果"}],
                    "timeouts": []}, force=True)
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

    FETCH_RETRIES = 2          # fetch 失敗先原地重試幾次（偶發的連線限制、金鑰暫時驗不過），都失敗才記錯誤、發警示
    FETCH_RETRY_WAIT_S = 5

    def _fetch_retrying(self):
        """git fetch；失敗先隔幾秒原地重試 FETCH_RETRIES 次，最後一次的錯誤才往上丟。"""
        for i in range(self.FETCH_RETRIES + 1):
            try:
                self.repo.fetch()
                if i:
                    log.info("fetch 第 %d 次重試成功", i)
                return
            except Exception as e:
                if i >= self.FETCH_RETRIES:
                    raise
                log.warning("fetch 失敗（%s），%d 秒後重試", str(e)[:200], self.FETCH_RETRY_WAIT_S)
                time.sleep(self.FETCH_RETRY_WAIT_S)

    def _fetch_sync(self):
        t0 = time.time()
        self._put("fetch", {"state": "running", "started_at": t0})
        try:
            self._fetch_retrying()
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
            self._fetch_retrying()
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
        self._sync_squashed_tips()
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
            if res.status == "fail" or j.kind == "baseline":
                try:
                    res = self._apply_relative(rid, j.kind, res)        # 計時類項目改比相對值（policy.relative_ratio>0 才動）
                except Exception as e:
                    log.warning("相對判定失敗（照舊判定）：%r", e)
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

    SQUASHED_TIPS_MAX = 300

    def _sync_squashed_tips(self):
        """把最近已合入分支的舊尖端交給 git 層：疊卡的父卡 squash 進 main 後，子卡只重放「父卡舊尖端..子卡」那一段。"""
        if not hasattr(self.repo, "squashed_tips"):
            return
        rows = self.con.execute("SELECT head_sha FROM branches WHERE state=? ORDER BY updated_at DESC LIMIT ?",
                                (S.MERGED.value, self.SQUASHED_TIPS_MAX)).fetchall()
        self.repo.squashed_tips = frozenset(r[0] for r in rows if r[0])

    def _after_merge(self, names, new_main, failed: Optional[Set[str]], main_before: str):
        """合入後：基準、刪分支、子分支脫離、撞檔檢查。"""
        c = self.con
        self._sync_squashed_tips()
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
