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
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple

from . import store
from .batch import GREEN, RED, PENDING, ladder, judge
from .cores import CorePool, foreign_busy as _foreign_busy
from .executor import Job
from .gitops import MainMoved
from .states import Event, State

log = logging.getLogger("jet")
S = State


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
                 foreign_busy=_foreign_busy):
        self.cfg, self.con, self.repo, self.executor = cfg, con, repo, executor
        self.clock = clock
        self.resources_ok = resources_ok or (lambda: default_resources_ok(cfg))
        self.foreign_busy = foreign_busy
        self.pool = CorePool(cfg.pool)
        self.jobs: Dict[int, Running] = {}
        self.batch: Optional[ActiveBatch] = None
        self.main: str = ""
        self._bad: Optional[Set[int]] = None
        self._start_fail = 0
        self._infra_streak = 0
        self._stop = False
        self.recover()

    # ---------- 啟動復原 ----------
    def recover(self):
        """上次中斷留下的半成品：殺掉遺留測試行程、工作作廢、批作廢、狀態退回上一步。
        例外：已經推進 main 的批（status=pushing 而且終點已是 main 的祖先）照合入收尾。"""
        c = self.con
        self._kill_leftovers()
        cl = getattr(self.repo, "cleanup_worktrees", None)
        if cl:
            cl()
        c.execute("UPDATE runs SET status='cancelled', finished_at=? WHERE status IN ('running','queued')",
                  (self.clock(),))
        for b in c.execute("SELECT * FROM batches WHERE status='pushing'").fetchall():
            self._recover_pushing(b)
        c.execute("UPDATE batches SET status='aborted', finished_at=? WHERE status IN ('testing','probing','pushing')",
                  (self.clock(),))
        for r in store.list_branches(c, S.SELFTESTING.value):
            store.apply_event(c, r["name"], Event.RETRY)
        for r in store.list_branches(c, S.BATCHING.value):
            store.apply_event(c, r["name"], Event.BATCH_DEFERRED)

    def _kill_leftovers(self):
        import signal
        root = os.path.abspath(self.cfg.work_dir)
        for r in self.con.execute("SELECT pgid FROM runs WHERE status='running' AND pgid IS NOT NULL"):
            pg = r[0]
            try:
                # 保險：只殺工作目錄在我們 worktree 底下的群組（避免重開機後行程號被別人用走）
                if pg > 1 and os.readlink(f"/proc/{pg}/cwd").startswith(root):
                    os.killpg(pg, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass

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

    # ---------- 一輪 ----------
    def tick(self) -> None:
        self._bad = None
        self.repo.fetch()
        self.main = self.repo.main_sha()
        self._scan()
        self._sweep_parents()
        self._collect()
        self._reap_orphans()
        self._promote()
        if store.is_paused(self.con) or not self.resources_ok():
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

    # ---------- 掃 Git ----------
    def _scan(self):
        remote = self.repo.list_branches()
        for name, sha in remote.items():
            row = store.get_branch(self.con, name)
            if row is not None and row["state"] == S.MERGED.value and row["head_sha"] != sha:
                self.con.execute("DELETE FROM branches WHERE name=?", (name,))
                row = None
            if row is None:
                self._insert(name, sha)
            elif row["head_sha"] != sha:
                self._updated(row, sha)
        for row in store.list_branches(self.con):
            if row["name"] not in remote and row["state"] != S.MERGED.value:
                self._drop(row)

    def _base_for(self, row) -> str:
        if row is not None and row["parent"]:
            p = store.get_branch(self.con, row["parent"])
            if p is not None:
                return p["head_sha"]
        return self.main

    def _insert(self, name, sha):
        cid = self.repo.content_id(sha, self.main)
        docs = self._classify_branch(sha)
        self.con.execute(
            "INSERT INTO branches(name,head_sha,content_id,state,seen_at,docs_only,updated_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (name, sha, cid, S.PENDING.value, self.clock(), docs, self.clock()))

    def _updated(self, row, sha):
        name = row["name"]
        cid = self.repo.content_id(sha, self._base_for(row))
        same = cid == row["content_id"]
        st = row["state"]
        if same and st != S.REJECTED.value:
            self.con.execute("UPDATE branches SET head_sha=? WHERE name=?", (sha, name))
            return
        # 內容變了（或退回後重疊）：停掉在跑的工作，回待自測
        self._cancel_jobs(branch=name)
        if st == S.BATCHING.value:
            self._abort_batch("成員有新提交")
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
            self._abort_batch("成員消失")
        for ch in store.children(self.con, name):
            self.con.execute("UPDATE branches SET parent=NULL WHERE name=?", (ch["name"],))
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
                self.con.execute("UPDATE branches SET parent=NULL WHERE name=?", (r["name"],))
            elif p["state"] in (S.REJECTED.value, S.HALTED.value):
                self._parent_changed(r)

    # ---------- 分類 ----------
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
            out = subprocess.run(argv, capture_output=True, text=True, timeout=60)
            return out.returncode == 0 and json.loads(out.stdout).get("verdict") == "docs_only"
        except Exception:
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
                base_sha=None, head_sha=None) -> bool:
        cores = self._acquire(ncores)
        if not cores:
            return False
        ctx = self.repo.worktree(sha)
        wd = ctx.__enter__()
        cur = self.con.execute(
            "INSERT INTO runs(kind,branch,batch_id,size,layer,commit_sha,base_sha,status,cores,started_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (kind, branch, batch_id, size, layer, sha, base_sha, "running",
             ",".join(map(str, cores)), self.clock()))
        rid = cur.lastrowid
        d = os.path.join(self.cfg.state_dir, "runs", str(rid))
        os.makedirs(d, exist_ok=True)
        log_p, rep_p = os.path.join(d, "output.log"), os.path.join(d, "report.json")
        self.con.execute("UPDATE runs SET log_path=?, report_path=? WHERE id=?", (log_p, rep_p, rid))
        try:
            handle = self.executor.start(Job(layer=layer, workdir=wd, cores=cores, timeout_s=timeout,
                                             report_path=rep_p, log_path=log_p))
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
        self.jobs[rid] = Running(handle, ctx, cores, rid, kind, branch, head_sha or sha, size, batch_id)
        return True

    def _release_job(self, rid, status):
        j = self.jobs.pop(rid)
        try:
            j.ctx.__exit__(None, None, None)
        finally:
            self.pool.release(j.cores)
        self.con.execute("UPDATE runs SET status=?, finished_at=? WHERE id=?", (status, self.clock(), rid))
        return j

    def _cancel_jobs(self, branch=None, batch=False):
        for rid, j in list(self.jobs.items()):
            if (branch and j.branch == branch and j.kind == "selftest") or \
               (batch and j.kind in ("point", "probe")):
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
            if j.kind == "selftest":
                self._on_selftest(j, res)
            elif j.kind == "baseline":
                self._on_baseline(j, res)
            else:
                self._on_point(j, res)

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
            store.set_paused(self.con, True, "scheduler")
            store.add_event(self.con, "queue_error", None, f"{key} 連續基礎設施錯誤，排隊器已暫停")
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
                store.set_baseline(self.con, dst, layer, b)
        store.set_meta(self.con, "last_main", dst)

    def _set_baseline_all(self, sha, failed):
        for layer in ("gate", "selftest"):
            store.set_baseline(self.con, sha, layer, failed)
        store.set_meta(self.con, "last_main", sha)

    def _on_baseline(self, j, res):
        if res.status in ("infra", "timeout", "cancelled"):
            self._infra_retry("baseline")
            return
        self._set_baseline_all(j.head_sha, set(res.failed_items))

    # ---------- 自測 ----------
    def _chain(self, name) -> List:
        rows, r = [], store.get_branch(self.con, name)
        while r is not None and (not rows or r["state"] != S.MERGED.value):
            rows.append(r)
            r = store.get_branch(self.con, r["parent"]) if r["parent"] else None
        return rows[::-1]

    def _dispatch_selftests(self):
        c = self.con
        rows = [r for r in store.list_branches(c, S.PENDING.value) if r["docs_only"] != 1]
        rows.sort(key=lambda r: (-r["prio_override"], r["seen_at"], r["name"]))
        in_use = sum(len(j.cores) for j in self.jobs.values() if j.kind == "selftest")
        wanted = self.batch is not None or bool(store.list_branches(c, S.QUEUED.value))
        for r in rows:
            if wanted and in_use + self.cfg.selftest_cores > self.cfg.reserve_for_selftest:
                break
            if r["parent"]:
                p = store.get_branch(c, r["parent"])
                if p is not None and p["state"] in (S.REJECTED.value, S.HALTED.value):
                    continue
            chain = self._chain(r["name"])
            res = self.repo.stack(self.main, [(x["name"], x["head_sha"]) for x in chain])
            if len(res.kept) != len(chain):
                store.apply_event(c, r["name"], Event.CONFLICT_MAIN, reject_reason="疊不上 main（撞檔）")
                store.add_event(c, "conflict", r["name"], "自測前疊不上 main")
                continue
            if not self._launch("selftest", "selftest", res.prefixes[-1], self.cfg.selftest_cores,
                                self.cfg.selftest_timeout_s, branch=r["name"],
                                base_sha=self.main, head_sha=r["head_sha"]):
                break
            store.apply_event(c, r["name"], Event.SELFTEST_START)
            in_use += self.cfg.selftest_cores

    def _on_selftest(self, j, res):
        c = self.con
        row = store.get_branch(c, j.branch)
        if row is None or row["state"] != S.SELFTESTING.value:
            return
        if res.status in ("infra", "cancelled"):
            self._infra_retry("selftest:" + j.branch)
            store.apply_event(c, j.branch, Event.RETRY)
            return
        base = store.get_baseline(c, self.main, "selftest") or set()
        if self._red(res, base):
            why = "自測逾時" if res.status == "timeout" else \
                "自測紅：" + ",".join(sorted(set(res.failed_items) - base)) 
            store.apply_event(c, j.branch, Event.SELFTEST_FAIL, reject_reason=why)
            store.add_event(c, "rejected", j.branch, why)
        else:
            store.apply_event(c, j.branch, Event.SELFTEST_PASS)

    def _reap_orphans(self):
        """保險：狀態說在跑、其實沒有對應工作的，退回上一步（不讓分支卡死）。"""
        live = {j.branch for j in self.jobs.values() if j.kind == "selftest"}
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
                if self._approved(r) and self._parent_ok(r):
                    store.apply_event(c, r["name"], Event.READY,
                                      queued_at=r["queued_at"] if r["queued_at"] else self.clock())
                    changed = True
        for r in store.list_branches(c, S.QUEUED.value):
            if not self._approved(r):
                store.apply_event(c, r["name"], Event.REVOKE)
        for r in store.list_branches(c, S.PENDING.value):
            if r["docs_only"] == 1 and not r["parent"] and self._approved(r):
                self._merge_docs_only(r)

    def _merge_docs_only(self, r):
        c, main = self.con, self.repo.main_sha()
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

    # ---------- 批測 ----------
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
        rows = [r for r in store.list_branches(self.con, S.QUEUED.value) if r["docs_only"] != 1]
        rows.sort(key=lambda r: (-r["prio_override"], r["queued_at"] or 0, r["name"]))
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
        cand = self._select()
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
        self.batch = ActiveBatch(bid, main, members, list(res.prefixes))

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
        base = store.get_baseline(self.con, b.base_sha, "gate") or set()
        red = self._red(res, base)       # 逾時＝這個測點紅，照一般流程找兇手
        target = b.results if j.kind == "point" else b.probes
        target[j.size] = RED if red else GREEN
        if res.status == "timeout":
            b.failed[j.size] = {"逾時"}
        else:
            b.failed[j.size] = set(res.failed_items) - base if red else set(res.failed_items)

    def _finish_batch(self, v):
        b, c = self.batch, self.con
        m = v.merge_upto
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
        if v.culprit:
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
            for n, _, _ in b.members[v.culprit:]:
                store.apply_event(c, n, Event.BATCH_DEFERRED)
        c.execute("UPDATE batches SET status=?, finished_at=?, merged_sha=?, merged_count=? WHERE id=?",
                  ("merged" if m else "aborted", self.clock(), final, m, b.id))
        self.batch = None
        if m > 0:
            self._after_merge([n for n, _, _ in b.members[:m]], final, b.failed.get(m, set()), b.base_sha)
