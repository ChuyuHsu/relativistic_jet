"""批測（gate_batch 模式）：選批、疊批、二分、收尾。純搬移自 scheduler.py。"""
import json
import logging
import os
import shutil
import subprocess
import threading
import time
from typing import Callable, Dict, List, Optional, Set, Tuple

from . import executor, store
from .batch import GREEN, RED, PENDING, ladder, judge
from .executor import Job
from .gitops import MainMoved
from .states import Event
from .sched_common import (S, log, MAX_OVERLOAD_RETRY, UNKNOWN_GRACE_S, Running, ActiveBatch)


class BatchMixin:
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
