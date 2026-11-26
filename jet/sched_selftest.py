"""自測派工與收結果、紅項複驗。純搬移自 scheduler.py。"""
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


class SelftestMixin:
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

    RECHECK_INFRA_MAX = 2        # 同一張卡在 main 上重跑連續幾次 infra 就不再重排、直接認定自測紅

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
                if getattr(res, "empty", None):
                    why = "壓合結果是空的（內容已在 main？不測也不合）"
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
                cnt = getattr(self, "_rc_infra", None)
                if cnt is None:
                    cnt = self._rc_infra = {}
                n = cnt.get(j.branch, 0) + (1 if res.status == "infra" else 0)
                cnt[j.branch] = n
                if n >= self.RECHECK_INFRA_MAX and rec.get("new_red"):
                    # 在 main 上重跑連續出基礎設施錯誤（例如這張卡新增的店／測項 main 還不認得、整個跑不起來）：
                    # 再重排只會一直重測一直 infra，不如認定紅是這張卡自己的（main 上確認不了＝不能拿 main 紅當理由放過）
                    cnt.pop(j.branch, None)
                    why = "自測紅：" + ",".join(sorted(rec["new_red"])) + "（在 main 上重跑確認連續失敗，測項在 main 跑不起來，視為這張卡自己的紅）"
                    store.apply_event(c, j.branch, Event.SELFTEST_FAIL, reject_reason=why)
                    store.add_event(c, "rejected", j.branch, why)
                    return
                self._infra_retry("recheck:" + j.branch)
                store.apply_event(c, j.branch, Event.RETRY)
                return
            getattr(self, "_rc_infra", {}).pop(j.branch, None)
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
