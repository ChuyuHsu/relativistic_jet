"""direct_merge 工作流：直接合入、背景 loop 與二分找兇手、每日完整層、現況快照、main 自己紅。純搬移自 scheduler.py。"""
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


class LoopMixin:
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
