"""外部來源：定時拉清單、清單決定放行與父卡（CARD_FLOW 第 5、6、9 節）。純搬移自 scheduler.py，由 Scheduler 繼承。"""
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


class SourceMixin:
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
        if cid == row["content_id"]:
            return "yes"
        # 內容指紋不同，但可能只是 rebase／解衝突（上下文變了、加減的行沒變）或父卡 rebase 帶動：比「每個提交只看加減的行」的指紋，
        # 現在的頭相對 main 的每個提交都出現在放行版本裡（沒有新增、沒有改過）就沿用放行（JET-12）
        if self._same_patches(row, ap):
            return "yes"
        return "no"

    def _same_patches(self, row, ap) -> bool:
        if not hasattr(self.repo, "commit_ids"):
            return False
        key = (row["name"], row["head_sha"], ap, self.main)
        cache = getattr(self, "_same_cache", None)
        if cache is None or len(cache) > 500:
            cache = self._same_cache = {}
        if key not in cache:
            try:
                head_ids = self.repo.commit_ids(row["head_sha"], self.main)
                ap_ids = self.repo.commit_ids(ap, self.main)
                cache[key] = bool(head_ids) and head_ids <= ap_ids
            except Exception:
                cache[key] = False
        return cache[key]

    def _sha_in_main(self, sha) -> bool:
        """sha 是不是目前 main 的祖先（40 碼才算；同一個 main 只問一次 git）。"""
        if not sha or len(sha) != 40:
            return False
        cache = getattr(self, "_inmain_cache", None)
        if cache is None or cache[0] != self.main:
            cache = self._inmain_cache = (self.main, {})
        d = cache[1]
        if sha not in d:
            try:
                d[sha] = bool(self.repo.is_ancestor(sha, self.main))
            except Exception:
                d[sha] = False
        return d[sha]

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
            if ap and row["state"] != S.BATCHING.value and self._sha_in_main(ap):
                # 放行的提交本來就是 main 的祖先（別條路進了 main，例如純文件直接推）：標已合入，不再等測試、不報「不在快照」
                store.apply_event(c, name, Event.IN_MAIN, merged_sha=ap)
                continue
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
            if not self._approved(r) and r["name"] not in self._prerun:
                self._cancel_jobs(branch=r["name"])
                store.apply_event(self.con, r["name"], Event.RETRY)
