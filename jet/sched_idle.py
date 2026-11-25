"""沒有可測的卡就立刻通知（JET-13）：自測佇列空、或剩下的卡都被擋（沒放行、父卡沒放行／被退回），整機閒置時
送一筆機器事件 idle_blocked，內容是「擋路清單」（擋住最多卡的最上游那一張排最前）。看板收到後叫醒該審的審查串、把擋路的卡排最前。
去重鍵是「擋路卡的分支集合」（不含計數、放行與否；退回／暫停的卡加 ! 記號，因為要開發者動手）：同一批只在第一次發現時送一次，仍然閒置每 IDLE_REMIND_S 秒最多再提醒一次，
集合變了才立即送。有可測的卡或有在跑的自測時不重置去重鍵（機器一下忙一下閒不會洗版）；沒有待自測的卡才清掉。"""
import json
from typing import Dict, List

from . import store
from .sched_common import S


class IdleMixin:
    IDLE_REMIND_S = 3600          # 同一批擋路卡仍然沒人處理：每 60 分鐘最多再送一次（擋路卡集合變了才立即送）
    IDLE_TOP = 10                 # 擋路清單最多列幾張

    def _testable(self, r) -> bool:
        """這條 pending 分支現在能不能排自測（放行了、沒被父卡擋、祖先沒被退回）。"""
        if self.cfg.order == "review_first" and not self._approved(r):
            return False
        if r["name"] in getattr(self, "_blocked", {}) or self._parent_waiting(r):
            return False
        if r["parent"]:
            chain = self._chain(r["name"])[:-1]
            if any(x["state"] in (S.REJECTED.value, S.HALTED.value) for x in chain):
                return False
        return True

    def _root_blocker(self, r):
        """往上找最上游那張還沒合入的祖先（自己沒有父卡就是自己）。"""
        chain = self._chain(r["name"])
        return chain[0] if chain else r

    def _idle_blockers(self, pending) -> List[dict]:
        by_root: Dict[str, dict] = {}
        for r in pending:
            root = self._root_blocker(r)
            e = by_root.setdefault(root["name"], {"branch": root["name"], "state": root["state"],
                                                  "approved": bool(self._approved(root)), "descendants": 0,
                                                  "approved_descendants": 0})
            if r["name"] != root["name"]:
                e["descendants"] += 1
                if self._approved(r):
                    e["approved_descendants"] += 1
        out = sorted(by_root.values(), key=lambda e: (-e["approved_descendants"], -e["descendants"], e["approved"], e["branch"]))
        return out

    def _idle_step(self):
        if any(j.kind in ("selftest", "recheck") for j in self.jobs.values()):
            return                                 # 有自測在跑：不報，也不動去重鍵
        pending = store.list_branches(self.con, S.PENDING.value)
        if not pending or any(self._testable(r) for r in pending):
            if not pending:                        # 沒有待自測的卡：清掉去重鍵，之後再有擋路就是新的一批
                self._idle_reset()
            return                                 # 有可測的卡只是在等核：不算擋路，也不動去重鍵
        allb = self._idle_blockers(pending)
        # 退回／暫停的卡要開發者動手，狀態變成這種算新的一批（加 ! 記號）；其餘狀態與計數、放行與否的變動都不算
        sig = json.dumps(sorted(b["branch"] + ("!" if b["state"] in ("rejected", "halted") else "") for b in allb), ensure_ascii=False)   # 去重鍵用全部擋路卡（不是只看前 10 名），排名小變動不算新的一批
        blockers = allb[:self.IDLE_TOP]
        now = self.clock()
        last_sig, last_at = store.get_meta(self.con, "idle_sig"), float(store.get_meta(self.con, "idle_at", "0") or 0)
        if sig == last_sig and now - last_at < self.IDLE_REMIND_S:
            return
        store.set_meta(self.con, "idle_sig", sig)
        store.set_meta(self.con, "idle_at", str(now))
        waiting = [r for r in pending if self._approved(r)]
        store.emit(self.con, "idle_blocked", None,
                   {"at": now, "pending": len(pending), "approved_pending": len(waiting), "blockers": blockers,
                    "detail": f"開發機沒有可測的卡：待自測 {len(pending)} 條、其中已放行 {len(waiting)} 條，都被擋住或沒放行；擋路最多的："
                              + "、".join(f"{b['branch']}（{'已放行' if b['approved'] else '沒放行'}，{b['state']}，擋 {b['approved_descendants']} 張已放行子卡）" for b in blockers[:5])},
                   force=True)

    def _idle_reset(self):
        if store.get_meta(self.con, "idle_sig"):
            store.set_meta(self.con, "idle_sig", "")
