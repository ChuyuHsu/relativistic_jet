"""沒有可測的卡就立刻通知（JET-13）：自測佇列空、或剩下的卡都被擋（沒放行、父卡沒放行／被退回），整機閒置時
送一筆機器事件 idle_blocked，內容是「擋路清單」（擋住最多卡的最上游那一張排最前）。看板收到後叫醒該審的審查串、把擋路的卡排最前。
同一批擋路清單只在第一次發現時送一次，仍然閒置每 IDLE_REMIND_S 秒再提醒一次；有可測的卡或有在跑的自測就重置。"""
import json
from typing import Dict, List

from . import store
from .sched_common import S


class IdleMixin:
    IDLE_REMIND_S = 900           # 仍然沒人處理：每 15 分鐘再送一次
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
        return out[:self.IDLE_TOP]

    def _idle_step(self):
        if any(j.kind in ("selftest", "recheck") for j in self.jobs.values()):
            self._idle_reset()
            return
        pending = store.list_branches(self.con, S.PENDING.value)
        if not pending or any(self._testable(r) for r in pending):
            self._idle_reset()                     # 沒有待測的卡（全空）不算擋路；有可測的卡只是在等核
            if not pending:
                return
            return
        blockers = self._idle_blockers(pending)
        sig = json.dumps([[b["branch"], b["state"], b["approved"]] for b in blockers], ensure_ascii=False)
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
