"""放棄防呆：清單帶來的 drop_checks 逐筆查，只讀 git、只發 drop_checked 事件。純搬移自 scheduler.py。"""
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


class DropMixin:
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
