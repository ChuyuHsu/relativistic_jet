"""純文件卡：直接合入後的保險（連續紅就暫停文件直通）。純搬移自 scheduler.py。"""
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
from .sched_common import (S, log, MAX_OVERLOAD_RETRY, UNKNOWN_GRACE_S, Running, ActiveBatch, docs_only_files)


class DocsMixin:
    def _docs_paused(self) -> Optional[dict]:
        raw = store.get_meta(self.con, "docs_direct_paused")
        try:
            return json.loads(raw) if raw else None
        except ValueError:
            return None

    def _direct_docs(self, r) -> bool:
        return r["docs_only"] == 1 and not self._docs_paused()

    def _note_docs_merge(self, name, sha, main_before):
        # 真的只改 *.md／docs/ 的合入不會讓測試變紅：不記進「疑似誤判」名單（JET-11）；分類說是純文件、實際卻改了別的檔才記
        try:
            files = self.repo.files_between(main_before, sha) if hasattr(self.repo, "files_between") else []
        except Exception:
            files = []
        if docs_only_files(files):
            return
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
