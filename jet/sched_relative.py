"""計時類項目改比相對值（JET-11）。純新增模組，用 policy.relative_* 設定；relative_ratio=0（預設）時完全不動。

負載會讓載入時間這類項目忽快忽慢，絕對門檻（例如 4000 ms）在機器忙的時候天天紅。這裡改成：
一個計時類項目（名稱符合 policy.timing_items）紅了、而且摘要能抽出數字時，跟「最近幾輪 main 基準的中位數」比——
超過 relative_ratio 倍、而且至少多 relative_min_extra 才算紅；沒超過就當它沒紅（從這次的紅項裡拿掉）。
基準數字只從 main 的背景測試（baseline）累積；沒有基準數字的項目照舊（絕對門檻判紅）。
"""
import json
import re
import statistics
from typing import Dict, List, Optional

from . import store
from .sched_common import log


def _read_items(report_path) -> List[dict]:
    try:
        with open(report_path, encoding="utf-8") as f:
            return [it for it in json.load(f)["items"] if isinstance(it, dict) and "id" in it]
    except (OSError, ValueError, KeyError, TypeError):
        return []


class RelativeMixin:
    def _rel_hist(self, item) -> List[float]:
        raw = store.get_meta(self.con, f"rel_hist:{item}")
        try:
            return [float(x) for x in json.loads(raw)] if raw else []
        except (ValueError, TypeError):
            return []

    def _apply_relative(self, rid, kind, res):
        """res.failed_items 裡的計時類紅項，數字沒超過相對門檻的拿掉；全拿光且是紅就改成綠。baseline 跑完把數字記進基準。"""
        cfg = self.cfg
        if not cfg.relative_ratio or not cfg.timing_items:
            return res
        rr = self.con.execute("SELECT report_path FROM runs WHERE id=?", (rid,)).fetchone()
        if not rr or not rr["report_path"]:
            return res
        try:
            vre = re.compile(cfg.relative_value_re)
        except re.error:
            return res
        vals: Dict[str, float] = {}
        for it in _read_items(rr["report_path"]):
            iid = str(it["id"])
            if not any(re.search(p, iid) for p in cfg.timing_items):
                continue
            m = vre.search(str(it.get("summary") or ""))
            if m:
                try:
                    vals[iid] = float(m.group(1))
                except ValueError:
                    pass
        if not vals:
            return res
        kept = []
        for it in res.failed_items:
            v, hist = vals.get(it), self._rel_hist(it)
            if v is not None and hist:
                base = statistics.median(hist)
                if not (v > cfg.relative_ratio * base and v - base >= cfg.relative_min_extra):
                    log.info("相對判定：%s 這次 %.0f、基準中位數 %.0f（門檻 %.2f 倍＋%g），不算紅", it, v, base, cfg.relative_ratio, cfg.relative_min_extra)
                    continue
            kept.append(it)
        if kind == "baseline":
            for it, v in vals.items():
                hist = (self._rel_hist(it) + [v])[-cfg.relative_keep:]
                store.set_meta(self.con, f"rel_hist:{it}", json.dumps(hist))
        if len(kept) != len(res.failed_items):
            res.failed_items = kept
            if not kept and res.status == "fail":
                res.status = "pass"
        return res
