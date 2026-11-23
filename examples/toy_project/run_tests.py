#!/usr/bin/env python3
"""合成測試指令（範例專案）。用法：run_tests.py <layer> <ncores> <report路徑>
在目前目錄找 tests/*.txt：內容含 FAIL 的檔任何層都算紅；含 GATE-FAIL 的檔只有 gate／full 層才紅
（模擬「自測過、整合才紅」）。檔名當測試項目。
退出碼：0 全過、1 有紅（報告列出）、2 infra 錯誤。"""
import glob
import json
import os
import sys

layer, ncores, report = sys.argv[1], sys.argv[2], sys.argv[3]
items = []
for p in sorted(glob.glob("tests/*.txt")):
    with open(p, encoding="utf-8") as f:
        text = f.read()
    bad = ("FAIL" in text.replace("GATE-FAIL", "")) or ("GATE-FAIL" in text and layer != "selftest")
    items.append({"id": os.path.basename(p), "status": "fail" if bad else "pass",
                  "summary": "內容含 FAIL" if bad else ""})
with open(report, "w", encoding="utf-8") as f:
    json.dump({"schema": 1, "duration_s": 0.0, "total": len(items), "items": items}, f)
sys.exit(1 if any(i["status"] == "fail" for i in items) else 0)
