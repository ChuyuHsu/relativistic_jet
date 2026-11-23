#!/usr/bin/env python3
"""合成分類指令：用法 classify.py <檔案清單路徑>。全是 .md／docs/ 就算純文件。"""
import json
import sys

files = [l.strip() for l in open(sys.argv[1], encoding="utf-8") if l.strip()]
docs = bool(files) and all(f.endswith(".md") or f.startswith("docs/") for f in files)
print(json.dumps({"verdict": "docs_only" if docs else "test"}))
