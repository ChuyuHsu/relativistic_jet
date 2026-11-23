#!/usr/bin/env bash
# 實跑示範：用範例專案（合成測試）啟動長駐 → 命令列放行 → 看進度頁 → 合入。
# 用法：bash examples/demo.sh        （全程在暫存目錄，不碰任何真實專案）
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
D="$(mktemp -d)"; cd "$D"
export GIT_AUTHOR_NAME=demo GIT_AUTHOR_EMAIL=d@x GIT_COMMITTER_NAME=demo GIT_COMMITTER_EMAIL=d@x
PORT="${PORT:-18080}"
say() { printf '\n$ %s\n' "$*"; }

say "# 1. 建一個範例專案：遠端 main＋三條分支（b2 在整合層會紅）"
git init -q --bare -b main remote.git && git clone -q remote.git work 2>/dev/null
( cd work && git checkout -q -b main && mkdir tests && echo ok > tests/base.txt \
  && git add -A && git commit -q -m base && git push -q origin main
  for b in b1 b2 b3; do
    git checkout -q -b $b main
    if [ $b = b2 ]; then echo GATE-FAIL > tests/$b.txt; else echo ok > tests/$b.txt; fi
    git add -A && git commit -q -m $b && git push -q origin $b && git checkout -q main
  done )

cat > jet.json <<JSON
{"git": {"remote": "$D/remote.git"},
 "paths": {"state_dir": "$D/state", "work_dir": "$D/jwork"},
 "cores": {"pool": "0-3", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 1},
 "limits": {"batch_max": 4, "points": [2, 4]},
 "policy": {"require_approval": true},
 "commands": {"test": ["python3", "$ROOT/examples/toy_project/run_tests.py", "{layer}", "{ncores}", "{report}"]},
 "api": {"listen": "127.0.0.1:$PORT", "read_key_file": "$D/keys/read", "write_key_file": "$D/keys/write"}}
JSON
export JET_CONFIG="$D/jet.json" PYTHONPATH="$ROOT"
say "jet check-config jet.json";  python3 -m jet check-config jet.json
say "jet init-keys";               python3 -m jet init-keys
say "jet serve &   # 長駐（背景）"
python3 -m jet serve >serve.log 2>&1 &
SERVE=$!
trap 'kill $SERVE 2>/dev/null || true' EXIT
for i in $(seq 1 50); do python3 -m jet status >/dev/null 2>&1 && break; sleep 0.2; done

say "jet status";   python3 -m jet status
say "# 2. 等自測跑完（沒放行，所以停在「自測綠」）"
sleep 6
say "jet branches"; python3 -m jet branches

say "# 3. 用命令列放行三條（放行要帶審過的提交）"
for b in b1 b2 b3; do
  sha=$(git -C work rev-parse --short=12 "origin/$b" 2>/dev/null || git -C work ls-remote origin "refs/heads/$b" | cut -c1-12)
  say "jet approve $b $sha"; python3 -m jet approve $b "$sha"
done
say "# 4. 等批測：b2 被找出退回，b1、b3 合入"
for i in $(seq 1 60); do
  [ "$(python3 -m jet branches --json | python3 -c 'import json,sys; print(sum(1 for b in json.load(sys.stdin)["branches"] if b["state"]=="merged"))' 2>/dev/null || echo 0)" = 2 ] && break
  sleep 1
done
say "jet branches"; python3 -m jet branches
say "jet branch b2"; python3 -m jet branch b2
say "jet merged";   python3 -m jet merged
say "jet todo";     python3 -m jet todo
say "curl 進度頁（前 12 行）"
curl -s "http://127.0.0.1:$PORT/" -H "X-Jet-Read-Key: $(cat keys/read)" | head -12
say "遠端現況（已合入的分支被刪，b2 還在）"
git -C work fetch -q origin && git -C work ls-remote --heads origin | sed 's#refs/heads/##'
say "main 上的測試檔"; git -C work ls-tree --name-only origin/main tests/
