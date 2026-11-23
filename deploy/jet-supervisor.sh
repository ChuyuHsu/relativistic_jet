#!/usr/bin/env bash
# 保活＋自動退版（DESIGN 第 9 節）。
# 目錄：$JET_HOME/releases/<版本>/（各版程式）、$JET_HOME/current → 現行版、$JET_HOME/previous → 上一版。
# 現行版在 WINDOW 秒內連續當掉 LIMIT 次，就把 current 切回 previous 再啟動，並留一筆 rollback 紀錄。
# 用法：JET_HOME=/srv/jet CONFIG=/srv/jet/jet.json deploy/jet-supervisor.sh
# 同一時間只准一份（flock）；crontab 可每分鐘叫一次，已在跑的會直接退出。
set -u
: "${JET_HOME:?要設 JET_HOME}"
: "${CONFIG:?要設 CONFIG}"
LIMIT="${LIMIT:-3}"
WINDOW="${WINDOW:-300}"
PY="${PYTHON:-python3}"
exec 9>"$JET_HOME/supervisor.lock"
flock -n 9 || exit 0
crashes=()
while true; do
  start=$(date +%s)
  (cd "$JET_HOME/current" && "$PY" -m jet serve --config "$CONFIG")
  rc=$?
  now=$(date +%s)
  [ "$rc" -eq 0 ] && exit 0                       # 正常停止（人下的 stop）
  kept=()
  for t in ${crashes[@]+"${crashes[@]}"}; do
    [ $((now - t)) -le "$WINDOW" ] && kept+=("$t")
  done
  crashes=(${kept[@]+"${kept[@]}"} "$now")
  echo "$(date -Is) 當掉 rc=$rc（$WINDOW 秒內第 ${#crashes[@]} 次）" >> "$JET_HOME/supervisor.log"
  if [ "${#crashes[@]}" -ge "$LIMIT" ]; then
    if [ -L "$JET_HOME/previous" ]; then
      ln -sfn "$(readlink -f "$JET_HOME/previous")" "$JET_HOME/current.new" && mv -T "$JET_HOME/current.new" "$JET_HOME/current"
      echo "$(date -Is) 自動退版到 $(readlink -f "$JET_HOME/current")" >> "$JET_HOME/supervisor.log"
      rm -f "$JET_HOME/previous"
    else
      echo "$(date -Is) 沒有上一版可退，停止" >> "$JET_HOME/supervisor.log"
      exit 1
    fi
    crashes=()
  fi
  sleep "${RESTART_DELAY:-2}"
done
