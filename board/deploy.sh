#!/usr/bin/env bash
# 一鍵部署看板（BD-08）：依序補跑還沒跑過的 migration → 部署 → 印出 Version ID → 打一次 /api/workflow 驗證。
#
#   bash deploy.sh                 # 正常部署（遠端資料庫）
#   bash deploy.sh --baseline 14   # 第一次用：資料庫已經手動跑到 migrate_014、還沒有 schema_version 時，指定它現在的版本
#   bash deploy.sh --dry-run       # 只印出會跑哪些 migration，不動資料庫、不部署
#
# 環境變數（都不進 repo）：
#   BOARD_URL     看板網址（驗證用；沒設就跳過驗證並明講）
#   READ_TOKEN    驗證用的讀取權杖（沒設就用 TOKEN）
#   WRANGLER      wrangler 指令（預設 "npx wrangler"；測試用假的取代）
#   DB_NAME       資料庫名稱（預設讀 wrangler.toml 的 database_name）
# 順序是「先 migration、再部署」：migration 都只加東西（加欄、加索引、加表），舊版程式碼在新 schema 上照常跑。
set -euo pipefail
cd "$(dirname "$0")"
WR=${WRANGLER:-npx wrangler}
DB=${DB_NAME:-$(sed -n 's/^database_name *= *"\(.*\)"/\1/p' wrangler.toml | head -1)}
BASELINE=""; DRY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --baseline) BASELINE="${2:?--baseline 要帶版本號}"; shift 2 ;;
    --dry-run) DRY=1; shift ;;
    *) echo "不認得的參數：$1" >&2; exit 2 ;;
  esac
done
[ -n "$DB" ] || { echo "找不到資料庫名稱（wrangler.toml 的 database_name 或 DB_NAME）" >&2; exit 2; }

# 目前 schema 版本（空字串＝沒有）；資料庫是否已有卡片表
q() { $WR d1 execute "$DB" --remote --json --command "$1" 2>/dev/null; }
first() { node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const r=JSON.parse(s)[0].results[0];process.stdout.write(r?String(Object.values(r)[0]):"")}catch(e){}})'; }
HAS_CARDS=$(q "SELECT name FROM sqlite_master WHERE type='table' AND name='cards'" | first)
CUR=""
if [ -n "$HAS_CARDS" ]; then CUR=$(q "SELECT value FROM meta WHERE key='schema_version'" | first || true); fi
LATEST=$(ls migrate_*.sql | sed 's/migrate_0*\([0-9]*\)\.sql/\1/' | sort -n | tail -1)

if [ -z "$HAS_CARDS" ]; then
  echo "資料庫是空的：跑 schema.sql（已含全部 migration），版本直接記為 $LATEST"
  if [ $DRY -eq 0 ]; then
    $WR d1 execute "$DB" --remote --file=schema.sql
    $WR d1 execute "$DB" --remote --command "INSERT INTO meta(key,value,updated_at) VALUES('schema_version','$LATEST',strftime('%s','now')) ON CONFLICT(key) DO UPDATE SET value=excluded.value"
  fi
  CUR=$LATEST
elif [ -z "$CUR" ]; then
  if [ -z "$BASELINE" ]; then
    echo "資料庫還沒有 schema_version。請確認它現在實際跑到第幾支 migration，再用 --baseline <N> 指定（例如手動跑到 migrate_014 就是 --baseline 14）。" >&2
    exit 3
  fi
  CUR=$BASELINE
  echo "以 --baseline $BASELINE 為起點"
fi
echo "資料庫版本 $CUR，最新 migration $LATEST"

for f in $(ls migrate_*.sql | sort); do
  n=$(echo "$f" | sed 's/migrate_0*\([0-9]*\)\.sql/\1/')
  [ "$n" -gt "$CUR" ] || continue
  echo "→ 跑 $f"
  if [ $DRY -eq 0 ]; then
    $WR d1 execute "$DB" --remote --file="$f"
    $WR d1 execute "$DB" --remote --command "INSERT INTO meta(key,value,updated_at) VALUES('schema_version','$n',strftime('%s','now')) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at"
  fi
  CUR=$n
done
[ $DRY -eq 1 ] && { echo "（dry-run：到此為止，沒有部署）"; exit 0; }

OUT=$($WR deploy 2>&1) || { echo "$OUT" >&2; echo "部署失敗" >&2; exit 1; }
echo "$OUT"
VID=$(echo "$OUT" | sed -n 's/.*Version ID: *\([0-9a-fA-F-]\{8,\}\).*/\1/p' | tail -1)
[ -n "$VID" ] && echo "Version ID: $VID" || echo "沒有在 wrangler 輸出找到 Version ID，請人工確認部署成功" >&2

if [ -n "${BOARD_URL:-}" ]; then
  CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "authorization: Bearer ${READ_TOKEN:-${TOKEN:-}}" "${BOARD_URL%/}/api/workflow")
  echo "驗證 /api/workflow：HTTP $CODE"
  [ "$CODE" = "200" ] || { echo "驗證失敗" >&2; exit 1; }
else
  echo "沒有設 BOARD_URL，跳過 /api/workflow 驗證" >&2
fi
echo "完成：schema 版本 $CUR${VID:+、Version ID $VID}"
