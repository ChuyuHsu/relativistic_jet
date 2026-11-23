# 任務看板（board）

與專案無關的通用任務卡服務（認領、優先序、放行、排隊器對接），Cloudflare Worker + D1。這是選配元件：放在 `board/`，用 wrangler 單獨部署；Python 核心（`jet/`）仍只用標準庫，不依賴它，沒有它排隊器照常運作。專案自己的匯入腳本與使用說明放在使用它的專案裡，不放這裡。

## 設計：看板不連排隊器，排隊器自己來拉
- 卡片與分支一對一。儲存的狀態只有 `open` 空、`claimed` 已認領、`dropped` 放棄；對外看的是由欄位算出來的 **`stage`**（不另存）：`new` 剛開卡 → `developing` 開發中 → `reviewing` 審查中 → `selftesting` 審查通過自測中 → `batching` 自測完畢批測中 → `merged` 已合併；`rejected` 退回；`dropped` 放棄。判斷：已放行且排隊器還沒回報或在待自測／自測中＝selftesting，自測綠／排隊／批測中＝batching；有審查退回原因、或排隊器 rejected／halted／撞檔＝rejected（`rejected` 欄位附來源 `review`／`queue`、`halted` 標記、原因與紅項）；已送審沒放行＝reviewing；其餘＝developing。`GET /api/cards?stage=reviewing,rejected` 可依階段過濾。
- **送審／退回／放行**：`POST /api/cards/<id>/submit {commit,actor}` 記下送審版本進審查中（退回後改好再送審，清掉退回、放行與舊結果，並叫醒 `REVIEW_THREADS`）；`POST /api/cards/<id>/reject {reason,actor}` 審查退回（原因存卡片、清放行、叫醒負責串）；放行的 commit 必須等於送審的 commit（前綴比對，不等回 409）。撤回放行回到審查中（保留送審版本）。
- **還卡**：`PATCH /api/cards/<id> {"status":"open"}`，回 `new`，清負責人、分支與所有送審放行結果。**放棄**：除已合併外任何階段都可（`status:"dropped"`）。
- **內容又改了**：排隊器送 `superseded` 事件（分支出現內容不同的新版本）→ 清掉送審與放行與舊結果，回 `developing`，叫醒負責串要重新送審。
- **卡住提醒**：cron 檢查，開發中或退回超過 `STUCK_HOURS`（預設 6）小時沒更新，通知 `ADMIN_THREADS`；有新動靜之前不重複提醒。
- **分支名由看板給**：認領時 `<BRANCH_PREFIX><卡號小寫>`（`BRANCH_PREFIX` 預設 `dev/`），呼叫端不能自己填。退回後重做用 `POST /api/cards/<id>/rebranch`，分支變 `<卡號>-r2`、`-r3`…，phase、最新結果、放行都清空。
- **放行就是卡片上的欄位**（`approved_commit`、`approved_by`、`approved_at`）：`POST /api/cards/<id>/approve {"commit":"<審過的 hash>","actor":"…"}`、`POST /api/cards/<id>/revoke`；必須先送審。看板不呼叫排隊器。
- **排隊器該管哪些分支與放行，由它定時來拉** `GET /api/queue/desired`（`Authorization: Bearer $HOOK_TOKEN` 或 `$READ_TOKEN`）：回所有「已認領、有分支、還沒合入」的卡，`[{"branch":"dev/p"},{"branch":"dev/k","parent":"dev/p","approved_sha":"abc1234"}]`；`parent` 是父卡（卡片的 `parent` 欄位）的分支，沒有就省略；`approved_sha` 只在已放行時有。這份清單就是排隊器該管的分支；放棄、釋放、已合入的卡不在內。出錯一律回非 200，不會回空清單。
- **結果由排隊器推回來**：`POST /hooks/queue`（`Authorization: Bearer $HOOK_TOKEN`），body `{"events":[{"id","kind","branch","commit","detail","from","to","failed_items"}]}`，可直接吃 relativistic_jet notify 的格式，多餘欄位忽略。依 branch 對到卡：`phase`（排隊器的分支狀態名）取事件的 `state`／`to`，沒有就依 kind（`pushed→pending`、`tested_green→selftest_green`、`tested_red／rejected→rejected`、`queued`、`merged`）；整理後的結果（kind、commit、detail、from、to、failed_items）存在卡片的 `last_result`。`approved`／`revoked` 事件不覆蓋最新結果。對不到卡照收；同 `source:id` 去重。`GET /api/cards/<id>` 與 `GET /api/cards/<id>/tests` 回的 `result` 就是存下來的最新結果。
- **叫醒負責串**：`tested_red`／`rejected`／`conflict`／`halted`／`queue_error`／`superseded`（以及審查退回、送審、卡住）進信箱，攢 `WAKE_BATCH_S`（預設 300）秒後依負責串各合成一則，POST 到 routine fire（`ROUTINE_ID` 設定、`ROUTINE_KEY` 機密），每小時最多 `WAKE_MAX_PER_HOUR`（預設 10）次，失敗保留重送；沒設 `ROUTINE_ID`／`ROUTINE_KEY` 就靜默略過。合入不叫醒。cron 每 2 分鐘檢查一次。

## API
讀取要 `TOKEN` 或 `READ_TOKEN`，寫入要 `TOKEN`（`Authorization: Bearer …`，常數時間比對）。看板網頁是靜態的：打開後輸入一次讀取權杖，或開 `https://<你的網址>/#t=<讀取權杖>`（`#` 後面不會送到伺服器），權杖記在瀏覽器並從網址列移除。`READ_TOKEN` 只能讀，適合手機只看不改。
```sh
B=https://<你的網址>
curl -H "authorization: Bearer $READ_TOKEN" "$B/api/cards?status=open"       # 列卡（依優先序，數字小先）
curl -X POST $B/api/cards  -H "authorization: Bearer $TOKEN" -d '{"id":"UI-01","title":"…","area":"ui","priority":10,"parent":null}'
curl -X POST $B/api/claim  -H "authorization: Bearer $TOKEN" -d '{"owner":"worker-1","owner_thread":"…","area":"ui"}'   # 依優先序搶第一張；也可帶 "id" 指定；回傳的 branch 就是分支名
curl -X PATCH $B/api/cards/UI-01 -H "authorization: Bearer $TOKEN" -d '{"status":"open"}'   # status 只能是 open／claimed／dropped；open 會釋放負責人、分支、放行與結果
curl -X POST $B/api/import -H "authorization: Bearer $TOKEN" -d '{"cards":[{"id":"…","title":"…"}]}'   # 已存在的 id 略過
```
認領是單句 `UPDATE … WHERE status='open'`，同一張卡只會有一條串成功，其他回 409。

## 部署（只做一次，在你的電腦或 Cloudflare 網頁）
1. `npm i -g wrangler && wrangler login`
2. `wrangler d1 create task-board`，把印出的 database_id 填進本機的 `wrangler.local.toml`（由 `wrangler.toml` 複製，不要提交）
3. `wrangler d1 execute task-board --remote --file=schema.sql`
4. 機密全用 `wrangler secret put`（自己想長亂碼，不要貼進聊天）：`TOKEN`（寫入）、`READ_TOKEN`（只讀，選配）、`HOOK_TOKEN`（排隊器用）、`ROUTINE_KEY`（叫醒用，選配）。設定（非機密）：`BRANCH_PREFIX`、`ROUTINE_ID`、`WAKE_BATCH_S`、`WAKE_MAX_PER_HOUR`、`REVIEW_THREADS`（送審時要叫醒的審查串 id，逗號分隔）、`ADMIN_THREADS`（卡住提醒對象）、`STUCK_HOURS`。串 id 屬於各專案私有設定，放本機的 `wrangler.local.toml`，不提交。
5. `wrangler deploy`，網址就是看板。

升級已部署的資料庫：依序各跑一次 `migrate_001.sql`～`migrate_004.sql`，再跑 `schema.sql`。測試：`cd board && npm test`（Node 22，不需安裝套件）。
