# 任務看板（board）

與專案無關的通用任務卡服務（認領、優先序、放行、排隊器對接），Cloudflare Worker + D1。這是選配元件：放在 `board/`，用 wrangler 單獨部署；Python 核心（`jet/`）仍只用標準庫，不依賴它，沒有它排隊器照常運作。專案自己的匯入腳本與使用說明放在使用它的專案裡，不放這裡。

## 設計：看板不連排隊器，排隊器自己來拉
完整的狀態流轉規格在 `docs/CARD_FLOW.md`（本元件與排隊器兩邊都以它為準）；這裡只列看板的實作要點。

- **卡片與分支一對一，分支名領卡時由看板給**：`<BRANCH_PREFIX><卡號小寫>`（`BRANCH_PREFIX` 預設 `dev/`），領卡後不變；還卡後重領拿到同一條。呼叫端不能自己填。
- **只存事實，狀態算出來**：儲存的只有 `status`（`open`／`claimed`／`merged`／`dropped`）、負責人、分支、父卡、送審版本、放行版本、審查退回原因、`retest` 計數，以及排隊器最近回報的 `phase`／`last_result`。對外看 **`stage`**：`new` 剛開卡、`developing` 開發中、`reviewing` 審查中、`selftesting` 審查通過自測中、`batching` 自測完畢批測中、`merged` 已合併、`rejected` 退回、`dropped` 放棄；`GET /api/cards?stage=reviewing,rejected` 可過濾。**列表預設只回進行中的卡**（`open`／`claimed`／`waiting` 與放棄待查），不含已合併與已放棄；要歷史帶 `?all=1`，或 `?stage=merged`、`?stage=dropped`、`?status=`。`GET /api/stats` 回各階段張數 `{stages:{…}, total}`（網頁頁首用）。退回的卡多一個 `rejected` 物件：`{source:"review"|"queue", halted, reason, failed_items}`。
- **動作**（轉移表以外一律 409 並回現在的 `stage`；同一動作重送結果一樣）：
  `POST /api/cards/<id>/submit {commit,actor}` 送審（`developing`／`reviewing`／`selftesting`／`batching`／`rejected` 可用；新版本會清放行、審查退回與排隊器結果，並叫醒 `REVIEW_THREADS`）·
  `approve {commit,actor}` 放行（只有 `reviewing`；版本必須等於送審的（前綴比對，≥7 碼）；放行的人不能是領卡的人）·
  `reject {reason,actor}` 審查退回（`reviewing`／`selftesting`／`batching`；清放行、叫醒負責串）·
  `revoke` 撤回放行（`selftesting`／`batching` 回 `reviewing`，保留送審版本）·
  `retest {actor}` 重測（只有被排隊器退回含暫停的卡；`retest` 加一、清排隊器結果、放行保留，回 `selftesting`；被審查退回的 409）·
  `PATCH /api/cards/<id> {"status":"open"}` 還卡（`developing`／`reviewing`／`rejected`；清負責人與送審放行結果，分支保留）·
  `{"status":"dropped"}` 放棄（除已合併外任何階段）。`merged`、`dropped` 是結局，任何動作都 409。
- **排隊器該管哪些分支與放行，由它定時來拉** `GET /api/queue/desired`（`Authorization: Bearer $HOOK_TOKEN` 或 `$READ_TOKEN`）：回所有「已領、未合併、未放棄」的卡 `[{"branch","parent"?,"approved_sha"?,"retest"}]`；`parent` 是父卡的分支（父卡已合併就省略），`approved_sha` 只在已放行時有。出錯一律回非 200，不會回空清單。
- **結果由排隊器推回來** `POST /hooks/queue`（`Authorization: Bearer $HOOK_TOKEN`），body `{"events":[{"id","kind","branch","commit","approved_sha","retest","detail","from","to","failed_items"}]}`。依 branch 對到卡後：**過期回報不算數**——卡片沒放行或已放棄，或事件的 `approved_sha`／`retest` 跟卡片現在的不符，一律丟掉（回 200，`stale` 計數，不改卡）；以事件編號去重。有效事件更新 `phase`（取 `state`／`to`，沒有就依 kind：`pushed→pending`、`tested_green→selftest_green`、`tested_red／rejected／conflict→rejected`、`queued`、`merged`、`halted→halted`）與 `last_result`（kind、commit、detail、from、to、failed_items；`GET /api/cards/<id>` 與 `/tests` 回它）。`superseded`（內容變了）清掉送審、放行與結果，回 `developing`。`parent_blocked`（父卡擋住）只註記。
- **父卡資訊**：`GET /api/cards` 與單卡讀取，有父卡的卡多帶 `parent_stage`（父卡現在的階段，不存在回 `missing`）、`waiting_parent`（父卡還沒合併＝子卡就算放行也要等）、`parent_blocked`（父卡被放棄或不存在，需改接）。父卡被放棄時，已領的子卡會通知管理者改它的父卡；改父卡用 `PATCH parent`（可改成別張卡或清空，放行保留；拒絕自己、不存在、已放棄、循環）。網頁看板的子卡會顯示父卡是誰、是否在等父卡。
- **通知**（攢 `WAKE_BATCH_S`，預設 120 秒，同一對象合成一則，每小時最多 `WAKE_MAX_PER_HOUR` 次，失敗保留重送；沒設 `ROUTINE_ID`／`ROUTINE_KEY` 就靜默略過）：送審→`REVIEW_THREADS`；審查退回、被排隊器退回（測試紅、撞檔）、內容變了→領卡的串；暫停、父卡擋住、排隊器自己出問題、卡住太久（開發中／審查中／退回超過 `STUCK_HOURS`，預設 6 小時，有新動靜前不重複）→`ADMIN_THREADS`。合併不通知。cron 每分鐘檢查一次。

## API
**角色不驗證**：所有串共用 `TOKEN`，`actor` 是自報的，「放行的人不能是領卡的人」只比對字串；要真正防冒用得每個角色一把權杖，目前不做。

讀取要 `TOKEN` 或 `READ_TOKEN`，寫入要 `TOKEN`（`Authorization: Bearer …`，常數時間比對）。看板網頁是靜態的：打開後輸入一次讀取權杖，或開 `https://<你的網址>/#t=<讀取權杖>`（`#` 後面不會送到伺服器），權杖記在瀏覽器並從網址列移除。`READ_TOKEN` 只能讀，適合手機只看不改。
```sh
B=https://<你的網址>
curl -H "authorization: Bearer $READ_TOKEN" "$B/api/cards?status=open"       # 列卡（依優先序，數字小先）
curl -X POST $B/api/cards  -H "authorization: Bearer $TOKEN" -d '{"id":"UI-01","title":"…","area":"ui","priority":10,"parent":null}'
curl -X POST $B/api/claim  -H "authorization: Bearer $TOKEN" -d '{"owner":"worker-1","owner_thread":"…","area":"ui"}'   # 依優先序搶第一張；也可帶 "id" 指定；回傳的 branch 就是分支名
curl -X PATCH $B/api/cards/UI-01 -H "authorization: Bearer $TOKEN" -d '{"status":"open"}'   # 還卡；{"status":"dropped"} 放棄
curl -X POST $B/api/import -H "authorization: Bearer $TOKEN" -d '{"cards":[{"id":"…","title":"…"}]}'   # 已存在的 id 略過
```
認領是單句 `UPDATE … WHERE status='open'`，同一張卡只會有一條串成功，其他回 409。

## 部署（只做一次，在你的電腦或 Cloudflare 網頁）
1. `npm i -g wrangler && wrangler login`
2. `wrangler d1 create task-board`，把印出的 database_id 填進本機的 `wrangler.local.toml`（由 `wrangler.toml` 複製，不要提交）
3. `wrangler d1 execute task-board --remote --file=schema.sql`
4. 機密全用 `wrangler secret put`（自己想長亂碼，不要貼進聊天）：`TOKEN`（寫入）、`READ_TOKEN`（只讀，選配）、`HOOK_TOKEN`（排隊器用）、`ROUTINE_KEY`（叫醒用，選配）。設定（非機密）：`BRANCH_PREFIX`、`ROUTINE_ID`、`WAKE_BATCH_S`、`WAKE_MAX_PER_HOUR`、`REVIEW_THREADS`（送審時要叫醒的審查串 id，逗號分隔）、`ADMIN_THREADS`（卡住提醒對象）、`STUCK_HOURS`。串 id 屬於各專案私有設定，放本機的 `wrangler.local.toml`，不提交。
5. `wrangler deploy`，網址就是看板。

升級已部署的資料庫：依序各跑一次 `migrate_001.sql`～`migrate_005.sql`，再跑 `schema.sql`。測試：`cd board && npm test`（Node 22，不需安裝套件）。

## 信箱（收信端自己來領）

看板不主動叫醒任何人（打外部排程任務的 routine fire 預設關閉，要開才設 `FIRE_ENABLED=1`；失敗原因看 `fire_log`）。通知產生後立刻進信箱，由收信端（例如傳令串）定時來領：

- `POST /api/outbox/claim {actor}`（TOKEN）：回 `{messages:[{to_thread, ids, kinds, card_ids, created_at, text}]}`，每個收件串一則、已合併；`text` 含哪張卡、發生什麼、原因與紅項、下一步。領過的標記時間，5 分鐘內不重給；沒有收件串的（沒填負責串）改給 `ADMIN_THREADS` 第一個。
- `POST /api/outbox/ack {actor, ids}`（TOKEN）：ids 用 claim 回傳的 `ids`（全部攤平）；ack 過的不再給，沒 ack 的 5 分鐘後會再給一次。

## /hooks/queue 的批次處理

一個請求最多處理 200 筆事件（超過的回 `remaining`，對方用 `processed` 之後的接著送）；回 `{ok, applied, unmatched, duplicate, stale, processed, remaining}`。處理方式：先用幾次查詢預載已見過的事件編號與牽涉的卡，在記憶體裡依序處理，寫入收成語句用 D1 batch 一次送出（每個事件的語句整組同進同出）。事件編號去重，整批重送不會重複寫；一批 50 筆約個位數次 D1 往返。

### 發信端限額（claim 參數與 release）

發信端有每小時訊息上限時，用這些參數避免洗版、被限額就不丟信：
- `POST /api/outbox/claim {actor, min_gap_s?, max_messages?}`：`min_gap_s`＝同一收件串兩則之間至少隔幾秒（剛發過的先攢著，下次一起發）；`max_messages`＝這次最多給幾個收件串。沒輪到的不是丟掉，領取標記會放掉，下一次 claim 立刻再給。
- 同一收件串、同一張卡的通知只送最新一則（舊的在 claim 時直接標成已送）；同一輪同一收件串本來就合成一則。
- `POST /api/outbox/release {actor, ids, hold_s?}`：發信失敗（例如被限額）時把領走的信放回去，不 ack；`hold_s` 秒內不再給（預設 0，最多 300）。
- 只給看板看的事件（開始自測、開始批測、tested_green 等）本來就不進信箱，只更新卡片階段。
