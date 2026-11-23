# 任務看板（board）

與專案無關的通用任務卡服務（認領、優先序、狀態），Cloudflare Worker + D1。這是選配元件：放在 `board/`，用 wrangler 單獨部署；Python 核心（`jet/`）仍只用標準庫，不依賴它，沒有它排隊器照常運作。專案自己的匯入腳本與使用說明放在使用它的專案裡，不放這裡。

## 卡片與分支一對一；狀態以排隊器為準
卡片自己只管前段與結局：`open` 空、`claimed` 已認領、`dropped` 放棄。中段（待自測、自測綠、排隊、批測、合入、退回、暫停）一律是上游排隊器的分支狀態，存在卡片的 `phase` 欄位當快取，不當真相。`GET /api/cards/<id>` 與 `GET /api/cards?live=1` 會經門牌向上游查 `/branches/<分支>` 並刷新快取（回傳多 `phase_live`、`approved`），查不到就用快取。事件（見「排隊器對接」）只更新快取與叫醒。分支被退回後，`PATCH /api/cards/<id> {"branch":"新分支"}` 換分支重做（phase 清空）。卡片欄位另有 `branch`、`owner_thread`（負責串 id）、`parent`。

## API
讀取要 `TOKEN` 或 `READ_TOKEN`，寫入要 `TOKEN`（`Authorization: Bearer …`，常數時間比對）；因為讀取會用看板自己的 `QUEUE_TOKEN` 代查排隊器，所以不開放匿名讀取。看板網頁本身是靜態的：打開後輸入一次讀取權杖，或直接開 `https://<你的網址>/?t=<讀取權杖>`，權杖會記在你的瀏覽器並從網址列移除，之後免再輸入。`READ_TOKEN` 只能讀（GET），不能寫，適合手機只看不改。
```sh
B=https://<你的網址>
curl -H "authorization: Bearer $READ_TOKEN" "$B/api/cards?status=open"       # 列卡（依優先序，數字小先）；加 live=1 向上游刷新
curl -X POST $B/api/cards  -H "authorization: Bearer $TOKEN" -d '{"id":"UI-01","title":"…","area":"ui","priority":10,"parent":null}'
curl -X POST $B/api/claim  -H "authorization: Bearer $TOKEN" -d '{"owner":"worker-1","owner_thread":"…","area":"ui","branch":"feature/ui-01"}'   # 依優先序搶第一張；也可帶 "id" 指定
curl -X PATCH $B/api/cards/UI-01 -H "authorization: Bearer $TOKEN" -d '{"status":"open"}'   # status 只能是 open／claimed／dropped；open 會釋放負責人與分支
curl -X POST $B/api/import -H "authorization: Bearer $TOKEN" -d '{"cards":[{"id":"…","title":"…"}]}'   # 已存在的 id 略過
```
認領是單句 `UPDATE … WHERE status='open'`，同一張卡只會有一條串成功，其他回 409。

## 門牌轉送（固定網址轉到會變動的上游）
- 登記：`curl -X PUT $B/relay/<名字> -H "authorization: Bearer $REGISTER_TOKEN" -d 'https://xxxx.trycloudflare.com'`（也可送 JSON：`-H 'content-type: application/json' -d '{"url":"https://…"}'`）
- 使用：`$B/relay/<名字>/<路徑>` 方法、查詢、內容、Authorization 原樣轉給上游，回應原樣傳回；上游的鑰匙由上游自己驗。
- 狀態：`GET $B/relay/<名字>/_status` → `{registered, updated_at}`（不洩漏上游網址）。沒登記或連不上回 503。
- 只接受 https、無帳密無埠號、網域後綴在 `RELAY_SUFFIXES`（預設 `trycloudflare.com`）內，避免變成開放代理。
- `REGISTER_TOKEN` 用 `wrangler secret put REGISTER_TOKEN` 設，不進 repo。

## 卡片與分支一對一；狀態以排隊器為準
卡片自己只管前段與結局：`open` 空、`claimed` 已認領、`dropped` 放棄。中段（待自測、自測綠、排隊、批測、合入、退回、暫停）一律是上游排隊器的分支狀態，存在卡片的 `phase` 欄位當快取，不當真相：`GET /api/cards/<id>` 與 `GET /api/cards?live=1` 會經門牌向上游查 `/branches/<分支>` 並刷新快取（回傳多 `phase_live`、`approved`），查不到就用快取。事件（下節）只用來更新快取與叫醒。分支被退回後，`PATCH /api/cards/<id> {"branch":"新分支"}` 換分支重做（phase 清空）。卡片欄位另有 `branch`、`owner_thread`（負責串 id）、`parent`。

## 排隊器對接
- `POST /hooks/queue`（`Authorization: Bearer $HOOK_TOKEN`）：body `{"events":[{"id":1,"kind":"tested_green","branch":"…","detail":"…"}]}`，多餘欄位忽略（可直接收 relativistic_jet notify 的原格式）。依 branch 對到卡更新 phase 快取（也可直接帶上游狀態名 `state`）；對不到卡照收；同 `source:id` 去重。kind→phase：`pushed→pending`、`tested_green→selftest_green`、`tested_red／rejected→rejected`、`queued`、`merged`；`conflict`／`halted`／`queue_error` 只記錄。
- 放行走看板：`POST /api/cards/<id>/approve {"commit":"<審過的 hash>","actor":"…"}`、`POST /api/cards/<id>/revoke`、`GET /api/cards/<id>/tests`（代查排隊器的分支狀態）。看板用自己的 `QUEUE_TOKEN` 經門牌（預設名 `queue`，設定 `QUEUE_RELAY` 可改）呼叫排隊器。
- 叫醒負責串：`tested_red`／`rejected`／`conflict`／`halted`／`queue_error` 進信箱，攢 `WAKE_BATCH_S`（預設 300）秒後依負責串各合成一則，POST 到 routine fire（`ROUTINE_ID` 設定、`ROUTINE_KEY` 機密），每小時最多 `WAKE_MAX_PER_HOUR`（預設 10）次，失敗保留重送；沒設 `ROUTINE_ID`／`ROUTINE_KEY` 就靜默略過。合入不叫醒。cron 每 2 分鐘檢查一次。
- 機密：`TOKEN`（寫入）、`READ_TOKEN`（只讀，選配）、`HOOK_TOKEN`、`QUEUE_TOKEN`、`REGISTER_TOKEN`、`ROUTINE_KEY`，全用 `wrangler secret put`。已部署的資料庫升級：依序各跑一次 `migrate_001.sql`、`migrate_002.sql`，再跑 `schema.sql`。

## 部署（只做一次，在你的電腦或 Cloudflare 網頁）
1. `npm i -g wrangler && wrangler login`
2. `wrangler d1 create task-board`，把印出的 database_id 填進 `wrangler.toml`
3. `wrangler d1 execute task-board --remote --file=schema.sql`
4. `wrangler secret put TOKEN`（自己想一串長亂碼；金鑰不要貼進聊天）。要用的功能再設其他機密：`READ_TOKEN`、`REGISTER_TOKEN`（門牌）、`HOOK_TOKEN`、`QUEUE_TOKEN`、`ROUTINE_KEY`
5. `wrangler deploy`，網址就是看板（iPhone 開得了）

測試：`npm test`（在 `board/` 下；Node 22，不需安裝套件）。
