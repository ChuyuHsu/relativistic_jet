# 部署

通用說明，不含任何專案的路徑或設定。專案自己的設定檔放在專案端或部署機本機，不進本公開庫。

## 1. 需求

Linux、Python ≥ 3.9、git ≥ 2.38、能讀寫目標 Git 遠端（推 `main`、刪已合入分支）。
不需要安裝套件：`git clone` 之後直接跑。

## 2. 目錄配置（建議）

```
$JET_HOME/
  releases/<版本>/     各版程式（git clone 或解壓）
  current  → releases/<現行版>
  previous → releases/<上一版>
  jet.json             設定檔（金鑰檔路徑只寫路徑，不寫金鑰本身）
  keys/read  keys/write   讀、寫兩把鑰匙（權限 600）
  state/state.sqlite   唯一的狀態檔
  state/backup/        每小時本機備份
  state/runs/<id>/     每個測試工作的輸出（output.log、report.json）
  work/                Git 鏡像與暫時 worktree（可整個砍掉重建）
  supervisor.log       守護輸出（crontab 範例導向這裡）
```

**狀態只在 `state/state.sqlite`。** `work/` 與 `state/runs/` 掉了不會影響正確性（`runs/` 只是舊輸出）。

## 3. 啟動

```sh
python3 -m jet check-config $JET_HOME/jet.json      # 先檢查設定檔
python3 -m jet init-keys --config $JET_HOME/jet.json   # 第一次：產生讀、寫鑰匙檔
python3 -m jet supervise --root $JET_HOME --config $JET_HOME/jet.json   # 長駐（守護＋主行程）
```

`supervise` 守著主行程（`jet serve`：調度主迴圈加 HTTP 查詢介面，預設只聽 127.0.0.1）：
掛了就重啟；現行版在短時間內連續當掉 3 次，自動把 `current` 切回 `previous`，並通知。
乾淨結束（退出碼 0）守護就跟著結束。收到 SIGTERM 會停止派新工作並正常結束。
下次啟動時，上次沒做完的測試與批測一律作廢重來，已經推進 `main` 但來不及記帳的批會自動補記。

**啟停不需要 kill**：
- `jet start --root $JET_HOME --config …`：把守護叫起來（背景，輸出 `supervisor.log`，`supervisor.pid`）。
- `jet stop --root $JET_HOME [--now] [--wait 秒]`：排隊器自己收尾退出，守護跟著停。預設不再派新工作、等手上的批測與自測做完；`--now` 立刻殺掉測試行程群組再退出（下次啟動自動接續）。
- `jet restart [--now]`：同樣收尾後退出（退出碼 75），守護不算當掉，重新解析 `current` 並 exec 自己成新版，再拉起排隊器。所以「切 `current` → `jet restart`」就是完整換版。
- `jet stop` 也會放一個 `<root>/STOP` 檔：就算排隊器已經掛了，守護下次看到子行程結束也不會再拉起。

只想前景跑（除錯）：`python3 -m jet serve --config $JET_HOME/jet.json`。

## 4. 保活

守護本身要有人拉起來。crontab（`supervise` 同一時間只該有一份，建議用 `flock`）：

```cron
* * * * * flock -n /srv/jet/supervisor.lock python3 -m jet supervise --root /srv/jet --config /srv/jet/jet.json >>/srv/jet/supervisor.log 2>&1
```

systemd：

```ini
[Unit]
Description=jet
After=network-online.target

[Service]
WorkingDirectory=/srv/jet/current
ExecStart=/usr/bin/python3 -m jet supervise --root /srv/jet --config /srv/jet/jet.json
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## 5. 換版與退版

排隊器的程式不經過它自己管的那條隊伍，所以壞了可以直接換。

```sh
cd $JET_HOME/releases && git clone <程式庫> v<新版>        # 或解壓
python3 -m jet check-config $JET_HOME/jet.json              # 用新版檢查設定檔格式（在新版目錄下跑）
python3 -m jet deploy --root $JET_HOME v<新版>              # previous←現行、current←新版
python3 -m jet restart --config $JET_HOME/jet.json          # 排隊器自己收尾退出，守護（連守護自己）用新版重新拉起；不需要 kill
python3 -m jet rollback --root $JET_HOME                    # 手動退版（current 與 previous 對調）
```

資料庫有版本號：新版程式可以升級舊資料庫，**舊版程式不會開比它新的資料庫**（會拒絕），
所以升級過結構之後要退版，先還原升級前的備份。換設定檔不用重啟：新設定先檢查格式，壞的不載、沿用舊的。

## 6. 備份與還原

排隊器每小時用 SQLite 備份 API 在 `state/backup/` 存一份。資料庫壞了：

1. 停掉排隊器。
2. `python3 -m jet restore --config $JET_HOME/jet.json`（預設用最新備份；`--from` 指定；`list-backups` 列出）。
3. 啟動。排隊器會重掃 Git 上的分支；備份之後才給的放行記號作廢、需重新確認（寧可重審，不可誤合）。

備份在同一台機器上，**不防整台機器壞**。要防的話，自己定期把 `state/backup/` 同步到別處。

## 7. 資源與安全

- 設定檔的 `cores.pool` 只放給排隊器用的核。每個工作綁自己專屬的核，不會共用；測試機上其他人的行程若占了池裡的核，派工前會被偵測到並避開。
- 記憶體或磁碟低於設定水位就暫停派新工作（`limits.min_free_mem_mb`、`min_free_disk_mb`）。
- 讀、寫用不同鑰匙；寫入都留在 `audit` 表。對外開放（改 `api.listen`）前先確認鑰匙檔權限，並放在反向代理或隧道後面。
- 排隊器對被測專案只做兩種寫入：快轉 `main`、刪已合入的分支。給它的 Git 憑證只需要這兩種權限。

## 8. 外部來源與先審後測（選配）

```json
"git":    {"remote": "…", "scope": "tracked"},
"policy": {"order": "review_first", "require_approval": true},
"source": {"url": "https://board.example.test/api/queue/desired", "token_file": "/path/to/token",
           "poll_seconds": 15, "max_age_s": 600, "timeout_s": 10}
```

- 排隊器每 `poll_seconds` 秒用 `GET`（`Authorization: Bearer <權杖>`，權杖每次從檔案讀）拉清單：
  `[{"branch", "parent"?, "approved_sha"?, "retest"?}]`。清單就是唯一依據：有的登記、沒有的取消；`approved_sha` 等於分支現在的提交、或內容指紋相同才算放行。
- 拉不到、非 200、逾時、格式錯：沿用上一份成功的清單，記一筆 `source_error`（同樣的錯只記一次），不取消登記、不撤回放行。空陣列是真的「一條都不管」。
- 推 main 前會再拉一次；放行被撤、分支不在清單、內容變了的成員（和它們的子孫）留下，只推最大的已測綠點，推不出就整批重排。清單超過 `max_age_s`（預設 600 秒）沒更新成功就只測不推。
- `policy.order`：`review_first` 沒放行的版本不測；`parallel` 一推上來就測。`source` 只往外連，不需要對外開放任何埠。
- 沒設 `source` 時，本機的 `/track`、`/approve` 命令照常可用。
- 升級：換新版、設定檔加上面三塊、重啟。資料庫自動補欄位（`branches.approved_sha／retest`、`events.approved_sha／retest`、`shas` 表）。換 `source` 設定要重啟。
- 事件欄位（送到通知目的地）多了 `approved_sha`、`retest`；新增事件種類 `superseded`、`parent_blocked`（要在 `notify.destinations[].kinds` 裡選，或用 `"all"`）。
  `source_error` 只記在本機事件表（資訊型），不送通知。

## 8.1 對外時的安全

- 所有資料（含進度頁用的）都要讀鑰匙，寫入要寫鑰匙，比對用常數時間；`/` 只回一個不含資料的頁面外殼。
- 請求內容上限 64 KB，連線 15 秒沒動就斷；回應裡本機的資料夾路徑與鑰匙字串一律遮蔽。
- 鑰匙檔權限 600；對外前先換掉預設產生的鑰匙以外的任何舊鑰匙。

## 8.2 留存測試產物（逾時也查得到哪一項最久）

設定 `commands.artifacts`＝相對工作目錄的 glob 清單，例如 `["**/all.log", "reports/*.json"]`。每次測試結束（含逾時、被殺）在清掉工作目錄前，符合的檔案會複製到 `<state_dir>/runs/<run id>/artifacts/`（單檔超過 5 MB 只留最後 5 MB，最多 50 個，指到工作目錄外的連結不留）。

## 9. 診斷（上線後看不到分支時）

只用 jet 自己的指令與 git：

```sh
python3 -m jet doctor --config $JET_HOME/jet.json        # 設定、remote 連線、分支樣式比對到幾條、測試指令、核池、鑰匙
python3 -m jet status --verbose --config $JET_HOME/jet.json   # 心跳、fetch 進度、最近一次掃描、最近錯誤
git --git-dir $JET_HOME/work/mirror.git for-each-ref refs/remotes/origin | wc -l   # 鏡像抓到幾個分支
du -sh $JET_HOME/work/mirror.git; ls -la $JET_HOME/work/mirror.git/objects/pack   # 第一次 fetch 進行中會看到 tmp_pack_* 在長大
```

怎麼判讀：

- `status --verbose` 的「fetch：進行中，已 N 秒」＝第一次抓大 repo 還在跑，等；超過 `git.fetch_timeout_s`（預設 3600）會中止並記錯誤。
- 「fetch：失敗 …」＝看錯誤字串（ssh 金鑰、網路、遠端路徑）。`doctor` 的「remote 連線」會用同樣的方式連一次。
- 「掃描：…遠端 0 條」或 `doctor` 的「分支樣式 ✗」＝`git.branch_patterns` 沒對上遠端分支名（樣式比對的是去掉 `origin/` 的名稱，例如 `dev/foo`）。
- 「心跳」超過 60 秒沒動＝主迴圈卡住，看 supervisor 輸出；所有 git 呼叫都有逾時（`git.timeout_s` 預設 900 秒），卡住的 ssh 會整群被殺。
- 鏡像（`work/mirror.git`）可以隨時整個刪掉重建，不影響正確性，只是下次要重新抓一次。

## 9. 通知與 webhook

設定檔 `notify.destinations` 可以放多個目的地（舊的 `notify.url` 仍可用，等於一個叫 `default` 的目的地，只收要人處理的事件）：

```json
"notify": {
  "destinations": [
    {"name": "board", "url": "https://board.example.test/hooks/queue", "token_file": "/path/to/board-token",
     "kinds": "all", "batch_seconds": 5},
    {"name": "people", "url": "https://chat.example.test/in", "kinds": ["rejected", "conflict", "halted", "queue_error"]}
  ]
}
```

- `kinds`：要收的事件種類，或 `"all"`。要人處理的：`rejected`、`conflict`、`halted`、`queue_error`；
  狀態變化：`pushed`（有新提交）、`tested_green`、`tested_red`（自測）、`queued`（進佇列）、`merged`、`approved`、`revoked`。
  只有某個目的地要收狀態變化時，排隊器才會寫那類事件。
- `token_file`：有就帶 `Authorization: Bearer <權杖>`（每次送出才從檔案讀）；帶權杖的網址只收 https（本機位址才可 http）。
- `batch_seconds`：最舊的事件滿這麼多秒才送一批（給機器的可以設很短，例如 5）。
- 每個目的地各有進度，事件依 id 由小到大送；送失敗就保留、退避重送（60 秒起倍增到 30 分鐘），不丟、不亂序。
  至少送一次，每筆帶 `id`（單調遞增，對方依此去重）。新加的目的地只收從加入起的事件，不重送歷史。

送出的內容（`POST`，JSON，`schema` 目前是 1，之後只會加欄位）：

```json
{"schema": 1, "sent_at": 1700000000.0, "destination": "board", "count": 1,
 "text": "合併佇列有 1 件事要看：...",
 "events": [{"id": 42, "kind": "tested_red", "branch": "feat-x", "commit": "<提交 hash>",
             "detail": "<hash 前 8 碼>｜原因｜紅項：t1、t2", "at": 1700000000.0,
             "from": "selftesting", "to": "rejected",
             "failed_items": [{"item_id": "t1", "status": "fail", "summary": "..."}]}]}
```

- 必有欄位：`id`、`kind`、`branch`（系統事件為 null）、`detail`、`at`；`commit` 是事件發生時分支的提交。
- `detail` 是給人看的一行（提交前 8 碼、原因、紅項），只讀這幾個欄位的對方就夠用。
- `from`／`to` 只在狀態變化類事件；`failed_items` 只在 `tested_red`、`rejected`（最近一次紅的測試的紅項）。
- 對方要查更多：`GET /branches/<名字>`（含 `last_selftest`、`last_batch` 與各自的紅項）；放行 `POST /approve` 要帶 `sha`（審過的提交），撤回 `POST /revoke`。
