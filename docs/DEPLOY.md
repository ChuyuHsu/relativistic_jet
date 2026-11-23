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
pkill -TERM -f "jet serve"                                   # 守護會用新版拉起來
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

## 8. 對外開放（選配）

排隊器預設只聽本機。要從外面看進度頁、用命令列操作，可以讓它自己開一條通道、把通道的公開網址登記到一個固定的轉送點。
核心不認識任何特定的通道服務或轉送服務：指令、抓網址的正規式、登記網址都在設定檔的 `expose` 區塊（沒設就什麼都不做）。

```json
"expose": {
  "tunnel_command": ["cloudflared", "tunnel", "--url", "http://127.0.0.1:8080"],
  "url_pattern": "https://[a-z0-9-]+\\.trycloudflare\\.com",
  "register_url": "https://relay.example.test/relay/queue",
  "register_token_file": "/path/to/relay-token",
  "reregister_seconds": 600
}
```

上面用 cloudflared 的臨時通道當例子，換成別的通道指令一樣用。行為：

- 排隊器啟動後由背景執行緒跑 `tunnel_command`，從輸出用 `url_pattern` 抓網址（有括號群組取第一組，沒有取整段，必須是 http／https）。
- 抓到就 `PUT register_url`，內容 `{"url": "<網址>"}`，標頭 `Authorization: Bearer <權杖>`；權杖每次登記時才從檔案讀。
- 通道行程死掉就重啟、拿新網址重新登記；每 `reregister_seconds` 秒（至少 10）重登記一次；登記失敗退避重試（5 秒起、倍增到 5 分鐘），第一次失敗與恢復各留一筆待辦事件。
- 排隊器結束時整個通道行程群組一起收掉。換 `expose` 設定要重啟才生效（熱重載會拒絕）。

對外時的安全：

- 所有資料（含進度頁用的）都要讀鑰匙，寫入要寫鑰匙，比對用常數時間；`/` 只回一個不含資料的頁面外殼。
- 請求內容上限 64 KB，連線 15 秒沒動就斷；回應裡本機的資料夾路徑與鑰匙字串一律遮蔽。
- 鑰匙檔權限 600；對外前先換掉預設產生的鑰匙以外的任何舊鑰匙。

## 8. 診斷（上線後看不到分支時）

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
