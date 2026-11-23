# 部署

通用說明，不含任何專案的路徑或設定。專案自己的設定檔放在專案端或部署機本機，不進本公開庫。

## 1. 需求

Linux、Python ≥ 3.10、git ≥ 2.38、能讀寫目標 Git 遠端（推 `main`、刪已合入分支）。
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
