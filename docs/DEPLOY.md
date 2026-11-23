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
  supervisor.log       保活與退版紀錄
```

**狀態只在 `state/state.sqlite`。** `work/` 與 `state/runs/` 掉了不會影響正確性（`runs/` 只是舊輸出）。

## 3. 啟動

先檢查設定檔，再長駐：

```sh
python3 -m jet check-config $JET_HOME/jet.json
cd $JET_HOME/current && python3 -m jet serve --config $JET_HOME/jet.json
```

`serve` 同時跑調度主迴圈與 HTTP 查詢介面（預設只聽 127.0.0.1）。收到 SIGTERM 會停止派新工作並正常結束（退出碼 0）；
下次啟動時，上次沒做完的測試與批測一律作廢重來，已經推進 `main` 但來不及記帳的批會自動補記。

## 4. 保活與自動退版

`deploy/jet-supervisor.sh` 負責長駐：行程掛了就重啟；現行版在 5 分鐘內連續當掉 3 次，
就把 `current` 切回 `previous` 再啟動（寫進 `supervisor.log`），沒有上一版可退就停下。
同一時間只會有一份（`flock`），所以可以放心每分鐘叫一次。

crontab：

```cron
* * * * * JET_HOME=/srv/jet CONFIG=/srv/jet/jet.json /srv/jet/current/deploy/jet-supervisor.sh >/dev/null 2>&1
```

或 systemd（`Restart=` 交給 supervisor，所以單元只要保證它活著）：

```ini
[Unit]
Description=jet supervisor
After=network-online.target

[Service]
Environment=JET_HOME=/srv/jet CONFIG=/srv/jet/jet.json
ExecStart=/srv/jet/current/deploy/jet-supervisor.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

可調環境變數：`LIMIT`（連續當掉幾次退版，預設 3）、`WINDOW`（秒，預設 300）、`PYTHON`。

## 5. 換版與退版

排隊器的程式不經過它自己管的那條隊伍，所以壞了可以直接換。

換版：

```sh
cd $JET_HOME/releases && git clone <程式庫> v<新版>        # 或解壓
python3 -m jet check-config ...                              # 用新版檢查設定檔格式
ln -sfn "$(readlink -f ../current)" ../previous
ln -sfn "$PWD/v<新版>" ../current.new && mv -T ../current.new ../current
pkill -TERM -f "jet serve"                                    # supervisor 會用新版拉起來
```

退版：把 `current` 指回 `previous` 同樣用 `mv -T`，再 TERM 一次。資料庫有版本號：
新版程式可以升級舊資料庫，**舊版程式不會開比它新的資料庫**（會拒絕），所以升級過結構之後要退版，先還原升級前的備份。

換設定檔不用重啟：新設定先檢查格式，壞的不載、沿用舊的。

## 6. 備份與還原

排隊器每小時用 SQLite 備份 API 在 `state/backup/` 存一份。資料庫壞了：

1. 停掉排隊器。
2. 把最新的備份複製成 `state/state.sqlite`（刪掉同目錄的 `-wal`、`-shm`）。
3. 啟動。排隊器會重掃 Git 上的分支；備份之後才給的放行記號作廢、需重新確認（寧可重審，不可誤合）。

備份在同一台機器上，**不防整台機器壞**。要防的話，自己定期把 `state/backup/` 同步到別處。

## 7. 資源與安全

- 設定檔的 `cores.pool` 只放給排隊器用的核。每個工作綁自己專屬的核，不會共用；測試機上其他人的行程若占了池裡的核，派工前會被偵測到並避開。
- 記憶體或磁碟低於設定水位就暫停派新工作（`limits.min_free_mem_mb`、`min_free_disk_mb`）。
- 讀、寫用不同鑰匙；寫入都留在 `audit` 表。對外開放（改 `api.listen`）前先確認鑰匙檔權限，並放在反向代理或隧道後面。
- 排隊器對被測專案只做兩種寫入：快轉 `main`、刪已合入的分支。給它的 Git 憑證只需要這兩種權限。
