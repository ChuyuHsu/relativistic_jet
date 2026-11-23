# 架構與介面（骨架）

> 行為以 `DESIGN.md` 為準；這份只定「檔案放哪、資料長怎樣、模組怎麼接」。要改行為先改 DESIGN。
> 公開庫：不放任何專案名稱、路徑、核編號、金鑰、真實分支紀錄。測試一律用合成資料。

## 1. 目錄

```
jet/            核心套件（只用標準庫，Python ≥ 3.9）
  states.py     分支狀態機（純函式）            ← 已實作
  batch.py      測點階梯、判決、找兇手（純函式）  ← 已實作
  config.py     設定檔載入與檢查                ← 已實作
  db.py         SQLite 結構、遷移、備份          ← 已實作
  cores.py      核池（專屬核、不共用）            ← 已實作（含 foreign_busy、/proc 掃描）
  gitops.py     Git 介面（只定 Protocol）
  executor.py   執行器（行程群組、綁核、逾時殺群）  ← 已實作
  scheduler.py  主迴圈 tick 順序
  api.py        HTTP 路由表
  notify.py     通知（M1）
  cli.py        命令列
board/          選配的任務看板（Cloudflare Worker＋D1，JavaScript，單獨用 wrangler 部署；核心不依賴它，細節見 board/README.md）
examples/       合成範例專案（測試指令、分類指令、範例設定）
tests/          unittest，雲端容器可跑，不需要任何專案
docs/           DESIGN（設計）、ARCHITECTURE（本檔）
```

跑測試：`python3 -m unittest discover -s tests -t .`

## 2. 狀態機

事件與轉移全在 `jet/states.py`，對照 DESIGN 第 3 節。要點：

| 狀態 | 值 | 進入 | 離開 |
| --- | --- | --- | --- |
| 待自測 | pending | 新提交、人工恢復、父分支變動 | selftest_start |
| 自測中 | selftesting | 有空槽 | pass→自測綠／fail→退回 |
| 自測綠 | selftest_green | 自測過 | ready→排隊 |
| 排隊 | queued | 自測綠且放行（或設定不需放行） | batch_start、conflict_main→退回 |
| 批測中 | batching | 排進批 | merged／deferred（回排隊）／culprit（退回）／double_fail（暫停） |
| 已合入 | merged | 進 main | 終態，刪分支 |
| 退回 | rejected | 自測紅、是兇手、撞 main | 新提交→待自測 |
| 暫停 | halted | 批測紅兩次 | resume→待自測 |

放行不是狀態：是 `approvals` 表的一列（branch＋content_id），撤回只填 `revoked_at`。
`ready` 事件由 scheduler 在「自測綠且（有效放行或不需放行）」時發出。
內容指紋（content_id）沒變、只是重疊到新 main：放行、自測結果、`queued_at` 都保留。

堆疊：`branches.parent` 指向父分支。排隊條件多一條「父已合入或也在隊裡」，
組批時父一定排在子前面。父被退回或有新提交 → 子收到 `parent_changed` 回待自測。
父是兇手 → 子孫一起 `batch_deferred`，不算紅。父合入 → `parent` 清為 NULL。

## 3. SQLite（`jet/db.py` 的 SCHEMA 是唯一定義）

`meta`、`branches`、`approvals`、`batches`、`batch_members`、`runs`、`run_items`、
`baseline`、`events`、`audit`。只有排隊器主行程寫；HTTP 的寫入請求交給主行程執行，
並寫 `audit`。完整測試輸出存檔案，`runs.log_path`／`report_path` 只記路徑。
備份用 SQLite backup API，每小時一份；還原後重掃 Git，備份之後才放行的放行記號作廢。

## 4. 設定檔（JSON，`examples/jet.example.json`）

| 區段 | 欄位 |
| --- | --- |
| git | remote、main、branch_patterns |
| paths | state_dir、work_dir |
| cores | pool（如 `"0-7"`）、selftest_cores、batch_cores、reserve_for_selftest |
| limits | selftest_timeout_s、batch_timeout_s、batch_max、points、double_fail_halt、min_free_mem_mb、min_free_disk_mb |
| policy | require_approval |
| commands | test（必要）、classify（選配） |
| notify | url、batch_seconds、destinations（name、url、token_file、kinds、batch_seconds） |
| api | listen、read_key_file、write_key_file |
| expose（選配，預設關；換設定要重啟） | tunnel_command、url_pattern、register_url（只收 https，本機位址才可 http）、register_token_file、reregister_seconds |

載入前先 `config.parse` 檢查；壞的不載、沿用舊的（`safe_reload`）。金鑰只放檔案路徑，不放值。

## 5. 兩個專案指令

**測試指令** `commands.test`：argv 範本，可用佔位 `{layer}`（selftest｜gate｜full）、
`{cores}`（逗號分隔核表）、`{ncores}`、`{workdir}`、`{report}`、`{only}`。
工作目錄＝被測提交的 worktree。排隊器已用 `sched_setaffinity` 綁核，指令自己用 `{ncores}` 決定並行度。

- 退出碼：0 全過；1 有測試失敗（報告列出）；2 基礎設施錯誤（不算紅，重跑）。
- 報告（`{report}` 路徑，JSON）：
  `{"schema":1,"duration_s":12.3,"total":40,"items":[{"id":"項目名","status":"pass|fail|timeout|skip","summary":"≤500 字"}]}`
- 判紅：只看 `status=fail|timeout` 的項目，是否出現在 main 基準的紅項集合之外。

**分類指令** `commands.classify`（選配，M1）：範本只有 `{files}`（一個檔案的路徑，每行一個改動檔）。
stdout：`{"verdict":"docs_only"}` 或 `{"verdict":"test"}`。沒設定＝一律要測。
純文件分支：放行後直接快轉合入，不進批。

## 6. 模組介面

- **gitops.Repo**（見 `gitops.py`）：fetch / list_branches / main_sha / content_id / changed_files /
  conflicts_with / stack / worktree / fast_forward_main / delete_branch。
  對被測專案只准寫兩件事：`fast_forward_main`（帶 expect_main，不 force）與 `delete_branch`（帶 expect_sha）。
- **executor.Executor**：`start(Job)→handle`、`poll(handle)→JobResult|None`、`cancel(handle)`。
  一個工作一個行程群組，逾時／取消殺整群；開跑前 `cores.foreign_busy` 檢查核上沒有外人。
  `start` 開不了時丟例外而不是回空：`CoreBusy(cores)`（核已被我方別的工作或外面綁核的行程佔住）、
  `ResourceLow(reason)`（記憶體或磁碟低於水位）、`ExecutorError`（其他，例如指令不存在）。
  開子行程時 fork 與 exec 之間不跑 Python（不用 preexec_fn，排隊器主行程可以有 HTTP 等執行緒）：
  exec 小啟動器 `jet/_launch.py`，由它在新行程裡 `sched_setaffinity` 後 exec 指令（pid 不變）；
  綁核失敗 125、不能執行 126、找不到指令 127（執行器當 infra）。
  `Job.argv` 可覆蓋 `Executor(test_cmd=…)` 的指令範本；handle 有 `.pgid`。`live_pgids()`／`live_cores()`
  給排隊器查；`check_resources(path)` 讓排隊器派工前先問水位。
  結果：退出碼 0→pass（但報告有紅項改判 fail）、1→fail、其他（含 2、被訊號殺）→infra；逾時 timeout、取消 cancelled；
  `failed_items` 是報告裡 status=fail｜timeout 的項目 id。
  `foreign_busy` 的「外人」＝可用核是基準（預設取 1 號行程的可用核）的真子集、與這些核有交集，
  且不屬於我方行程群組或工作階段的行程；核心執行緒、殭屍、本行程不算。
- **cores.CorePool**：`acquire(n)`（不夠不部分給）、`release`、`free_count`。批測空閒時，
  排隊器把批測核借給自測；批測要用時只用空的核，不搶正在跑的自測。
- **batch.judge(n, results, probes)**：純函式，回 `Verdict(wait|probe|done, merge_upto, culprit, probe)`。
  scheduler 負責：派測點 → 收結果 → judge → 合入 `merge_upto` → 退回 culprit → 其餘 `batch_deferred`。
- **scheduler.Scheduler.tick()**：順序寫在 `scheduler.py` 檔頭。單執行緒，所有狀態寫入都在這裡。
- **api**：路由表見 `api.py`；讀寫鑰匙分開；預設只聽 127.0.0.1。
- **notify**：事件表 `events` 攢 batch_seconds 後一次 POST；合入不逐條通知。

## 7. 合入流程（scheduler 實作要點）

1. 組批：隊伍前 N 條（優先序→排隊時間；父在子前），`stack` 疊在 main 上；撞檔的留下一批。
2. 派 `ladder(n)` 的測點，每點一份專屬核、跑 gate 層。
3. 逐點收結果（infra 錯誤重跑）→ `judge`。需要 probe 就同時派 g+1..r-1 前綴。
4. `done`：`fast_forward_main(stack[merge_upto-1], expect_main)`；新基準＝該點結果；
   刪已合入分支；對仍在排隊的分支立刻檢查撞檔，撞的退回。
5. main 在批測途中被動：多出來全是純文件 → 重疊後直接合；否則整批作廢重來。
6. 逾時：殺整批、`batch_deferred` 全部、重排。

## 8. 建議怎麼拆（並行）

| 串 | 範圍 | 依賴 |
| --- | --- | --- |
| A Git | `gitops` 實作（patch-id、stack 壓合、worktree、fast-forward、刪分支）＋暫存 git repo 的測試 | 無 |
| B 執行器 | `executor`＋`cores.foreign_busy`（/proc 掃描）、行程群組、逾時殺群、假測試指令的測試 | 無 |
| C 調度核心 | `scheduler.tick`、自測派工、放行、排隊、堆疊、通知事件寫入 | A、B 的介面（先用假的） |
| D 批測 | 組批、四點測試、找兇手、合入後處理、純文件例外、基準 | A、B、C 的介面 |
| E 介面與維運 | `api`、`cli`、進度頁、備份／還原、設定重載、自動退版、`notify` | db（已有） |

A、B、E 可立刻並行；C、D 先對介面寫、用假的 Repo／Executor 測，A、B 完成後接起來。
