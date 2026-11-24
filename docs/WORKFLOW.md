# 工作流模式（workflow）

設定檔 `workflow` 區塊選擇合併流程。預設 `gate_batch`，行為與先前完全相同。

```json
"workflow": {
  "mode": "direct_merge",
  "loop": { "enabled": true, "cores": 0, "timeout_s": 0 }
}
```

| 模式 | 流程 |
|-|-|
| `gate_batch`（預設） | 放行＋自測綠 → 排隊 → 疊成一批 → 閘門批測（階梯＋探針）→ 沒有新紅就快轉合入 |
| `direct_merge` | 放行＋自測綠 → 逐張疊到最新主線、快轉合入（不批測）。背景 loop 持續對最新主線跑閘門層測試；出現新紅項時，從上次綠的主線到現在的提交二分（每步一次閘門測試）找出兇手，發 `loop_red`、`loop_culprit` 事件（含分支名），由通知管道送出 |

`loop.cores`、`loop.timeout_s` 為 0 時沿用批測的核數與逾時。loop 一次只跑一個工作，不與自測搶槽（自測照常最多 `max_selftest_slots`）。
`direct_merge` 時 `main_red` 守門仍然有效（主線紅時只放行能修它的卡）。換模式需要重啟排隊器。

## 背景 loop 找兇手：triage

`workflow.loop.triage`：

- `bisect`（預設）：背景測試出現新紅項就直接二分。
- `code_first`：先發 `loop_triage` 事件（`main_sha`、`green_sha`、`commits`［每個提交的 sha、分支、標題］、`failed_items`［項目＋失敗訊息原文］、`deadline_at`），等對方回報「嫌疑分支」；收到只驗證一次（嫌疑提交與它前一個提交各跑一次紅項），兩者符合（嫌疑紅、前一個綠）就直接發 `loop_culprit`，不符合才縮小範圍續二分；`triage_wait_s`（預設 900）內沒回報就改二分。

回報方式：清單回應（`/api/queue/desired`）的頂層物件加 `loop_suspect: {"main_sha": "<紅的那個 main>", "branch": "<嫌疑分支>"}`；`main_sha` 要對上 `loop_triage` 事件的 `main_sha`（前 12 碼相符），不符就忽略。

## 性能量測

設定 `commands.times_file`（相對工作目錄的 glob，TSV：項目、開始、結束，epoch 秒）後，背景 loop 每次測試會把逐項耗時記進 `item_times`、整體耗時／核數／CPU 使用率記進 `loop_runs`，並送一筆 `loop_perf` 事件（`perf`：`wall_s`、`cores`、`cpu_util`、`items`、`top`［最慢 15 項］）。`jet perf [--last N] [--top K] [--json]` 看最近 N 次與平均最慢的項目。

### 逐項畫面量測（commands.metrics_file）

設定 `commands.metrics_file`（相對工作目錄的 glob）後，jet 會讀測試輸出的 JSON，存進 `item_metrics`，並把摘要放進 `loop_perf` 事件的 `perf.metrics`：

```json
{"schema": 1, "items": {"<項目名>": {"secs": 12.3, "draw_calls_avg": 900, "draw_calls_max": 1500,
                                     "fps_avg": 28.5, "fps_min": 12, "objects": 3000, "mem_mb": 400}}}
```

欄位都可省略（缺的當沒量）。`perf.metrics`：`items`、`fps_avg`、`fps_min`、`draw_calls_avg`、`draw_calls_max`、`lowest_fps`（FPS 最低 10 項 `{item,value}`）、`most_draw_calls`（繪製數最高 10 項）。`jet perf` 也會列 FPS 最低的項目。
