# CARD_FLOW 規則 → 測試對照

> 每條規則至少一個測試。測試都在 `tests/test_cardflow.py`（個別規則）與 `tests/test_sim_cards.py`（隨機模擬，守住「推 main 的整串都有效」等不變式）。
> 看板給的流程說明（`開發流程_看板與排隊器`）第 4、6、7 節的條目也併在表裡。

## 第 4 節：一串卡怎麼走到 main

| 規則 | 測試 |
| --- | --- |
| A、B、C 同一批照順序進 main | `ChainToMain.test_chain_merges_in_one_batch_in_order` |
| 紅的是 C：A、B 進 main，C 退回 | `ChainToMain.test_red_is_C_a_and_b_merge` |
| 紅的是 B：A 進、B 退回、C 留下不算紅不記次數 | `ChainToMain.test_red_is_B_a_merges_c_waits_not_red_no_count` |
| 紅的是 A：B、C 留下不算紅不記次數 | `ChainToMain.test_red_is_A_b_and_c_wait_not_red_no_count` |
| 自測要等父卡已合入或已在隊伍裡 | `ChainToMain.test_child_does_not_selftest_until_parent_queued_or_merged` |
| 父卡被還卡（清單上沒 approved_sha）時子卡照樣等 | `ChainToMain.test_parent_unapproved_returned_card_child_keeps_waiting` |
| 一串超過一批上限：前段先合、後段下一批 | `ChainToMain.test_chain_longer_than_batch_limit_front_first` |
| 父卡進 main 後子卡變普通卡、放行繼續有效 | `ChainToMain.test_parent_merged_child_becomes_plain_approval_kept` |
| 沒有卡的分支完全不理 | `ChainToMain.test_branch_without_card_is_ignored` |

## 第 6 節：每個狀態會遇到什麼事

| 規則 | 測試 |
| --- | --- |
| 沒放行不測（先審後測） | `StateEvents.test_no_selftest_without_approval` |
| 放行後才開始自測、合併 | `StateEvents.test_approval_arrives_then_selftest_and_merge` |
| 自測紅退回（附紅項） | `StateEvents.test_selftest_red_rejected` |
| 自測逾時退回 | `StateEvents.test_selftest_timeout_rejected` |
| 跟 main 撞檔退回 | `StateEvents.test_conflict_with_main_rejected` |
| 自測中撤回放行：停掉、回等待、不放行不測 | `StateEvents.test_revoke_during_selftest_stops_it_back_to_waiting` |
| 批測中撤回放行：不推 main、回等放行 | `StateEvents.test_revoke_while_batching_goes_back_to_waiting_not_merged` |
| 自己那一段又改了：放行失效、`superseded` 只發一次、事件帶舊放行版本與新提交 | `StateEvents.test_own_segment_changed_means_superseded_once_and_unapproved` |
| 單純 rebase：放行繼續、不發 `superseded` | `StateEvents.test_pure_rebase_keeps_approval_no_superseded_no_retest_needed` |
| 被退回後單純重新接主線：回自測、`pushed` 事件帶同一個 approved_sha | `StateEvents.test_rejected_then_pure_rebase_goes_back_with_same_approved_sha` |
| rebase 時動到自己那一段：算改，要重審 | `StateEvents.test_rebase_that_changed_own_part_needs_new_approval` |
| 重測（retest 變大）清掉退回、放行保留 | `StateEvents.test_retest_clears_rejected_and_keeps_approval` |
| 重測清掉暫停 | `StateEvents.test_retest_clears_halted` |
| retest 沒變大就不動 | `StateEvents.test_retest_counter_not_larger_does_nothing` |
| 已合併的卡不受清單影響 | `StateEvents.test_merged_and_dropped_ignored` |
| 父卡自測紅／被退回：子卡等，不算子卡紅 | `StateEvents.test_parent_selftest_red_child_waits_not_red` |
| 父卡改了內容、子卡重接且自己那一段沒變：放行續用、不發 `superseded` | `StateEvents.test_parent_rebased_child_own_part_same_keeps_approval` |
| 父卡內容變了：子卡回待自測 | `StateEvents.test_parent_content_changed_child_goes_back_to_waiting` |
| 父卡被放棄：`parent_blocked` 只發一次；改接 main 後放行保留 | `StateEvents.test_parent_dropped_child_blocked_event_once_then_reparent_to_main` |
| 清單上父卡被改：放行保留、從待自測重來 | `StateEvents.test_parent_changed_in_list_keeps_approval_restarts_from_pending` |

## 第 7 節：規則

| 規則 | 測試 |
| --- | --- |
| 1 先審後測（含兩種模式都能跑） | `StateEvents.test_no_selftest_without_approval`、`Rules.test_two_modes_both_run_parallel_mode_tests_before_approval`、`Rules.test_local_api_approval_without_source_in_review_first` |
| 2 放行跟著內容走（三種 rebase） | `test_pure_rebase_*`、`test_rebase_that_changed_own_part_*`、`StateEvents.test_rejected_then_pure_rebase_*` |
| 3 父卡沒進 main，子卡不會進 | `ChainToMain.test_red_is_A_*`、`Rules.test_repull_before_push_revoked_member_and_children_stay`、隨機模擬「父卡排在前面」 |
| 4 內容比較只比自己那一段（內容指紋相對父分支算；清單版本用已看過的提交對照） | `StateEvents.test_parent_rebased_child_own_part_same_keeps_approval`；`tests/test_gitops.py` 的指紋測試 |
| 5 父卡的錯不算子卡的 | `ChainToMain.test_red_is_B_*`、`test_red_is_A_*`、`StateEvents.test_parent_selftest_red_child_waits_not_red` |
| 6 放行的版本＝送審的版本（看板那邊）；排隊器只認清單的 approved_sha | `Rules.test_repull_before_push_content_changed_is_not_pushed` |
| 7 過期的結果不算數：事件帶 approved_sha、retest、遞增事件編號 | `Rules.test_events_carry_approved_sha_retest_and_increasing_ids`、`Rules.test_notify_payload_has_approved_sha_and_retest` |
| 8 推 main 前再確認：被撤的與子孫留下、推最大已測綠點 | `Rules.test_repull_before_push_revoked_member_and_children_stay` |
| 8 推 main 前再確認：中間的被撤，整批重排不推 | `Rules.test_repull_before_push_revoked_in_the_middle_whole_requeue_not_push` |
| 8 推 main 前再確認：放行版本內容不符 | `Rules.test_repull_before_push_content_changed_is_not_pushed` |
| 9 連不到看板：照舊清單測、清單太舊（預設 600 秒）不推 main | `Rules.test_stale_list_tests_but_does_not_push`、`Rules.test_fetch_failure_keeps_status_quo` |
| 10 父卡不能繞圈、不能指向不存在的卡 | `Rules.test_cycle_in_list_rejected_by_parser`、`ChainToMain`／`parent_blocked` 測試 |
| 空清單是真的 | `Rules.test_empty_list_is_real` |
| 格式錯只記錯誤、不動現況 | `Rules.test_bad_format_is_error_not_change`、`SourcePollerTest.test_parse_errors` |
| 純文件的卡放行後不測直接合 | `Rules.test_docs_only_card_merges_directly_after_approval` |
| 來源輪詢：權杖、非 200、保留上一份、同樣的錯只報一次 | `SourcePollerTest.*` |

兩條小規則：連續退回兩次暫停——`tests/test_scheduler.py` 的 `test_double_fail_halts`；重測只對排隊器退回的卡有效——看板那邊負責，排隊器看到 `retest` 變大就清掉退回與暫停。

放棄防呆（§11）：squash 進 main、兩個提交 squash 後 `git cherry` 會誤判、rebase 後內容相同、取代卡改了同一段（判未包含）、一邊刪檔一邊改檔、改名檔、二進位檔、分支不存在、取代分支不存在改看 main、超過 20 個檔——`tests/test_gitops.py` 的 `DropCheckTest.*`；清單格式、事件只發一次與結論變了才重發、git 失敗重試、notify 欄位——`tests/test_cardflow.py` 的 `DropCheckFlowTest.*`。
