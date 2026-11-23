# 實跑示範

用範例專案（合成測試，不含任何真實專案）跑一次：啟動長駐 → 命令列放行 → 看進度頁 → 合入。
重現：`bash examples/demo.sh`（全程在暫存目錄）。b2 的測試檔在整合層才會紅，所以自測綠、批測被找出退回；b1、b3 合入並刪分支。

```text

$ # 1. 建一個範例專案：遠端 main＋三條分支（b2 在整合層會紅）

$ jet check-config jet.json
設定檔正確

$ jet init-keys
已產生讀、寫鑰匙（權限 600）。鑰匙不要放進版本庫。

$ jet serve &   # 長駐（背景）

$ jet status
狀態：運作中　排隊 0　待辦 0
各狀態：待自測 3
  跑著：#1 baseline/gate  自 11-23 04:12:32

$ # 2. 等自測跑完（沒放行，所以停在「自測綠」）

$ jet branches
分支  狀態   放行  父分支  優先  更新
b1  自測綠  -   -    0   11-23 04:12:33
b2  自測綠  -   -    0   11-23 04:12:33
b3  自測綠  -   -    0   11-23 04:12:33

$ # 3. 用命令列放行三條（放行要帶審過的提交）

$ jet approve b1 1cae7915870a
已放行 b1

$ jet approve b2 28ed8a9c9aef
已放行 b2

$ jet approve b3 fd2b54ef81f9
已放行 b3

$ # 4. 等批測：b2 被找出退回，b1、b3 合入

$ jet branches
分支  狀態   放行  父分支  優先  更新
b1  已合入  是   -    0   11-23 04:12:39
b2  退回   是   -    0   11-23 04:12:40
b3  已合入  是   -    0   11-23 04:12:41

$ jet branch b2
b2　退回　放行：是　父：-　優先：0
退回原因：批測紅：b2.txt
  跑 #3 selftest/selftest pass 11-23 04:12:32

$ jet merged
分支  提交          時間
b3  b7da52f2f6  11-23 04:12:41
b1  537d2611b3  11-23 04:12:39

$ jet todo
待辦事件 1　等人處理的分支 1
  事件 #1 rejected b2　批測紅：b2.txt
  b2：退回　批測紅：b2.txt

$ curl 進度頁（前 12 行）

$ 遠端現況（已合入的分支被刪，b2 還在）
28ed8a9c9aef984ab4e3248871790b390ba0303e	b2
b7da52f2f6eb047c5d81ac05e39108677dffc1f6	main

$ main 上的測試檔
tests/b1.txt
tests/b3.txt
tests/base.txt
```
