"""主迴圈（DESIGN 第 3、4 節）。骨架：只定一輪 tick 的順序。

一輪 tick（單執行緒，狀態只在這裡寫）：
 1. 讀設定（壞的沿用舊的）；資源水位不足就只收尾不派新工。
 2. 掃 Git：新分支／新提交 → PUSHED；消失的分支清掉。
 3. 收完成的工作：自測結果 → 綠／紅；批測點結果 → batch.judge。
 4. 放行＋自測綠 → READY 排隊。
 5. 若沒有批在跑：排隊順序取前 N 條組批、派四個測點。
 6. 有空槽就派自測（優先序，再先推先測）；批測空閒時核借給自測。
 7. 通知事件攢著、時間到一次打出去；每小時備份。
"""


class Scheduler:
    def __init__(self, cfg, con, repo, executor):
        self.cfg, self.con, self.repo, self.executor = cfg, con, repo, executor

    def tick(self) -> None: raise NotImplementedError
    def run_forever(self) -> None: raise NotImplementedError
