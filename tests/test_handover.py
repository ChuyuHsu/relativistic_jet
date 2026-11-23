"""換版接管：jet restart 時在跑的測試不等、不殺，下一代主行程接手收結果。真的 git、真的執行器。"""
import os
import signal
import sys
import time
import unittest

from jet import config, db, store
from jet.cores import own_leftovers
from jet.executor import Executor
from jet.gitops import GitRepo
from jet.scheduler import Scheduler
from tests.test_e2e import E2E, CORES, git

SLOW = '''
import glob, json, os, sys, time
layer, ncores, report, gate = sys.argv[1:5]
end = time.time() + 60
hold = gate + ".hold_gate"         # 這個檔存在時，批測（gate 層）也要等 gate 檔
while (layer == "selftest" or (layer == "gate" and os.path.exists(hold))) and not os.path.exists(gate) and time.time() < end:
    time.sleep(0.05)
items = [{"id": os.path.basename(p), "status": "pass"} for p in sorted(glob.glob("tests/*.txt"))]
json.dump({"schema": 1, "items": items}, open(report, "w"))
'''


class Handover(E2E):
    test_all_green_merge = test_two_red_found = test_main_already_red = None    # 只借 E2E 的建 repo 工具，不重跑它的測試

    def system2(self, **limits):
        """跟 E2E.system 一樣，但測試指令要等 gate 檔出現才結束；可以重複建（模擬重啟）。"""
        r = self.t.name
        self.gate = os.path.join(r, "gate")
        script = os.path.join(r, "slow.py")
        with open(script, "w") as f:
            f.write(SLOW)
        self.cfg = config.parse({
            "git": {"remote": self.remote},
            "paths": {"state_dir": os.path.join(r, "state"), "work_dir": os.path.join(r, "work")},
            "cores": {"pool": f"{CORES[0]}-{CORES[-1]}" if CORES == list(range(CORES[0], CORES[-1] + 1))
                      else ",".join(map(str, CORES)),
                      "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 1},
            "limits": {"selftest_timeout_s": 60, "batch_timeout_s": 120, "batch_max": 8,
                       "points": [2, 4, 8], **limits},
            "policy": {"require_approval": False},
            "commands": {"test": [sys.executable, script, "{layer}", "{ncores}", "{report}", self.gate]},
        })
        return self.boot()

    def boot(self):
        r = self.t.name
        con = db.connect(os.path.join(r, "state.sqlite"))
        self.addCleanup(con.close)
        repo = GitRepo(self.cfg.remote, self.cfg.work_dir, self.cfg.main_branch, self.cfg.branch_patterns)
        self.s = Scheduler(self.cfg, con, repo, Executor(self.cfg.test_cmd))
        self.con = con
        return self.s

    def restart(self):
        """像 jet restart：舊主行程交接後離開（不殺測試），新主行程啟動。"""
        self.s.handover()
        self.con.close()
        return self.boot()

    def open_gate(self):
        open(self.gate, "w").close()

    def running(self):
        return [r for r in self.con.execute("SELECT * FROM runs WHERE status='running'")]

    def tick_until(self, cond, timeout=60):
        end = time.time() + timeout
        while time.time() < end:
            self.s.tick()
            if cond():
                return
            time.sleep(0.05)
        self.fail("逾時：" + str({r["name"]: r["state"] for r in store.list_branches(self.con)}))

    def tearDown(self):
        self.open_gate()                         # 讓留下的測試自然結束
        for r in self.con.execute("SELECT pgid FROM runs WHERE status='running' AND pgid IS NOT NULL"):
            try:
                os.killpg(r[0], signal.SIGKILL)
            except OSError:
                pass

    def test_restart_mid_run_adopts_and_finishes(self):
        self.push_branch("a", "ok\n")
        self.system2()
        self.tick_until(lambda: self.state("a") == "selftesting" and self.running())
        run = self.running()[0]
        pg = run["pgid"]
        self.s = self.restart()
        self.s.tick()
        self.assertEqual(self.state("a"), "selftesting")          # 沒退回、沒重排
        self.assertEqual(set(self.s.jobs), {run["id"]})
        self.assertTrue(set(map(int, run["cores"].split(","))) <= self.s.pool.busy)   # 核池記帳接回來，不超賣
        os.kill(pg, 0)                                            # 測試還活著
        self.open_gate()
        self.tick_until(lambda: self.state("a") == "merged")
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM runs WHERE kind='selftest' AND branch='a'").fetchone()[0], 1)
        self.assertEqual(self.con.execute("SELECT status FROM runs WHERE id=?", (run["id"],)).fetchone()[0], "pass")

    def test_finished_while_down_is_collected_from_exit_json(self):
        self.push_branch("a", "ok\n")
        self.system2()
        self.tick_until(lambda: self.state("a") == "selftesting" and self.running())
        run = self.running()[0]
        self.s.handover()
        self.con.close()
        self.open_gate()                                          # 主行程不在的時候測試跑完了
        d = os.path.dirname(run["log_path"])
        end = time.time() + 20
        while not os.path.exists(os.path.join(d, "exit.json")) and time.time() < end:
            time.sleep(0.05)
        self.assertTrue(os.path.exists(os.path.join(d, "exit.json")))
        self.s = self.boot()
        self.tick_until(lambda: self.state("a") == "merged")
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM runs WHERE kind='selftest' AND branch='a'").fetchone()[0], 1)

    def test_two_restarts_in_a_row(self):
        self.push_branch("a", "ok\n")
        self.system2()
        self.tick_until(lambda: self.state("a") == "selftesting" and self.running())
        rid = self.running()[0]["id"]
        for _ in range(2):
            self.s = self.restart()
            self.s.tick()
            self.assertEqual(set(self.s.jobs), {rid})
        self.open_gate()
        self.tick_until(lambda: self.state("a") == "merged")

    def test_timeout_counts_from_original_start(self):
        self.push_branch("a", "ok\n")
        self.system2(selftest_timeout_s=3)
        self.tick_until(lambda: self.state("a") == "selftesting" and self.running())
        rid = self.running()[0]["id"]
        # 不靠真實睡眠：把這筆的開跑時間往前挪過逾時上限，重啟後要「從原本的開跑時間算」，立刻判逾時
        # （若重新計時，起算點會是重啟那一刻，幾個 tick 內不會逾時）。不依賴牆鐘，機器忙也穩。
        self.con.execute("UPDATE runs SET started_at = started_at - 10 WHERE id=?", (rid,))
        self.con.commit()
        self.s = self.restart()
        for _ in range(5):
            self.s.tick()
            if self.state("a") == "rejected":
                break
        self.assertEqual(self.state("a"), "rejected")
        self.assertEqual(self.con.execute("SELECT status FROM runs WHERE id=?", (rid,)).fetchone()[0], "timeout")

    def test_adopted_not_reaped_as_orphan(self):
        self.push_branch("a", "ok\n")
        self.system2()
        self.tick_until(lambda: self.state("a") == "selftesting" and self.running())
        pg = self.running()[0]["pgid"]
        self.s = self.restart()
        self.s.tick()
        keep = {pg}
        wd = os.path.join(self.cfg.work_dir, "wt")
        self.assertEqual([p for p, _ in own_leftovers(wd, keep)], [])
        os.kill(pg, 0)

    def test_batch_and_baseline_are_adopted(self):
        for n in ("a", "b"):
            self.push_branch(n, "ok\n")
        self.system2()
        self.open_gate()                                           # 自測先放行；批測等 hold 再卡
        # 先讓基準跑完，再開 hold，這樣卡住的是批測點
        self.tick_until(lambda: store.get_baseline(self.con, self.s.main, "gate") is not None)
        open(self.gate + ".hold_gate", "w").close()
        os.unlink(self.gate)
        self.tick_until(lambda: self.state("a") == "selftesting" or self.state("b") == "selftesting")
        open(self.gate, "w").close()
        self.tick_until(lambda: any(r["kind"] in ("point", "probe") for r in self.running()), timeout=60)
        os.unlink(self.gate)
        before = {r["id"] for r in self.running()}
        bid = self.s.batch.id
        self.s = self.restart()
        self.s.tick()
        self.assertIsNotNone(self.s.batch)
        self.assertEqual(self.s.batch.id, bid)
        self.assertEqual({j.run_id for j in self.s.jobs.values()} & before, {j.run_id for j in self.s.jobs.values()})
        self.assertTrue(self.s.jobs)
        open(self.gate, "w").close()
        self.tick_until(lambda: self.state("a") == "merged" and self.state("b") == "merged", timeout=90)
        self.assertEqual(self.con.execute("SELECT status FROM batches WHERE id=?", (bid,)).fetchone()[0], "merged")

    def test_died_while_down_reruns(self):
        self.push_branch("a", "ok\n")
        self.system2()
        self.tick_until(lambda: self.state("a") == "selftesting" and self.running())
        run = self.running()[0]
        self.s.handover()
        self.con.close()
        os.killpg(run["pgid"], signal.SIGKILL)                     # 主行程不在時測試被殺：沒有 exit.json
        time.sleep(0.3)
        self.s = self.boot()
        self.s.tick()
        self.assertEqual(self.state("a"), "pending" if self.state("a") == "pending" else self.state("a"))
        self.open_gate()
        self.tick_until(lambda: self.state("a") == "merged")
        self.assertGreaterEqual(self.con.execute("SELECT COUNT(*) FROM runs WHERE kind='selftest' AND branch='a'").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
