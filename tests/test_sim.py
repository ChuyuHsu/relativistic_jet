"""隨機模擬：假 Git／假執行器＋亂數種子，混合推新提交、放行、堆疊、撞檔、逾時、
基礎設施錯誤、推 main 失敗、外人推 main、排隊器被砍掉重啟。

唯一硬性斷言：每一次推進 main 的提交，都是完整測過而且跟當時 main 相比沒有新紅項的那個。
另有收斂檢查：停止外力後所有分支都走到終點（或合理的等待）。
環境變數 JET_SIM_SEEDS 調整種子數（預設 40）。"""
import os, random, tempfile, unittest

from jet import config, db, store
from jet.gitops import MainMoved
from jet.scheduler import Scheduler
from tests.fakes import FakeRepo, FakeExecutor

SEEDS = int(os.environ.get("JET_SIM_SEEDS", "40"))
ACTIVE = {"pending", "selftesting", "queued", "batching"}


class Sim:
    def __init__(self, seed, tmp):
        self.r = random.Random(seed)
        r = self.r
        self.cfg = config.parse({
            "git": {"remote": "x"}, "paths": {"state_dir": os.path.join(tmp, "st")},
            "cores": {"pool": f"0-{r.choice([5, 11, 31])}", "selftest_cores": 1, "batch_cores": 1,
                      "reserve_for_selftest": r.choice([0, 2, 4])},
            "limits": {"batch_max": r.choice([2, 4, 8, 16]), "double_fail_halt": r.choice([2, 3])},
            "policy": {"require_approval": r.random() < 0.7}, "commands": {"test": ["t"]}})
        self.con = db.connect(os.path.join(tmp, "a.sqlite"))
        self.repo = FakeRepo()
        self.repo.main_red = {"old"} if r.random() < 0.3 else set()
        self.ff_fail = 0.0
        self.violations = []
        orig = self.repo.fast_forward_main

        def ff(sha, expect):
            if r.random() < self.ff_fail:
                raise MainMoved()
            orig(sha, expect)
            # 核心斷言：推上去的提交沒有「相對推之前 main」的新紅項，而且確實有過綠的測試紀錄
            new_red = self.repo.reds_of(sha, "gate") - self.repo.reds_of(expect, "gate")
            if new_red:
                self.violations.append(f"推 main 帶新紅 {sha} {new_red}")
            ran = self.con.execute("SELECT count(*) FROM runs WHERE commit_sha=? AND layer='gate' "
                                   "AND status IN ('pass','fail')", (sha,)).fetchone()[0]
            if not ran:
                self.violations.append(f"推 main 的提交沒有完整測過 {sha}")
        self.repo.fast_forward_main = ff
        self.make_sched()
        self.counter = 0
        self.pending_parent = []

    def make_sched(self):
        self.ex = FakeExecutor(self.repo, self.r.choice([0, 1, 2]))
        self.s = Scheduler(self.cfg, self.con, self.repo, self.ex, clock=lambda: 1000.0)

    def chaos(self, p=1.0):
        r, repo = self.r, self.repo
        if r.random() < 0.35 * p and self.counter < 24:
            self.counter += 1
            n = f"b{self.counter}"
            repo.add(n, reds={"s"} if r.random() < 0.1 else (),
                     gate_reds={"g"} if r.random() < 0.25 else (),
                     conflict_main=r.random() < 0.04,
                     conflict_with=({f"b{r.randint(1, self.counter)}"} if r.random() < 0.15 else ()))
            if r.random() < 0.3 and self.counter > 1:
                self.pending_parent.append((n, f"b{r.randint(1, self.counter - 1)}"))
        for n, par in list(self.pending_parent):
            if store.get_branch(self.con, n) and store.get_branch(self.con, par):
                try:
                    store.set_parent(self.con, n, par)
                except ValueError:
                    pass
                self.pending_parent.remove((n, par))
        names = list(repo.branches)
        if names and r.random() < 0.25 * p:
            n = r.choice(names)
            if store.get_branch(self.con, n):
                store.approve(self.con, n, "sim")
        if names and r.random() < 0.05 * p:
            n = r.choice(names)
            repo.push_new(n, reds=(), gate_reds={"g"} if r.random() < 0.3 else (),
                          conflict_with=repo.branches[n]["conflict_with"])
            repo.branches[n]["files"] = ["src/new.py"]
        if r.random() < 0.02 * p:
            self.make_sched()                                   # 砍掉重啟
        if r.random() < 0.03 * p:
            ext = f"ext{r.randint(0, 10**6)}"
            repo.merged[ext] = dict(reds=set(), gate_reds=set(), files=["src/x.py"])
            repo.main = repo.main + ">" + ext
            repo.main_files[repo.main] = ["src/x.py"]
        if store.is_paused(self.con) and r.random() < 0.2:
            store.set_paused(self.con, False, "sim")        # 人來處理暫停
        self.ex.force = {}
        if r.random() < 0.08 * p and repo.branches:
            n = r.choice(list(repo.branches))
            self.ex.force[f"{repo.main}>{n}"] = r.choice(["infra", "timeout"])

    def snapshot(self):
        return tuple((x["name"], x["state"], x["head_sha"]) for x in store.list_branches(self.con))


class SimTest(unittest.TestCase):
    def run_seed(self, seed):
        with tempfile.TemporaryDirectory() as tmp:
            sim = Sim(seed, tmp)
            try:
                sim.ff_fail = 0.1
                for _ in range(250):
                    sim.chaos()
                    sim.s.tick()
                sim.ff_fail = 0.0
                store.set_paused(sim.con, False, "sim")
                for n in list(sim.repo.branches):
                    if store.get_branch(sim.con, n):
                        store.approve(sim.con, n, "sim")
                stable, last = 0, None
                for _ in range(1500):
                    sim.ex.force = {}
                    sim.s.tick()
                    snap = (sim.snapshot(), sim.repo.main)
                    stable = stable + 1 if snap == last else 0
                    last = snap
                    if stable >= 15 and not sim.s.jobs and sim.s.batch is None:
                        break
                self.assertEqual(sim.violations, [], f"seed={seed}")
                for x in store.list_branches(sim.con):
                    st = x["state"]
                    waiting_parent = x["parent"] and (store.get_branch(sim.con, x["parent"]) or {"state": ""})["state"] in (
                        "rejected", "halted", "pending")
                    if st in ACTIVE and not waiting_parent:
                        self.fail(f"seed={seed} 沒收斂：{x['name']} {st} {x['reject_reason']}")
            finally:
                sim.con.close()

    def test_seeds(self):
        for seed in range(SEEDS):
            with self.subTest(seed=seed):
                self.run_seed(seed)


if __name__ == "__main__":
    unittest.main()
