"""隨機模擬（先審後測＋外部來源）：假看板清單會隨機發卡、父卡、放行、撤回、重測、放棄、
內容改動與單純 rebase，來源會隨機拉不到；排隊器隨機重啟。

硬性斷言：
 1. 每次推 main 的提交完整測過、沒有新紅（跟原本的模擬一樣）。
 2. 推 main 時，整串成員在看板當下都還是登記的、放行的版本內容就是測過的內容；
    父卡不在 main 的話一定也在同一次推進裡而且排在前面。
 3. 清單超過 max_age 沒更新成功時，一次都不推 main。
 4. 先審後測：沒放行過的內容不開自測。
收斂：外力停止、來源恢復後，所有分支走到終點或合理的等待。
環境變數 JET_SIM_SEEDS 調整種子數（預設 40）。"""
import json, os, random, tempfile, unittest

from jet import config, db, store
from jet.gitops import MainMoved
from jet.scheduler import Scheduler
from jet.source import SourceError, SourcePoller, parse_list
from tests.fakes import FakeRepo, FakeExecutor

SEEDS = int(os.environ.get("JET_SIM_SEEDS", "40"))
ACTIVE = {"pending", "selftesting", "queued", "batching"}


class CardSim:
    def __init__(self, seed, tmp):
        self.r = r = random.Random(seed)
        self.cfg = config.parse({
            "git": {"remote": "x", "scope": "tracked"}, "paths": {"state_dir": os.path.join(tmp, "st")},
            "cores": {"pool": "0-11", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 2},
            "limits": {"batch_max": r.choice([2, 4, 16]), "double_fail_halt": r.choice([2, 3]), "baseline_history": 500},
            "policy": {"order": r.choice(["review_first", "review_first", "parallel"])},
            "source": {"url": "http://127.0.0.1:9/x", "max_age_s": 600, "poll_seconds": 15},
            "commands": {"test": ["t"]}})
        self.con = db.connect(os.path.join(tmp, "a.sqlite"))
        store.set_meta(self.con, "emit_machine_events", "1")
        self.repo = FakeRepo()
        self.t = 1000.0
        self.board = {}            # 分支 → {"parent", "ap"(放行的 sha 或 None), "retest"}
        self.down = False
        self.violations = []
        self.last_ok = 1000.0
        self.counter = 0
        self.ff_fail = 0.0
        orig = self.repo.fast_forward_main

        def ff(sha, expect):
            if r.random() < self.ff_fail:
                raise MainMoved()
            orig(sha, expect)
            names = self.repo.stack_names_of(sha)
            if self.t - self.last_ok > 600:
                self.violations.append(f"清單過期還推了 main {sha}")
            new_red = self.repo.reds_of(sha, "gate") - self.repo.reds_of(expect, "gate")
            if new_red:
                self.violations.append(f"推 main 帶新紅 {sha} {new_red}")
            ran = self.con.execute("SELECT count(*) FROM runs WHERE commit_sha=? AND layer='gate' "
                                   "AND status IN ('pass','fail')", (sha,)).fetchone()[0]
            if not ran:
                self.violations.append(f"推 main 的提交沒有完整測過 {sha}")
            if not self.down:                                  # 看板當下的樣子才是準的
                seen = set()
                for n in names:
                    e = self.board.get(n)
                    b = self.repo.branches.get(n)
                    if e is None or b is None:
                        self.violations.append(f"推 main 帶了看板上已不在的卡 {n}")
                        continue
                    if not e["ap"]:
                        self.violations.append(f"推 main 帶了沒放行的卡 {n}")
                    elif self.cid_of(n, e["ap"]) != self.repo.content_id(b["sha"], None):
                        self.violations.append(f"推 main 帶了放行版本內容不符的卡 {n}")
                    par = e["parent"]
                    if par and par in self.board and par not in seen and par in self.repo.branches:
                        self.violations.append(f"推 main 時 {n} 的父卡 {par} 沒有排在它前面")
                    seen.add(n)
        self.repo.fast_forward_main = ff
        self.shas = {}             # (分支, sha) → 內容指紋，模擬「看過的提交」
        self.make_sched()

    def cid_of(self, n, sha):
        v = self.shas.get((n, sha))
        return v

    def remember(self, n):
        b = self.repo.branches[n]
        self.shas[(n, b["sha"])] = self.repo.content_id(b["sha"], None)

    def make_sched(self):
        self.ex = FakeExecutor(self.repo, self.r.choice([0, 1, 2]))
        orig_start = self.ex.start

        def start(job):
            if job.layer == "selftest" and self.cfg.order == "review_first":
                names = self.repo.stack_names_of(job.workdir)
                n = names[-1] if names else None
                e = self.board.get(n) if n else None
                if not self.down and e is not None and not e["ap"] and self.t - self.last_ok < 60:
                    self.violations.append(f"先審後測：沒放行就開自測 {n}")
            return orig_start(job)
        self.ex.start = start
        self.s = Scheduler(self.cfg, self.con, self.repo, self.ex, clock=lambda: self.t,
                           foreign_busy=lambda cores, pgids: set())

        def fetch():
            if self.down:
                raise SourceError("down")
            out = []
            for n, e in self.board.items():
                d = {"branch": n, "retest": e["retest"]}
                if e["parent"] and e["parent"] in self.board or (e["parent"] and e["parent"] not in self.repo.merged):
                    d["parent"] = e["parent"]
                if e["ap"]:
                    d["approved_sha"] = e["ap"]
                out.append(d)
            self.last_ok = self.t
            return parse_list(json.loads(json.dumps(out)))
        self.s.source = SourcePoller("http://127.0.0.1:9/x", None, 15, 1, fetcher=fetch, clock=lambda: self.t,
                                     threaded=False)
        self.s.source.ok_at = None

    def chaos(self, p=1.0):
        r, repo = self.r, self.repo
        self.t += r.choice([5, 16, 16, 40])
        if r.random() < 0.3 * p and self.counter < 20:
            self.counter += 1
            n = f"b{self.counter}"
            repo.add(n, reds={"s"} if r.random() < 0.1 else (), gate_reds={"g"} if r.random() < 0.2 else (),
                     conflict_main=r.random() < 0.03,
                     conflict_with=({f"b{r.randint(1, self.counter)}"} if r.random() < 0.1 else ()))
            self.remember(n)
            par = None
            alive = [m for m in self.board if m in repo.branches]
            if alive and r.random() < 0.4:
                par = r.choice(alive)
            self.board[n] = {"parent": par, "ap": None, "retest": 0}
        names = [n for n in self.board if n in repo.branches]
        if names and r.random() < 0.3 * p:
            n = r.choice(names)
            self.board[n]["ap"] = repo.branches[n]["sha"]            # 放行目前版本
        if names and r.random() < 0.06 * p:
            n = r.choice(names)
            self.board[n]["ap"] = None                                 # 撤回放行
        if names and r.random() < 0.05 * p:                            # 單純 rebase：內容不變，換提交
            n = r.choice(names)
            b = repo.branches[n]
            repo.push_new(n, reds=b["reds"], gate_reds=b["gate_reds"], conflict_with=b["conflict_with"])
            self.remember(n)
        if names and r.random() < 0.04 * p:                            # 自己那一段改了
            n = r.choice(names)
            repo.push_new(n, reds=(), gate_reds={"g"} if r.random() < 0.3 else (),
                          conflict_with=repo.branches[n]["conflict_with"])
            repo.branches[n]["files"] = [f"src/c{r.randint(0, 9999)}.py"]
            self.remember(n)
        if names and r.random() < 0.05 * p:
            self.board[r.choice(names)]["retest"] += 1                 # 重測
        if names and r.random() < 0.04 * p:                            # 改父卡（含拿掉）
            n = r.choice(names)
            cand = [m for m in names if m != n]
            par = r.choice(cand + [None]) if cand else None
            p2, hop = par, 0
            while p2 and hop < 50:                                     # 看板不允許繞圈
                if p2 == n:
                    par = None
                    break
                p2 = self.board.get(p2, {}).get("parent")
                hop += 1
            self.board[n]["parent"] = par
        if names and r.random() < 0.03 * p:                            # 放棄一張卡（從清單消失）
            n = r.choice(names)
            if not any(e["parent"] == n for e in self.board.values()) or r.random() < 0.3:
                self.board.pop(n, None)
        if r.random() < 0.04 * p:
            self.down = not self.down                                  # 來源壞掉／恢復
        if r.random() < 0.02 * p:
            self.make_sched()
        if r.random() < 0.03 * p:
            ext = f"ext{r.randint(0, 10**6)}"
            repo.merged[ext] = dict(reds=set(), gate_reds=set(), files=["src/x.py"])
            repo.main = repo.main + ">" + ext
            repo.main_files[repo.main] = ["src/x.py"]
        if store.is_paused(self.con) and r.random() < 0.2:
            store.set_paused(self.con, False, "sim")
        self.ex.force = {}
        if r.random() < 0.06 * p and repo.branches:
            n = r.choice(list(repo.branches))
            self.ex.force[f"{repo.main}>{n}"] = r.choice(["infra", "timeout"])


class CardSimTest(unittest.TestCase):
    def run_seed(self, seed):
        with tempfile.TemporaryDirectory() as tmp:
            sim = CardSim(seed, tmp)
            try:
                sim.ff_fail = 0.1
                for _ in range(300):
                    sim.chaos()
                    sim.s.tick()
                sim.ff_fail, sim.down = 0.0, False
                store.set_paused(sim.con, False, "sim")
                for n in list(sim.board):                               # 外力停止：全部放行目前版本
                    if n in sim.repo.branches:
                        sim.board[n]["ap"] = sim.repo.branches[n]["sha"]
                stable, last = 0, None
                for _ in range(2500):
                    sim.ex.force = {}
                    sim.t += 16
                    sim.s.tick()
                    snap = (tuple((x["name"], x["state"], x["head_sha"]) for x in store.list_branches(sim.con)), sim.repo.main)
                    stable = stable + 1 if snap == last else 0
                    last = snap
                    if stable >= 15 and not sim.s.jobs and sim.s.batch is None:
                        break
                self.assertEqual(sim.violations, [], f"seed={seed}")
                for x in store.list_branches(sim.con):
                    if x["state"] not in ACTIVE:
                        continue
                    anc, waiting = x["parent"], False
                    while anc and not waiting:
                        pr = store.get_branch(sim.con, anc)
                        if pr is None:
                            break
                        waiting = pr["state"] in ("rejected", "halted", "pending")
                        anc = pr["parent"]
                    e = sim.board.get(x["name"], {})
                    par = e.get("parent")
                    blocked = par and par not in sim.board and par not in sim.repo.merged
                    if not waiting and not blocked:
                        self.fail(f"seed={seed} 沒收斂：{x['name']} {x['state']} parent={x['parent']} {x['reject_reason']}")
            finally:
                sim.con.close()

    def test_seeds(self):
        for seed in range(SEEDS):
            with self.subTest(seed=seed):
                self.run_seed(seed)


if __name__ == "__main__":
    unittest.main()
