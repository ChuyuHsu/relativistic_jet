"""端到端：真的 git、真的執行器、合成範例專案（examples/toy_project）。
驗收 1、2、3、5（DESIGN 第 11 節）。"""
import os, subprocess, tempfile, time, unittest

from jet import config, cores, db, store
from jet.executor import Executor
from jet.gitops import GitRepo
from jet.scheduler import Scheduler

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TOY = os.path.join(ROOT, "examples", "toy_project")
def _free4():
    allowed = sorted(os.sched_getaffinity(0))
    try:
        busy = cores.foreign_busy(allowed, set())          # 前幾顆核常被別人釘住工作：挑沒被綁的
    except Exception:
        busy = set()
    free = [c for c in allowed if c not in busy]
    return (free if len(free) >= 4 else allowed)[:4]


CORES = _free4()


def git(cwd, *a):
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@x", GIT_COMMITTER_NAME="t",
               GIT_COMMITTER_EMAIL="t@x")
    return subprocess.run(["git", *a], cwd=cwd, check=True, capture_output=True, text=True, env=env).stdout.strip()


class E2E(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.addCleanup(self.t.cleanup)
        r = self.t.name
        self.remote = os.path.join(r, "remote.git")
        self.work = os.path.join(r, "clone")
        git(r, "init", "-q", "--bare", "-b", "main", self.remote)
        git(r, "clone", "-q", self.remote, self.work)
        git(self.work, "checkout", "-q", "-b", "main")
        os.makedirs(os.path.join(self.work, "tests"))
        self.write("tests/base.txt", "ok\n")
        git(self.work, "add", "-A"); git(self.work, "commit", "-q", "-m", "base")
        git(self.work, "push", "-q", "origin", "main")

    def write(self, path, text):
        with open(os.path.join(self.work, path), "w") as f:
            f.write(text)

    def push_branch(self, name, text):
        git(self.work, "checkout", "-q", "-b", name, "main")
        self.write(f"tests/{name}.txt", text)
        git(self.work, "add", "-A"); git(self.work, "commit", "-q", "-m", name)
        git(self.work, "push", "-q", "origin", name)
        git(self.work, "checkout", "-q", "main")

    def system(self, **limits):
        r = self.t.name
        cfg = config.parse({
            "git": {"remote": self.remote},
            "paths": {"state_dir": os.path.join(r, "state"), "work_dir": os.path.join(r, "work")},
            "cores": {"pool": f"{CORES[0]}-{CORES[-1]}" if CORES == list(range(CORES[0], CORES[-1] + 1))
                      else ",".join(map(str, CORES)),
                      "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 1},
            "limits": {"selftest_timeout_s": 600, "batch_timeout_s": 1200, "batch_max": 8,
                       "points": [2, 4, 8], **limits},
            "policy": {"require_approval": False},
            "commands": {"test": ["python3", os.path.join(TOY, "run_tests.py"), "{layer}", "{ncores}", "{report}"]},
        })
        con = db.connect(os.path.join(r, "state.sqlite"))
        self.addCleanup(con.close)
        repo = GitRepo(cfg.remote, cfg.work_dir, cfg.main_branch, cfg.branch_patterns)
        s = Scheduler(cfg, con, repo, Executor(cfg.test_cmd))
        self.con, self.s = con, s
        return s

    def drive(self, until, timeout=900):
        end = time.time() + timeout
        while time.time() < end:
            self.s.tick()
            if until():
                return
            time.sleep(0.05)
        self.fail("逾時：" + str({r["name"]: (r["state"], r["reject_reason"]) for r in store.list_branches(self.con)}))

    def state(self, n):
        r = store.get_branch(self.con, n)
        return r["state"] if r else None

    def remote_refs(self):
        out = git(self.work, "ls-remote", "--heads", self.remote)
        return {l.split("refs/heads/")[1] for l in out.splitlines()}

    def main_tests(self):
        git(self.work, "fetch", "-q", "origin")
        return set(git(self.work, "ls-tree", "--name-only", "origin/main", "tests/").replace("tests/", "").split())

    # 驗收 1、5：自測、批測、合入；只動 main 與已合入的分支
    def test_all_green_merge(self):
        for n in ("a", "b", "c"):
            self.push_branch(n, "ok\n")
        self.system()
        self.drive(lambda: all(self.state(n) == "merged" for n in "abc"))
        self.assertEqual(self.main_tests(), {"base.txt", "a.txt", "b.txt", "c.txt"})
        self.assertEqual(self.remote_refs(), {"main"})          # 已合入的分支刪掉，沒有別的 ref

    # 驗收 2：兩條自測綠、整合紅的分支在同一批，兩條都被找出，其餘合入
    def test_two_red_found(self):
        reds = {"b3", "b6"}
        for i in range(1, 9):
            self.push_branch(f"b{i}", "GATE-FAIL\n" if f"b{i}" in reds else "ok\n")
        self.system()
        self.drive(lambda: self.state("b3") == "rejected" and self.state("b6") == "rejected"
                   and sum(self.state(f"b{i}") == "merged" for i in range(1, 9)) == 6, timeout=900)
        self.assertEqual(self.main_tests(), {"base.txt"} | {f"b{i}.txt" for i in (1, 2, 4, 5, 7, 8)})
        self.assertEqual(self.remote_refs(), {"main", "b3", "b6"})
        for n in reds:
            self.assertIn("b" + n[1:] + ".txt", store.get_branch(self.con, n)["reject_reason"])

    # 驗收 3：main 本來就有紅項，沒有新紅的分支照常合入，新紅的被擋
    def test_main_already_red(self):
        self.write("tests/old.txt", "FAIL\n")
        git(self.work, "add", "-A"); git(self.work, "commit", "-q", "-m", "old red")
        git(self.work, "push", "-q", "origin", "main")
        self.push_branch("good", "ok\n")
        self.push_branch("bad", "FAIL\n")
        self.system()
        self.drive(lambda: self.state("good") == "merged" and self.state("bad") == "rejected")
        self.assertIn("old.txt", self.main_tests())
        self.assertIn("good.txt", self.main_tests())
        self.assertNotIn("bad.txt", self.main_tests())


if __name__ == "__main__":
    unittest.main()
