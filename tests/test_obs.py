"""可觀測性與回歸：非 bare 來源、fetch 逾時、背景 fetch、掃描日誌、doctor、status --verbose。"""
import json, logging, os, subprocess, tempfile, threading, time, unittest

from jet import config, db, doctor, ops, store
from jet.cli import _verbose_lines
from jet.gitops import GitError, GitRepo
from jet.scheduler import Scheduler
from tests.fakes import FakeRepo, FakeExecutor

ENV = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@x", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@x")


def git(cwd, *a):
    return subprocess.run(["git", *a], cwd=cwd, check=True, capture_output=True, text=True, env=ENV).stdout.strip()


def make_nonbare(root):
    """非 bare 來源（有工作目錄、分支檢出過），分支有 dev/、ok/、claude/ 前綴。"""
    src = os.path.join(root, "src")
    os.makedirs(src)
    git(src, "init", "-q", "-b", "main")
    open(os.path.join(src, "a.txt"), "w").write("x")
    git(src, "add", "-A"); git(src, "commit", "-q", "-m", "base")
    for b in ("dev/a", "dev/b", "ok/a", "claude/x"):
        git(src, "checkout", "-q", "-b", b, "main")
        open(os.path.join(src, b.replace("/", "_")), "w").write(b)
        git(src, "add", "-A"); git(src, "commit", "-q", "-m", b)
    git(src, "checkout", "-q", "main")
    return src


class NonBare(unittest.TestCase):
    def test_nonbare_source_refs_under_remotes_and_pattern(self):
        with tempfile.TemporaryDirectory() as t:
            src = make_nonbare(t)
            repo = GitRepo(src, os.path.join(t, "work"), "main", ["dev/*"])
            repo.fetch()
            refs = git(repo.mirror, "for-each-ref", "--format=%(refname)").splitlines()
            self.assertIn("refs/remotes/origin/dev/a", refs)
            self.assertFalse([r for r in refs if r.startswith("refs/heads/")])   # 全在 remotes 底下
            self.assertEqual(set(repo.list_branches()), {"dev/a", "dev/b"})        # 樣式只收 dev/*，不收 main／ok／claude
            info = repo.mirror_info()
            self.assertEqual(info["refs"], 5)
            self.assertEqual(repo.main_sha(), git(src, "rev-parse", "main"))

    def test_scheduler_scans_nonbare_source(self):
        with tempfile.TemporaryDirectory() as t:
            src = make_nonbare(t)
            cfg = config.parse({"git": {"remote": src, "branch_patterns": ["dev/*"]},
                                "paths": {"state_dir": os.path.join(t, "st"), "work_dir": os.path.join(t, "w")},
                                "cores": {"pool": "0-1"}, "commands": {"test": ["true"]}})
            con = db.connect(os.path.join(t, "a.sqlite"))
            repo = GitRepo(src, cfg.work_dir, "main", ["dev/*"])
            s = Scheduler(cfg, con, repo, FakeExecutor(FakeRepo()))
            s.tick()
            self.assertEqual({r["name"] for r in store.list_branches(con)}, {"dev/a", "dev/b"})
            scan = json.loads(store.get_meta(con, "obs_scan"))
            self.assertEqual((scan["remote"], scan["new"]), (2, 2))
            con.close()

    def test_fetch_timeout_kills_hung_ssh(self):
        with tempfile.TemporaryDirectory() as t:
            env = dict(os.environ, GIT_SSH_COMMAND="sh -c 'sleep 31' x")
            old = os.environ.copy()
            os.environ.update(env)
            try:
                repo = GitRepo("ssh://host/repo.git", os.path.join(t, "w"), "main", ["*"], 10, 1)
                t0 = time.time()
                with self.assertRaises(GitError) as cm:
                    repo.fetch()
                self.assertLess(time.time() - t0, 10)
                self.assertIn("逾時", str(cm.exception))
            finally:
                os.environ.clear(); os.environ.update(old)
            time.sleep(0.3)
            ps = subprocess.run(["pgrep", "-f", "sleep 31"], capture_output=True, text=True).stdout.split()
            self.assertEqual(ps, [])                     # 沒有留下孤兒


class SlowFetchRepo(FakeRepo):
    def __init__(self, delay):
        super().__init__()
        self.delay, self.fetched = delay, False

    def fetch(self):
        time.sleep(self.delay)
        self.fetched = True

    def main_sha(self):
        if not self.fetched:
            raise GitError("鏡像是空的")
        return super().main_sha()


class Observability(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.addCleanup(self.t.cleanup)
        self.cfg = config.parse({"git": {"remote": "x", "fetch_interval_s": 1},
                                 "paths": {"state_dir": os.path.join(self.t.name, "st")},
                                 "cores": {"pool": "0-3"}, "commands": {"test": ["t"]}})
        self.con = db.connect(os.path.join(self.t.name, "a.sqlite"))
        self.addCleanup(self.con.close)

    def test_async_fetch_does_not_block_and_reports(self):
        repo = SlowFetchRepo(0.6)
        repo.add("a")
        s = Scheduler(self.cfg, self.con, repo, FakeExecutor(repo), async_fetch=True)
        t0 = time.time()
        s.tick()
        self.assertLess(time.time() - t0, 0.3)                       # 第一輪不等 fetch
        f = json.loads(store.get_meta(self.con, "obs_fetch"))
        self.assertEqual(f["state"], "running")
        self.assertIn("第一次 fetch", json.loads(store.get_meta(self.con, "obs_scan"))["waiting"])
        self.assertEqual(store.list_branches(self.con), [])
        time.sleep(0.9)
        s.tick()
        self.assertEqual(json.loads(store.get_meta(self.con, "obs_fetch"))["state"], "ok")
        self.assertEqual(len(store.list_branches(self.con)), 1)
        # status 可以讀到
        st = ops.query_status(self.con)
        self.assertEqual(st["scheduler"]["scan"]["remote"], 1)
        self.assertTrue(any("掃描" in l for l in _verbose_lines(st["scheduler"], st["now"])))

    def test_fetch_error_logged_and_event_once(self):
        class Bad(FakeRepo):
            def fetch(self):
                raise GitError("git fetch 失敗（128）：Permission denied (publickey)")
        repo = Bad()
        s = Scheduler(self.cfg, self.con, repo, FakeExecutor(repo))
        for _ in range(3):
            with self.assertRaises(GitError):
                s.tick()
        self.assertEqual(self.con.execute("SELECT count(*) FROM events WHERE kind='queue_error'").fetchone()[0], 1)
        f = json.loads(store.get_meta(self.con, "obs_fetch"))
        self.assertEqual(f["state"], "error")
        self.assertIn("publickey", f["error"])
        self.assertIn("publickey", json.loads(store.get_meta(self.con, "obs_last_error"))["message"])

    def test_scan_logged_and_empty_remote_flagged(self):
        repo = FakeRepo()
        s = Scheduler(self.cfg, self.con, repo, FakeExecutor(repo))
        with self.assertLogs("jet", level="INFO") as cm:
            s.tick()
        self.assertTrue(any("掃描：遠端 0 條" in m for m in cm.output))
        self.assertTrue(any("樣式" in m for m in cm.output))           # 空遠端會提示檢查分支樣式
        repo.add("a")
        with self.assertLogs("jet", level="INFO") as cm:
            s.tick()
        self.assertTrue(any("新 1" in m for m in cm.output))


class Doctor(unittest.TestCase):
    def test_doctor_flags_pattern_mismatch_and_ok_case(self):
        with tempfile.TemporaryDirectory() as t:
            src = make_nonbare(t)

            def cfg(patterns):
                return config.parse({"git": {"remote": src, "branch_patterns": patterns},
                                     "paths": {"state_dir": os.path.join(t, "st"), "work_dir": os.path.join(t, "w")},
                                     "cores": {"pool": "0-1"}, "commands": {"test": ["python3", "x.py"]}})
            good = {n: s for s, n, _ in doctor.run_checks(cfg(["dev/*"]), 20)}
            self.assertEqual(good["remote 連線"], "ok")
            self.assertEqual(good["分支樣式"], "ok")
            bad = {n: (s, m) for s, n, m in doctor.run_checks(cfg(["feature/*"]), 20)}
            self.assertEqual(bad["分支樣式"][0], "fail")
            self.assertIn("沒有任何一條符合", bad["分支樣式"][1])

    def test_doctor_remote_unreachable(self):
        with tempfile.TemporaryDirectory() as t:
            c = config.parse({"git": {"remote": os.path.join(t, "nope.git")},
                              "paths": {"state_dir": os.path.join(t, "st"), "work_dir": os.path.join(t, "w")},
                              "cores": {"pool": "0-1"}, "commands": {"test": ["python3"]}})
            res = {n: s for s, n, _ in doctor.run_checks(c, 10)}
            self.assertEqual(res["remote 連線"], "fail")


if __name__ == "__main__":
    unittest.main()
