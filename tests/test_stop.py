"""停機與換版不需要 kill：jet stop／restart／start。"""
import os, shutil, tempfile, time, unittest

from jet import guard, ops, store
from jet.cli import main as cli_main
from tests.ops_helpers import make_env, Running


class FakeSched:
    """只實作 serve 用到的：tick、idle、draining、shutdown。"""
    def __init__(self, cfg, con, wq):
        self.busy = 0
        self.draining = False
        self.killed = False

    def tick(self): pass
    def idle(self): return self.busy <= 0
    def shutdown(self): self.killed = True


class SupervisorStopTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        for r in ("v1", "v2"):
            os.makedirs(os.path.join(self.d, "releases", r))
        guard.deploy(self.d, "v1")
        self.addCleanup(shutil.rmtree, self.d, True)

    def sup(self, codes, **kw):
        class P:
            def __init__(s, code): s.code = code
            def wait(s): return s.code
            def poll(s): return s.code
        it = iter(codes)
        self.starts = []

        def popen(cmd, cwd=None):
            self.starts.append(cwd)
            return P(next(it))
        return guard.Supervisor(self.d, self.d, lambda r: ["x"], sleeper=lambda s: None, popen=popen, **kw)

    def test_restart_code_relaunches_with_new_current_and_is_not_a_crash(self):
        calls = []
        sup = self.sup([guard.RESTART_CODE, guard.RESTART_CODE, guard.RESTART_CODE, guard.RESTART_CODE, 0],
                       reexec=lambda: calls.append(1))
        guard.deploy(self.d, "v2")                         # 換版：current 指到新的
        self.assertEqual(sup.run(max_starts=10), 0)
        self.assertEqual(len(calls), 4)                    # 每次重啟都請守護自己 exec 成新版
        self.assertEqual(guard._target(self.d, "current"), "v2")   # 重啟不算當掉：沒有觸發退版
        self.assertTrue(all(c.endswith("v2") for c in self.starts))

    def test_restart_without_reexec_still_relaunches(self):
        sup = self.sup([guard.RESTART_CODE, 0])
        self.assertEqual(sup.run(max_starts=5), 0)
        self.assertEqual(len(self.starts), 2)

    def test_stop_file_stops_supervisor_even_after_crash(self):
        sup = self.sup([1, 0])
        open(os.path.join(self.d, guard.STOP_FILE), "w").write("x")
        self.assertEqual(sup.run(max_starts=5), 0)
        self.assertEqual(len(self.starts), 1)              # 不再拉起
        self.assertFalse(os.path.exists(os.path.join(self.d, guard.STOP_FILE)))   # 用掉就清

    def test_clean_exit_stops_supervisor(self):
        sup = self.sup([0, 0])
        self.assertEqual(sup.run(max_starts=5), 0)
        self.assertEqual(len(self.starts), 1)

    def test_pid_alive_checks_real_process(self):
        self.assertIsNone(guard.pid_alive(self.d))
        guard.write_pid(self.d)                            # 這個測試行程不是 supervise
        self.assertIsNone(guard.pid_alive(self.d))


class ServerStopTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        self.env = make_env(self.d)
        self.sched = {}

        def factory(cfg, con, wq):
            self.sched["s"] = FakeSched(cfg, con, wq)
            return self.sched["s"]
        self.factory = factory

    def post(self, run, body):
        import json, urllib.request
        wk = open(self.env["write_key"]).read().strip() if "write_key" in self.env else None
        return wk

    def wait_exit(self, run, secs=5):
        end = time.time() + secs
        while time.time() < end and run.t.is_alive():
            time.sleep(0.05)
        return not run.t.is_alive()

    def test_op_stop_validates(self):
        with self.assertRaises(ops.OpError):
            ops.op_stop(None, 0, "t", {"mode": "x"})
        with self.assertRaises(ops.OpError):
            ops.op_stop(None, 0, "t", {"restart": "yes"})

    def test_graceful_waits_for_idle_then_exits_with_code(self):
        with Running(self.env, self.factory) as r:
            s = self.sched["s"]
            s.busy = 1
            store.set_meta(r.srv.con, "x", "1") if False else None
            r.srv.wq.submit("stop", {"restart": True}, "t")
            time.sleep(1.2)
            self.assertTrue(r.t.is_alive())               # 還在忙：不退
            self.assertTrue(s.draining)                    # 但已經不再派新工作
            s.busy = 0
            self.assertTrue(self.wait_exit(r))
            self.assertEqual(r.srv.exit_code, guard.RESTART_CODE)

    def test_now_kills_jobs_and_exits_zero_for_stop(self):
        with Running(self.env, self.factory) as r:
            s = self.sched["s"]
            s.busy = 5
            r.srv.wq.submit("stop", {"mode": "now"}, "t")
            self.assertTrue(self.wait_exit(r))
            self.assertTrue(s.killed)
            self.assertEqual(r.srv.exit_code, 0)

    def test_stale_request_ignored_after_restart(self):
        with Running(self.env, self.factory) as r:
            r.srv.wq.submit("stop", {"mode": "now"}, "t")
            self.assertTrue(self.wait_exit(r))
        with Running(self.env, self.factory) as r2:           # 新一輪啟動：舊的停機要求已清掉
            time.sleep(1.0)
            self.assertTrue(r2.t.is_alive())


class CliTest(unittest.TestCase):
    def test_parser_has_commands(self):
        from jet.cli import build_parser
        p = build_parser()
        a = p.parse_args(["stop", "--root", "/x", "--now", "--wait", "3"])
        self.assertTrue(a.now)
        self.assertEqual(a.wait, 3)
        self.assertTrue(p.parse_args(["restart"]).fn)
        self.assertTrue(p.parse_args(["start", "--root", "/x"]).fn)


if __name__ == "__main__":
    unittest.main()
