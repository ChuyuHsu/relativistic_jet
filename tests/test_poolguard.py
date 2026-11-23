"""核池防呆：實體核模式但池裡沒有兄弟執行緒時，doctor／check-config／執行中的最近錯誤都要講出來。"""
import json, os, tempfile, unittest

from jet import config, cores, db, store, ops
from jet.scheduler import Scheduler
from tests.fakes import FakeRepo, FakeExecutor

TOPO = {c: frozenset([c, c + 4]) for c in range(4)}
TOPO.update({c + 4: frozenset([c, c + 4]) for c in range(4)})


def cfg_of(tmp, pool, unit="physical", sc=1, bc=1):
    return config.parse({"git": {"remote": "x"}, "paths": {"state_dir": os.path.join(tmp, "st")},
                         "cores": {"pool": pool, "selftest_cores": sc, "batch_cores": bc, "unit": unit},
                         "commands": {"test": ["t"]}})


class PoolCheck(unittest.TestCase):
    def test_no_siblings_in_pool_is_fail_and_names_excluded(self):
        with tempfile.TemporaryDirectory() as t:
            r = cores.pool_check(cfg_of(t, "0-3"), TOPO)               # 只有每個實體核的第一條
            self.assertEqual(r[0][0], "fail")
            self.assertIn("0", r[0][1])
            self.assertIn("排除", r[0][1])

    def test_fewer_than_job_needs_is_fail(self):
        with tempfile.TemporaryDirectory() as t:
            r = cores.pool_check(cfg_of(t, "0,4", sc=2), TOPO)         # 一個完整實體核，工作要 2 個
            self.assertEqual(r[0][0], "fail")

    def test_partial_excluded_but_enough_is_warn(self):
        with tempfile.TemporaryDirectory() as t:
            r = cores.pool_check(cfg_of(t, "0,4,1"), TOPO)             # 1 的兄弟 5 不在池裡
            self.assertEqual(r[0][0], "warn")
            self.assertIn("1", r[0][1])

    def test_full_pool_ok(self):
        with tempfile.TemporaryDirectory() as t:
            self.assertEqual(cores.pool_check(cfg_of(t, "0,4,1,5"), TOPO)[0][0], "ok")

    def test_logical_mode_ok(self):
        with tempfile.TemporaryDirectory() as t:
            self.assertEqual(cores.pool_check(cfg_of(t, "0-3", unit="logical"), TOPO)[0][0], "ok")

    def test_scheduler_reports_empty_pool_in_last_error(self):
        with tempfile.TemporaryDirectory() as t:
            cfg = cfg_of(t, "0-3")
            con = db.connect(os.path.join(t, "a.sqlite"))
            repo = FakeRepo()
            s = Scheduler(cfg, con, repo, FakeExecutor(repo), clock=lambda: 1.0,
                          foreign_busy=lambda c, p: set())
            s.pool = cores.PhysicalPool(cfg.pool, cfg.threads, TOPO)    # 模擬這台機器的拓撲
            s.tick()
            ev = [r["detail"] for r in con.execute("SELECT detail FROM events WHERE kind='queue_error'")]
            self.assertTrue(any("核池" in d and "排除" in d for d in ev), ev)
            tick = json.loads(store.get_meta(con, "obs_tick"))
            self.assertEqual((tick["pool_unit"], tick["pool_total"]), ("physical", 0))
            con.close()

    def test_status_counts_in_physical_units(self):
        with tempfile.TemporaryDirectory() as t:
            cfg = cfg_of(t, "0,4,1,5")
            con = db.connect(os.path.join(t, "a.sqlite"))
            repo = FakeRepo()
            s = Scheduler(cfg, con, repo, FakeExecutor(repo), clock=lambda: 1.0, foreign_busy=lambda c, p: set())
            s.pool = cores.PhysicalPool(cfg.pool, cfg.threads, TOPO)
            s.tick()
            tick = json.loads(store.get_meta(con, "obs_tick"))
            self.assertEqual((tick["pool_unit"], tick["pool_total"]), ("physical", 2))
            con.close()


class NotifyCodeTest(unittest.TestCase):
    def test_response_code_recorded(self):
        from jet import notify
        with tempfile.TemporaryDirectory() as t:
            con = db.connect(os.path.join(t, "a.sqlite"))
            con.execute("INSERT INTO events(kind,branch,detail,at) VALUES('rejected','a','x',0)")

            def poster(url, payload, timeout, headers=None):
                raise notify.NotifyError("對方回 403")
            n = notify.Notifier("http://127.0.0.1:9/x", 0, poster=poster)
            n.tick(con, 100.0)
            for d in n.dests:
                d.thread.join()
            n.tick(con, 100.1)
            last = json.loads(store.get_meta(con, "notify_last:default"))
            self.assertEqual(last["code"], 403)
            self.assertIn("403", last["error"])
            con.close()


if __name__ == "__main__":
    unittest.main()
