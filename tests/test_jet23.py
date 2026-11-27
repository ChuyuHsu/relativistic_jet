"""指定種類的工作每顆實體核只用一條執行緒（並行數減半）；其他種類照舊兩條都給。"""
import unittest

from jet import config
from jet.cores import PhysicalPool

TOPO = {c: frozenset((c, c + 8)) for c in range(0, 8)}
TOPO.update({c + 8: frozenset((c, c + 8)) for c in range(0, 8)})


class OneThreadTest(unittest.TestCase):
    def test_pool_acquire_threads_override(self):
        p = PhysicalPool(range(0, 16), "all", topo=TOPO)
        a = p.acquire(2, "one")
        self.assertEqual(len(a), 2)                      # 兩個實體核、各一條
        b = p.acquire(2)
        self.assertEqual(len(b), 4)                      # 預設 all：兩個實體核、各兩條
        self.assertFalse(set(a) & set(b))
        p.release(a)
        self.assertEqual(len(p.acquire(2, "one")), 2)    # 釋放後那兩個實體核整顆回來
        c = p.acquire(4, "one")
        self.assertEqual(len(c), 4)
        self.assertEqual(p.acquire(1, "one"), [])        # 8 個實體核都用完（兄弟那條空著也算占）

    def test_config_parse_and_validate(self):
        base = {"git": {"remote": "x"}, "commands": {"test": ["t"]},
                "cores": {"pool": "0-15", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 1}}
        d = dict(base, cores=dict(base["cores"], unit="physical", one_thread_kinds=["baseline", "loopprobe", "nightly"]))
        self.assertEqual(config.parse(d).one_thread_kinds, ("baseline", "loopprobe", "nightly"))
        bad = dict(base, cores=dict(base["cores"], one_thread_kinds=["nope"]))
        with self.assertRaises(config.ConfigError):
            config.parse(bad)
        self.assertEqual(config.parse(base).one_thread_kinds, ())


from tests.test_scheduler import Base


class LaunchTest(Base):
    def test_launch_uses_one_thread_for_listed_kinds(self):
        s = self.mk(cores={"unit": "physical", "pool": "0-15", "selftest_cores": 2, "batch_cores": 4, "reserve_for_selftest": 8,
                           "one_thread_kinds": ["baseline"]})
        s.pool = PhysicalPool(self.cfg.pool, self.cfg.threads, TOPO)
        main = self.repo.main_sha()
        self.assertTrue(s._launch("baseline", "gate", main, 4, 100, base_sha=main))
        self.assertEqual(len(self.ex.started[-1].cores), 4)              # 4 個實體核、各一條
        self.assertTrue(s._launch("selftest", "selftest", main, 2, 100, branch="x", base_sha=main, head_sha=main))
        self.assertEqual(len(self.ex.started[-1].cores), 4)              # 沒列的種類：2 個實體核、各兩條


if __name__ == "__main__":
    unittest.main()
