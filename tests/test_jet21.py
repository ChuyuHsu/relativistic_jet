"""背景 loop：看板回了嫌疑且 main 前進就改對最新 main 重跑閘門；完整層等核太久時不啟動新探測。"""
import json
import unittest

from jet import store
from tests.test_scheduler import Base


class LoopRegateTest(Base):
    def _loop(self):
        s = self.mk(workflow={"mode": "direct_merge", "loop": {"enabled": True}})
        return s

    def test_answered_and_main_moved_cancels_bisect(self):
        s = self._loop()
        st = {"items": ["x"], "cands": ["c0", "c1", "c2"], "lo": -1, "hi": 2, "phase": "bisect", "queue": [],
              "red_sha": "old-main", "answered": True}
        store.set_meta(self.con, "loop_bisect", json.dumps(st))
        store.set_meta(self.con, "loop_tested", "old-main")
        s.main = self.repo.main_sha()
        s._loop_step()
        self.assertEqual(store.get_meta(self.con, "loop_bisect"), "")
        self.assertNotEqual(store.get_meta(self.con, "loop_tested"), s.main)

    def test_unanswered_bisect_is_kept(self):
        s = self._loop()
        st = {"items": ["x"], "cands": ["c0", "c1", "c2"], "lo": -1, "hi": 2, "phase": "bisect", "queue": [],
              "red_sha": "old-main"}
        store.set_meta(self.con, "loop_bisect", json.dumps(st))
        s.main = self.repo.main_sha()
        s._loop_step()
        self.assertTrue(store.get_meta(self.con, "loop_bisect"))

    def test_starving_nightly_blocks_new_probes(self):
        s = self._loop()
        s.main = self.repo.main_sha()
        s._nightly_wait_since = s.clock() - 99999
        s._loop_step()
        self.assertEqual([j for j in self.ex.started if j.layer == "gate"], [])


if __name__ == "__main__":
    unittest.main()
