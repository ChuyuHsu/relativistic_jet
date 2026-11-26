"""同一張卡同一個提交連續自測逾時 2 次，下一次自測用兩倍核、佔兩個槽（同時最多 2 張雙倍卡、不搶一般卡槽到剩不到 2 槽）。"""
import json
import unittest

from jet import store
from tests.test_scheduler import Base


class DoubleCoresTest(Base):
    def _cfg(self, slots=6):
        return self.mk(cores={"max_selftest_slots": slots, "max_selftest_slots_with_batch": slots,
                              "selftest_cores": 2, "reserve_for_selftest": 24, "pool": "0-31"},
                       workflow={"mode": "direct_merge"}, policy={"require_approval": False})

    def _units(self, name):
        r = self.con.execute("SELECT units FROM runs WHERE branch=? AND kind='selftest' ORDER BY id DESC LIMIT 1", (name,)).fetchone()
        return r[0] if r else None

    def _prime(self, s, name, n):
        store.set_meta(self.con, "st_to:" + name, json.dumps({"sha": name + "-1", "n": n}))

    def test_counter_counts_same_commit_and_resets(self):
        s = self._cfg()
        self.assertEqual(s._timeouts_of("a", "x1"), 0)
        s._note_selftest_result("a", "x1", True)
        s._note_selftest_result("a", "x1", True)
        self.assertEqual(s._timeouts_of("a", "x1"), 2)
        self.assertEqual(s._timeouts_of("a", "x2"), 0)            # 換了提交不算
        s._note_selftest_result("a", "x1", False)                 # 出現逾時以外的結果：歸零
        self.assertEqual(s._timeouts_of("a", "x1"), 0)

    def test_double_after_two_timeouts_only(self):
        s = self._cfg()
        self.repo.add("a"); self.repo.add("b")
        self._prime(s, "a", 2)
        self._prime(s, "b", 1)
        self.go(10, until=lambda: self._units("a") and self._units("b"))
        self.assertEqual(self._units("a"), 4)                      # 連續逾時 2 次：兩倍核
        self.assertEqual(self._units("b"), 2)                      # 只逾時 1 次：一般

    def test_cap_and_keep_two_slots_for_normal(self):
        s = self._cfg(slots=4)
        for n in ("a", "b", "c"): self.repo.add(n)
        self._prime(s, "a", 2); self._prime(s, "b", 2)
        self.go(10, until=lambda: self._units("a") and self._units("c"))
        self.assertEqual(self._units("a"), 4)                      # 4 槽：第一張雙倍卡可以（佔 2 槽，還留 2 槽給一般卡）
        self.assertIsNone(self._units("b"))                        # 第二張雙倍卡會把一般卡的槽搶到剩 0 槽：先排隊
        self.assertEqual(self._units("c"), 2)                      # 一般卡照跑，沒被擋住



class SlowItemsNotDoubledTest(DoubleCoresTest):
    def test_low_parallel_timeouts_do_not_double(self):
        s = self._cfg()
        self.repo.add("a")
        s._note_selftest_result("a", "a-1", True, 1.5)
        n, low = s._note_selftest_result("a", "a-1", True, 2.0)
        self.assertEqual((n, low), (2, True))
        self.assertTrue(s._low_parallel("a", "a-1"))
        self.go(10, until=lambda: self._units("a"))
        self.assertEqual(self._units("a"), 2)                      # 平均用核低：不加倍

    def test_high_parallel_timeouts_still_double(self):
        s = self._cfg()
        self.repo.add("a")
        s._note_selftest_result("a", "a-1", True, 1.5)
        n, low = s._note_selftest_result("a", "a-1", True, 6.0)
        self.assertEqual((n, low), (2, False))
        self.go(10, until=lambda: self._units("a"))
        self.assertEqual(self._units("a"), 4)

    def test_avg_cores(self):
        class R: cpu_s = 1200.0; duration_s = 600.0
        class J: timeout_s = 600
        self.assertEqual(self._cfg()._avg_cores(R, J), 2.0)


if __name__ == "__main__":
    unittest.main()
