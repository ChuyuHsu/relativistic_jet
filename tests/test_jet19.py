"""jet loop-reset：結束進行中的背景二分（清 loop_bisect），沒有二分時回錯誤，audit 留紀錄。"""
import json
import unittest

from jet import ops, store
from tests.test_scheduler import Base


class LoopResetTest(Base):
    def test_clears_bisect_and_audits(self):
        self.mk()
        store.set_meta(self.con, "loop_bisect", json.dumps({"phase": "verify", "lo": 1, "hi": 5, "cands": ["a"] * 6}))
        store.set_meta(self.con, "loop_green", "g1")
        r = ops.apply(self.con, "loop-reset", {}, "tester")
        self.assertTrue(r["loop_bisect_cleared"])
        self.assertEqual(store.get_meta(self.con, "loop_bisect"), "")
        self.assertEqual(store.get_meta(self.con, "loop_green"), "g1")          # 不動綠基準
        self.assertTrue(any(x["action"] == "loop-reset" and x["actor"] == "tester"
                            for x in self.con.execute("SELECT action, actor FROM audit")))

    def test_without_bisect_is_error(self):
        self.mk()
        with self.assertRaises(ops.OpError):
            ops.apply(self.con, "loop-reset", {}, "tester")


if __name__ == "__main__":
    unittest.main()
