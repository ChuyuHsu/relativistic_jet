"""recheck 在 main 上重跑連續 infra：不無限重排，第 2 次起認定自測紅是這張卡自己的。"""
import unittest
from types import SimpleNamespace as NS

from jet import store
from tests.test_scheduler import Base


class RecheckInfraTest(Base):
    def _setup(self):
        s = self.mk(workflow={"mode": "direct_merge"})
        self.repo.add("a")
        self.go(2)
        self.con.execute("UPDATE branches SET state='selftesting' WHERE name='a'")
        return s

    def _recheck(self, s, status):
        s._rc["a"] = {"approved": (None, 0), "new_red": {"godot_x"}, "mode": "main"}
        self.con.execute("UPDATE branches SET state='selftesting' WHERE name='a'")
        s._on_recheck(NS(branch="a"), NS(status=status, failed_items=[]))

    def test_second_consecutive_infra_rejects_with_reason(self):
        s = self._setup()
        self._recheck(s, "infra")
        self.assertEqual(self.state("a"), "pending")            # 第一次：照舊重排
        self._recheck(s, "infra")
        row = store.get_branch(self.con, "a")
        self.assertEqual(row["state"], "rejected")
        self.assertIn("godot_x", row["reject_reason"])
        self.assertIn("跑不起來", row["reject_reason"])

    def test_non_infra_result_resets_counter(self):
        s = self._setup()
        self._recheck(s, "infra")
        self._recheck(s, "pass")                               # 中間有一次正常結果：計數歸零
        self.con.execute("UPDATE branches SET state='selftesting' WHERE name='a'")
        self._recheck(s, "infra")
        self.assertNotEqual(self.state("a"), "rejected")


if __name__ == "__main__":
    unittest.main()
