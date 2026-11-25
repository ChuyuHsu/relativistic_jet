"""JET-22：父卡改版後，子卡疊在父卡更早的尖端上，不算過期；舊尖端交給 git 層當共同祖先（等於 rebase --onto）。"""
import unittest

from jet import store
from tests.test_scheduler import Base


class OldParentTipTest(Base):
    def _pair(self):
        s = self.mk(workflow={"mode": "direct_merge"})
        self.repo.add("p")
        self.repo.add("c")
        self.go(2)
        self.con.execute("UPDATE branches SET parent='p' WHERE name='c'")
        return s

    def _heads(self, name):
        return {"p": store.get_branch(self.con, "p")["head_sha"], "c": store.get_branch(self.con, "c")["head_sha"]}[name]

    def test_child_on_old_parent_tip_is_not_stale(self):
        s = self._pair()
        self.con.execute("INSERT INTO events(kind,branch,detail,at,commit_sha) VALUES('pushed','p','x',1.0,'oldtip')")
        self.repo.is_ancestor = lambda a, b: a == "oldtip"          # 現在的父尖端不是子的祖先，只有舊尖端是
        s._anc_cache = {}
        self.assertFalse(s._stale_parent(store.get_branch(self.con, "c")))

    def test_child_without_any_parent_tip_is_still_stale(self):
        s = self._pair()
        self.repo.is_ancestor = lambda a, b: False
        s._anc_cache = {}
        self.assertTrue(s._stale_parent(store.get_branch(self.con, "c")))

    def test_old_tip_missing_from_repo_is_skipped(self):
        s = self._pair()
        self.con.execute("INSERT INTO events(kind,branch,detail,at,commit_sha) VALUES('pushed','p','x',1.0,'gone')")

        def anc(a, b):
            if a == "gone":
                raise RuntimeError("bad object")
            return False
        self.repo.is_ancestor = anc
        s._anc_cache = {}
        self.assertTrue(s._stale_parent(store.get_branch(self.con, "c")))

    def test_squashed_tips_include_parent_history(self):
        s = self._pair()
        self.con.execute("INSERT INTO events(kind,branch,detail,at,commit_sha) VALUES('pushed','p','x',1.0,'oldtip')")
        self.repo.squashed_tips = frozenset()
        s._sync_squashed_tips()
        self.assertIn("oldtip", self.repo.squashed_tips)


if __name__ == "__main__":
    unittest.main()
