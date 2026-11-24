"""JET-06：排隊器把已合入分支的尖端交給 git 層。"""
import unittest

from tests.test_scheduler import Base


class SquashedTipsTest(Base):
    def test_tips_follow_merged_branches(self):
        s = self.mk()
        self.repo.squashed_tips = frozenset()
        for n in "ab":
            self.repo.add(n)
        s.tick(); self.approve_all("a", "b")
        self.go(until=lambda: self.merged() == {"a", "b"})
        s.tick()
        self.assertEqual(self.repo.squashed_tips, frozenset(["a-1", "b-1"]))

    def test_repo_without_attribute_is_fine(self):
        s = self.mk()
        del_attr = hasattr(self.repo, "squashed_tips")
        if del_attr:
            del self.repo.squashed_tips
        s.tick()                                          # 沒這個屬性的 repo（舊實作、假 repo）不報錯
        self.assertFalse(hasattr(self.repo, "squashed_tips"))


if __name__ == "__main__":
    unittest.main()
