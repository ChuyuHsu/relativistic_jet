"""JET-21：squashed_tips 挑的共同祖先算出撞檔時，再用普通 merge-base 算一次（某張疊卡誤判撞檔）。"""
import unittest

from jet.gitops import GitRepo


def mk(squash_tree, plain_tree):
    r = GitRepo.__new__(GitRepo)
    r.squashed_tips = frozenset({"SQ"})
    r._is_ancestor = lambda a, b: False
    r._merge_base_for = lambda b, t, extra=(): "SQ"
    r.old_tips = {}
    r._merge_base = lambda b, t: "PLAIN"
    r._merge_tree = lambda tip, b, mb: squash_tree if mb == "SQ" else plain_tree
    r._out = lambda *a, **k: "TT" if a and a[0] == "rev-parse" else "sha"
    return r


class RetryPlainBaseTest(unittest.TestCase):
    def test_stack_keeps_when_plain_base_is_clean(self):
        r = mk(None, "TREE")
        self.assertEqual(r.stack("main", [("n", "b")]).kept, [0])

    def test_stack_conflict_when_both_conflict(self):
        r = mk(None, None)
        self.assertEqual(r.stack("main", [("n", "b")]).kept, [])

    def test_conflicts_with(self):
        self.assertFalse(mk(None, "TREE").conflicts_with("b", "main"))
        self.assertTrue(mk(None, None).conflicts_with("b", "main"))
        self.assertFalse(mk("TREE", None).conflicts_with("b", "main"))


if __name__ == "__main__":
    unittest.main()


class EmptySquashTest(unittest.TestCase):
    def _repo(self, rev_list, tips, merge_tree, tree_of_tip="TT"):
        r = GitRepo.__new__(GitRepo)
        r.squashed_tips = frozenset(tips)
        r.old_tips = {}
        r._is_ancestor = lambda a, b: False

        class R:
            returncode = 0
            stdout = " ".join(rev_list)
        r._git = lambda *a, **k: R()
        r._merge_base = lambda a, b: "PLAIN"
        r._merge_tree = merge_tree
        r._out = lambda *a, **k: tree_of_tip if a and a[0] == "rev-parse" else "sha"
        return r

    def test_own_head_is_never_the_merge_base(self):
        r = self._repo(["HEAD1", "OLD"], {"HEAD1", "OLD"}, lambda *a: "T")
        self.assertEqual(r._merge_base_for("HEAD1", "main"), "OLD")          # 自己的尖端跳過，找下一個
        r2 = self._repo(["HEAD1"], {"HEAD1"}, lambda *a: "T")
        self.assertEqual(r2._merge_base_for("HEAD1", "main"), "PLAIN")

    def test_empty_squash_is_not_kept_and_reported(self):
        r = self._repo(["H"], set(), lambda tip, b, mb: "TT")                # 不論哪個共同祖先，壓出來都跟 tip 的 tree 一樣
        res = r.stack("main", [("n", "H")])
        self.assertEqual((res.kept, res.empty), ([], [0]))

    def test_empty_with_tip_base_falls_back_to_plain_base(self):
        r = self._repo(["H", "OLD"], {"OLD"}, lambda tip, b, mb: "TT" if mb == "OLD" else "NEW")
        res = r.stack("main", [("n", "H")])
        self.assertEqual((res.kept, res.empty), ([0], []))
