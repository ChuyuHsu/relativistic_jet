"""JET-21：squashed_tips 挑的共同祖先算出撞檔時，再用普通 merge-base 算一次（某張疊卡誤判撞檔）。"""
import unittest

from jet.gitops import GitRepo


def mk(squash_tree, plain_tree):
    r = GitRepo.__new__(GitRepo)
    r.squashed_tips = frozenset({"SQ"})
    r._is_ancestor = lambda a, b: False
    r._merge_base_for = lambda b, t: "SQ"
    r._merge_base = lambda b, t: "PLAIN"
    r._merge_tree = lambda tip, b, mb: squash_tree if mb == "SQ" else plain_tree
    r._out = lambda *a, **k: "sha"
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
