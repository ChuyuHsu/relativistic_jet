import unittest
from jet.batch import ladder, judge, GREEN, RED, PENDING


class LadderTest(unittest.TestCase):
    def test_full(self): self.assertEqual(ladder(16), [2, 4, 8, 16])
    def test_small(self): self.assertEqual(ladder(1), [1])
    def test_odd(self): self.assertEqual(ladder(5), [2, 4, 5])
    def test_zero(self): self.assertEqual(ladder(0), [])


class JudgeTest(unittest.TestCase):
    def test_wait(self):
        self.assertEqual(judge(4, {2: GREEN}).state, "wait")

    def test_all_green(self):
        v = judge(4, {2: GREEN, 4: GREEN})
        self.assertEqual((v.state, v.merge_upto, v.culprit), ("done", 4, None))

    def test_adjacent_red(self):
        v = judge(4, {2: GREEN, 4: RED})
        self.assertEqual(v.state, "probe")
        self.assertEqual(v.probe, [3])
        v = judge(4, {2: GREEN, 4: RED}, {3: RED})
        self.assertEqual((v.merge_upto, v.culprit), (2, 3))
        v = judge(4, {2: GREEN, 4: RED}, {3: GREEN})
        self.assertEqual((v.merge_upto, v.culprit), (3, 4))

    def test_first_point_red_is_direct(self):
        v = judge(2, {2: RED})
        self.assertEqual(v.state, "probe")
        v = judge(2, {2: RED}, {1: RED})
        self.assertEqual((v.merge_upto, v.culprit), (0, 1))

    def test_single(self):
        v = judge(1, {1: RED})
        self.assertEqual((v.state, v.merge_upto, v.culprit), ("done", 0, 1))

    def test_two_culprits_one_per_round(self):
        # 16 條，第 3、10 條紅：點 2 綠、4/8/16 紅 → 查 3
        r = {2: GREEN, 4: RED, 8: RED, 16: RED}
        self.assertEqual(judge(16, r).probe, [3])
        v = judge(16, r, {3: RED})
        self.assertEqual((v.merge_upto, v.culprit), (2, 3))


if __name__ == "__main__":
    unittest.main()
