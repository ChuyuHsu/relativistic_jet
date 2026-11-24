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
        self.assertEqual((v.state, v.merge_upto, v.culprit), ("done", 2, None))   # 先合乾淨的前 2 條
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
        v0 = judge(16, r)                      # 已知前 2 條綠：先合，不等查兇手
        self.assertEqual((v0.state, v0.merge_upto, v0.culprit), ("done", 2, None))
        self.assertEqual(judge(16, {4: RED, 8: RED, 16: RED, 2: RED}).probe, [1])   # 第一點就紅：沒有可先合的，照舊逐條查
        v = judge(16, r, {3: RED})
        self.assertEqual((v.merge_upto, v.culprit), (2, 3))


if __name__ == "__main__":
    unittest.main()


class EarlyTest(unittest.TestCase):
    def test_smaller_points_and_later_points_dont_block(self):
        # 2 重跑中、4 綠、8 紅、16 還在跑：前 4 條先合（探針 5～7 未跑也先合）
        v = judge(16, {2: PENDING, 4: GREEN, 8: RED, 16: PENDING})
        self.assertEqual((v.state, v.merge_upto), ("done", 4))
        v = judge(16, {2: RED, 4: GREEN, 8: RED, 16: RED})
        self.assertEqual((v.state, v.merge_upto), ("done", 4))
