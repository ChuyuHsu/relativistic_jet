"""自測排序加等待時間：每等 60 分鐘升一級，HOTFIX 仍最先、手動 prio_override 最優先。"""
import unittest

from tests.test_scheduler import Base


def row(name, seen_at, override=0):
    return {"name": name, "seen_at": seen_at, "prio_override": override}


class AgingOrderTest(Base):
    def _keys(self, s, rows, now, prios):
        s._board_prio = lambda n: prios.get(n, float("inf"))
        return [r["name"] for r in sorted(rows, key=lambda r: s._selftest_order_key(r, now))]

    def test_old_card_overtakes_newer_smaller_number(self):
        s = self.mk()
        now = 100 * 3600
        rows = [row("new", now - 60), row("old", now - 30 * 3600)]
        # new 優先序 5、old 優先序 20：old 等了 30 小時升 30 級（20-30=-10）超過 new（5）
        self.assertEqual(self._keys(s, rows, now, {"new": 5, "old": 20}), ["old", "new"])
        # 沒等多久就不插隊
        rows2 = [row("new", now - 60), row("old", now - 3 * 3600)]
        self.assertEqual(self._keys(s, rows2, now, {"new": 5, "old": 20}), ["new", "old"])

    def test_hotfix_always_first_and_override_beats_all(self):
        s = self.mk()
        now = 100 * 3600
        rows = [row("dev/hotfix-x", now - 60), row("old", now - 500 * 3600), row("dev/manual", now - 60, override=5)]
        self.assertEqual(self._keys(s, rows, now, {"dev/hotfix-x": 50, "old": 1, "dev/manual": 99}), ["dev/manual", "dev/hotfix-x", "old"])

    def test_no_board_priority_sorts_last_but_ages(self):
        s = self.mk()
        now = 2000 * 3600
        rows = [row("noprio", now - 60), row("withprio", now - 60)]
        self.assertEqual(self._keys(s, rows, now, {"withprio": 100}), ["withprio", "noprio"])
        rows = [row("noprio", now - 1000 * 3600), row("withprio", now - 60)]
        self.assertEqual(self._keys(s, rows, now, {"withprio": 100}), ["noprio", "withprio"])


if __name__ == "__main__":
    unittest.main()
