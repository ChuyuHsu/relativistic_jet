"""JET-02：定期送現況快照（state_snapshot），送不出去不影響調度。"""
import json
import unittest

from jet import config, notify, store
from tests.test_scheduler import Base


def snaps(con):
    return [json.loads(r[0]) for r in con.execute("SELECT detail FROM events WHERE kind='state_snapshot' ORDER BY id")]


class SnapshotTest(Base):
    def setUp(self):
        self.now = [1000.0]

    def mk2(self, **over):
        s = self.mk(**over)
        s.clock = lambda: self.now[0]
        return s

    def test_first_tick_then_every_interval(self):
        s = self.mk2()
        self.repo.add("a")
        s.tick()
        self.assertEqual([x["at"] for x in snaps(self.con)], [1000.0])
        s.tick(); self.now[0] += 299; s.tick()
        self.assertEqual([x["at"] for x in snaps(self.con)], [1000.0])      # 還沒到間隔
        self.now[0] += 2; s.tick()
        self.assertEqual([x["at"] for x in snaps(self.con)], [1301.0])      # 新的取代舊的（只留最新一筆）

    def test_content_and_merged_excluded(self):
        s = self.mk2()
        self.repo.add("a"); self.repo.add("b")
        s.tick(); self.approve_all("a")
        self.go(until=lambda: "a" in self.merged() or self.state("a") in ("queued", "merged"))
        self.now[0] += 400
        s.tick()
        d = snaps(self.con)[-1]
        by = {b["branch"]: b for b in d["branches"]}
        if self.state("a") == "merged":
            self.assertIn("a", by)                          # 剛合入的要列（看板的合併事件萬一沒收到，靠快照對帳）
            self.assertEqual(by["a"]["state"], "merged")
        self.assertIn("b", by)
        b = by["b"]
        self.assertEqual(set(b), {"branch", "state", "commit", "approved_sha", "retest"})
        self.assertEqual(b["commit"], "b-1")
        self.assertEqual(d["at"], self.now[0])

    def test_old_merged_not_listed(self):
        s = self.mk2()
        self.repo.add("a")
        s.tick(); self.approve_all("a")
        self.go(until=lambda: "a" in self.merged())
        self.now[0] += 400
        s.tick()
        rec = {b["branch"]: b for b in snaps(self.con)[-1]["branches"]}
        self.assertEqual(rec["a"]["state"], "merged")             # 剛合入：列在快照裡
        self.assertEqual(rec["a"]["commit"], "a-1")
        self.con.execute("UPDATE branches SET updated_at=? WHERE name='a'", (store.now() - 3 * 3600,))
        self.now[0] += 400
        s.tick()
        self.assertNotIn("a", [b["branch"] for b in snaps(self.con)[-1]["branches"]])

    def test_disabled_with_zero(self):
        s = self.mk2(notify={"snapshot_seconds": 0})
        self.repo.add("a")
        s.tick(); self.now[0] += 10000; s.tick()
        self.assertEqual(snaps(self.con), [])

    def test_interval_configurable_and_validated(self):
        s = self.mk2(notify={"snapshot_seconds": 60})
        s.tick(); self.now[0] += 61; s.tick()
        self.assertEqual([x["at"] for x in snaps(self.con)], [1061.0])
        with self.assertRaises(config.ConfigError):
            self.mk(notify={"snapshot_seconds": -1})

    def test_payload_carries_branches(self):
        s = self.mk2()
        self.repo.add("a")
        s.tick()
        row = self.con.execute("SELECT * FROM events WHERE kind='state_snapshot'").fetchone()
        e = notify._enrich(self.con, row)
        p = notify.build_payload([e], 1.0)["events"][0]
        self.assertEqual(p["kind"], "state_snapshot")
        self.assertEqual(p["branches"][0]["branch"], "a")
        self.assertIn("1 條分支", p["detail"])

    def test_failure_does_not_break_tick(self):
        s = self.mk2()
        self.repo.add("a")
        orig = store.emit
        store.emit = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")) if a[1] == "state_snapshot" else orig(*a, **k)
        try:
            s.tick()
        finally:
            store.emit = orig
        self.assertIsNotNone(self.state("a"))              # 調度照常登記分支


if __name__ == "__main__":
    unittest.main()
