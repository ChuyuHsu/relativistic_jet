import os, tempfile, unittest
from jet import db, store
from jet.states import State


def mk(con, name, state="pending", parent=None):
    con.execute("INSERT INTO branches(name,head_sha,content_id,parent,state,updated_at) VALUES(?,?,?,?,?,0)",
                (name, "s", "c-" + name, parent, state))


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.con = db.connect(os.path.join(self.t.name, "a.sqlite"))
    def tearDown(self): self.t.cleanup()

    def test_approve_revoke(self):
        mk(self.con, "a")
        self.assertFalse(store.is_approved(self.con, "a", "c-a"))
        store.approve(self.con, "a", "tester")
        self.assertTrue(store.is_approved(self.con, "a", "c-a"))
        self.assertFalse(store.is_approved(self.con, "a", "other-content"))
        store.revoke(self.con, "a")
        self.assertFalse(store.is_approved(self.con, "a", "c-a"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM audit").fetchone()[0], 2)

    def test_parent_cycle_rejected(self):
        mk(self.con, "a"); mk(self.con, "b", parent="a")
        with self.assertRaises(ValueError):
            store.set_parent(self.con, "a", "b")
        with self.assertRaises(ValueError):
            store.set_parent(self.con, "a", "a")

    def test_set_parent_sends_back(self):
        mk(self.con, "p"); mk(self.con, "c", state="queued")
        store.set_parent(self.con, "c", "p")
        self.assertEqual(store.get_branch(self.con, "c")["state"], State.PENDING.value)

    def test_baseline_and_pause(self):
        self.assertIsNone(store.get_baseline(self.con, "m1", "gate"))
        store.set_baseline(self.con, "m1", "gate", ["x", "y"])
        self.assertEqual(store.get_baseline(self.con, "m1", "gate"), {"x", "y"})
        store.set_paused(self.con, True)
        self.assertTrue(store.is_paused(self.con))


if __name__ == "__main__":
    unittest.main()
