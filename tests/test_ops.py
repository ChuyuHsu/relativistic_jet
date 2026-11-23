import os, tempfile, unittest
from jet import db, ops


def fresh(t):
    from tests.ops_helpers import seed
    con = db.connect(os.path.join(t, "a.sqlite"))
    seed(con)
    return con


class ReadTest(unittest.TestCase):
    def test_status_and_branches(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            s = ops.query_status(con, now=2000.0)
            self.assertEqual(s["queue_len"], 1)
            self.assertEqual(s["counts"]["rejected"], 1)
            self.assertFalse(s["paused"])
            bs = {b["name"]: b for b in ops.query_branches(con)}
            self.assertTrue(bs["feat-a"]["approved"])
            self.assertFalse(bs["feat-b"]["approved"])
            self.assertEqual(ops.query_branches(con, "rejected")[0]["name"], "feat-c")

    def test_branch_detail_and_missing(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            d = ops.query_branch(con, "feat-a")
            self.assertEqual(d["children"], ["feat-b"])
            with self.assertRaises(ops.OpError) as cm:
                ops.query_branch(con, "nope")
            self.assertEqual(cm.exception.code, 404)

    def test_todo_metrics_merged(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            con.execute("INSERT INTO events(kind, branch, detail, at) VALUES('rejected','feat-c','x',1)")
            td = ops.query_todo(con)
            self.assertEqual(len(td["events"]), 1)
            self.assertEqual(td["waiting"][0]["name"], "feat-c")
            m = ops.query_metrics(con, now=2000.0)
            self.assertEqual(m["merged_24h"], 0)
            self.assertEqual(ops.query_merged(con), [])


class WriteTest(unittest.TestCase):
    def test_approve_revoke_cycle_and_audit(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            sha = con.execute("SELECT head_sha FROM branches WHERE name='feat-b'").fetchone()[0]
            for bad in (None, "abc", "0" * 40):   # 沒帶／太短／不是目前的版本
                with self.assertRaises(ops.OpError) as cm:
                    ops.apply(con, "approve", {"branch": "feat-b", "sha": bad}, "t")
                self.assertIn(cm.exception.code, (400, 409))
            self.assertEqual(con.execute("SELECT count(*) FROM approvals WHERE branch='feat-b'").fetchone()[0], 0)
            r = ops.apply(con, "approve", {"branch": "feat-b", "sha": sha, "by": "rev"}, "t", now=3000.0)
            self.assertTrue(r["approved"])
            self.assertTrue(ops.apply(con, "approve", {"branch": "feat-b", "sha": sha[:8]}, "t")["already"])
            r = ops.apply(con, "revoke", {"branch": "feat-b"}, "t", now=3001.0)
            self.assertEqual(r["state"], "selftest_green")   # 排隊中撤回→回自測綠
            self.assertGreaterEqual(con.execute("SELECT count(*) FROM audit").fetchone()[0], 3)

    def test_priority_validation(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            self.assertEqual(ops.apply(con, "priority", {"branch": "feat-a", "value": 5}, "t")["priority"], 5)
            for bad in ("5", True, 99999, None):
                with self.assertRaises(ops.OpError):
                    ops.apply(con, "priority", {"branch": "feat-a", "value": bad}, "t")
            # 被拒的也留 audit
            n = con.execute("SELECT count(*) FROM audit WHERE action='priority:rejected'").fetchone()[0]
            self.assertEqual(n, 4)

    def test_branch_name_validation(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            for bad in ("../x", "a b", "", "x;rm", None, 5):
                with self.assertRaises(ops.OpError):
                    ops.apply(con, "approve", {"branch": bad}, "t")

    def test_set_parent_rules(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            with self.assertRaises(ops.OpError):
                ops.apply(con, "set-parent", {"branch": "feat-a", "parent": "feat-a"}, "t")
            with self.assertRaises(ops.OpError):   # a→b 而 b 的父已是 a：會繞成圈
                ops.apply(con, "set-parent", {"branch": "feat-a", "parent": "feat-b"}, "t")
            r = ops.apply(con, "set-parent", {"branch": "feat-b", "parent": None}, "t")
            self.assertIsNone(r["parent"])
            self.assertEqual(r["state"], "pending")   # 測試底變了：排隊中的要回待自測重測
            self.assertEqual(con.execute("SELECT parent FROM branches WHERE name='feat-b'").fetchone()[0], None)

    def test_adhoc_run_dedup_and_validation(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            r = ops.apply(con, "adhoc-run", {"branch": "feat-a", "layer": "gate", "only": "item_x"}, "t")
            r2 = ops.apply(con, "adhoc-run", {"branch": "feat-a", "layer": "gate"}, "t")
            self.assertEqual(r["run_id"], r2["run_id"])
            for bad in ({"layer": "weird"}, {"only": "a b;c"}):
                with self.assertRaises(ops.OpError):
                    ops.apply(con, "adhoc-run", dict({"branch": "feat-a"}, **bad), "t")

    def test_pause_resume_and_halted(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            ops.apply(con, "pause", {}, "t")
            self.assertTrue(ops.query_status(con)["paused"])
            ops.apply(con, "resume", {}, "t")
            self.assertFalse(ops.query_status(con)["paused"])
            with self.assertRaises(ops.OpError):
                ops.apply(con, "resume", {"branch": "feat-a"}, "t")   # 不是暫停狀態
            con.execute("UPDATE branches SET state='halted', fail_count=2 WHERE name='feat-c'")
            self.assertEqual(ops.apply(con, "resume", {"branch": "feat-c"}, "t")["state"], "pending")

    def test_resolve(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            con.execute("INSERT INTO events(kind, branch, detail, at) VALUES('halted','feat-c','x',1)")
            ops.apply(con, "resolve", {"id": 1}, "t")
            with self.assertRaises(ops.OpError):
                ops.apply(con, "resolve", {"id": 1}, "t")

    def test_unknown_action_and_queue(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            with self.assertRaises(ops.OpError):
                ops.apply(con, "drop-table", {}, "t")
            wq = ops.WriteQueue()
            sha = con.execute("SELECT head_sha FROM branches WHERE name='feat-b'").fetchone()[0]
            p = wq.submit("approve", {"branch": "feat-b", "sha": sha}, "q")
            self.assertFalse(p.done.is_set())   # 沒 drain 前不會執行
            wq.drain(con)
            self.assertTrue(p.done.is_set() and p.error is None)
            bad = wq.submit("approve", {"branch": "nope", "sha": sha}, "q")
            wq.drain(con)
            self.assertEqual(bad.error.code, 404)

    def test_readonly_connection_cannot_write(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as t:
            fresh(t)
            ro = ops.open_readonly(os.path.join(t, "a.sqlite"))
            with self.assertRaises(sqlite3.OperationalError):
                ro.execute("UPDATE branches SET state='merged'")


if __name__ == "__main__":
    unittest.main()


class RevokeLogTest(unittest.TestCase):
    def test_revoke_aborts_when_log_fails(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)

            def boom(action, args, now):
                raise OSError("disk full")
            with self.assertRaises(OSError):
                ops.apply(con, "revoke", {"branch": "feat-a"}, "t", pre_commit=boom)
            self.assertEqual(con.execute("SELECT count(*) FROM approvals WHERE branch='feat-a' AND revoked_at IS NULL").fetchone()[0], 1)
            seen = []
            ops.apply(con, "revoke", {"branch": "feat-a"}, "t", pre_commit=lambda a, b, n: seen.append(a))
            self.assertEqual(seen, ["revoke"])
            self.assertEqual(con.execute("SELECT count(*) FROM approvals WHERE branch='feat-a' AND revoked_at IS NULL").fetchone()[0], 0)


class ShortHeadTest(unittest.TestCase):
    def test_empty_head_sha_never_matches(self):
        with tempfile.TemporaryDirectory() as t:
            con = fresh(t)
            con.execute("UPDATE branches SET head_sha='' WHERE name='feat-b'")
            with self.assertRaises(ops.OpError) as cm:
                ops.apply(con, "approve", {"branch": "feat-b", "sha": "deadbeefdead"}, "t")
            self.assertEqual(cm.exception.code, 409)
