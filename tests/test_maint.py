"""維運測試：通知、備份還原、設定重載、自動退版。全用合成資料。"""
import http.server
import json
import os
import shutil
import tempfile
import threading
import unittest

from jet import config, db, guard, maintenance, notify
from tests.ops_helpers import make_env, seed


def _con(env):
    os.makedirs(env["state"], exist_ok=True)
    return db.connect(maintenance.db_path(env["state"]))


class NotifyTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.env = make_env(self.d)
        self.con = _con(self.env)
        seed(self.con)
        self.con.execute("DELETE FROM events")
        self.con.execute("INSERT INTO events(kind, branch, detail, at) VALUES('rejected','x','壞',100)")
        self.con.execute("INSERT INTO events(kind, branch, detail, at) VALUES('merged','y','好',100)")

    def tearDown(self):
        self.con.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def _wait(self, n, now):
        for _ in range(200):
            n.tick(self.con, now)
            if n.dests[0].thread is None:
                return
            n.dests[0].thread.join(0.05)
        self.fail("通知沒完成")

    def test_batch_wait_then_send_only_notify_kinds(self):
        sent = []
        n = notify.Notifier("http://127.0.0.1:1/x", batch_s=180, poster=lambda u, p, t: sent.append(p))
        n.tick(self.con, 150)          # 還沒攢滿
        self.assertEqual(sent, [])
        n.tick(self.con, 300)
        n.dests[0].thread.join()
        n.tick(self.con, 301)          # 收結果
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["count"], 1)
        marked = self.con.execute("SELECT kind FROM events WHERE notified_at IS NOT NULL").fetchall()
        self.assertEqual([r[0] for r in marked], ["rejected"])

    def test_failure_keeps_and_retries_with_backoff(self):
        calls = []

        def poster(u, p, t):
            calls.append(1)
            if len(calls) == 1:
                raise notify.NotifyError("boom")
        n = notify.Notifier("http://127.0.0.1:1/x", batch_s=0, poster=poster)
        n.tick(self.con, 1000)
        n.dests[0].thread.join()
        n.tick(self.con, 1001)
        self.assertIsNotNone(n.last_error)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM events WHERE notified_at IS NOT NULL").fetchone()[0], 0)
        n.tick(self.con, 1010)         # 還在退避
        self.assertEqual(len(calls), 1)
        n.tick(self.con, 1100)
        n.dests[0].thread.join()
        n.tick(self.con, 1101)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM events WHERE notified_at IS NOT NULL").fetchone()[0], 1)

    def test_real_http_post(self):
        got = []

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(s):
                got.append(json.loads(s.rfile.read(int(s.headers["Content-Length"]))))
                s.send_response(200)
                s.end_headers()

            def log_message(s, *a):
                pass
        httpd = http.server.HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        try:
            notify.post_json(f"http://127.0.0.1:{httpd.server_address[1]}/", {"a": 1})
        finally:
            httpd.shutdown()
            httpd.server_close()
        self.assertEqual(got, [{"a": 1}])

    def test_url_whitelist(self):
        with self.assertRaises(notify.NotifyError):
            notify.check_url("file:///etc/passwd")
        self.assertIsNone(notify.check_url(""))


class BackupRestoreTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.env = make_env(self.d)
        self.state = self.env["state"]
        self.con = _con(self.env)
        seed(self.con)

    def tearDown(self):
        try:
            self.con.close()
        except Exception:
            pass
        shutil.rmtree(self.d, ignore_errors=True)

    def test_backup_rotation(self):
        for i in range(5):
            maintenance.make_backup(self.con, self.state, now=1_700_000_000 + i * 3600, keep=3)
        self.assertEqual(len(maintenance.list_backups(self.state)), 3)
        self.assertTrue(maintenance.integrity_ok(maintenance.list_backups(self.state)[0]))

    def test_schedule_interval(self):
        s = maintenance.BackupSchedule(self.state, interval_s=3600)
        self.assertIsNotNone(s.tick(self.con, 10_000_000))
        self.assertIsNone(s.tick(self.con, 10_000_100))
        self.assertIsNotNone(s.tick(self.con, 10_004_000))

    def test_restore_replays_revocation_and_cancels_runs(self):
        t0 = 1_700_000_000.0
        maintenance.make_backup(self.con, self.state, now=t0)
        approved = [r[0] for r in self.con.execute("SELECT branch FROM approvals WHERE revoked_at IS NULL")]
        self.assertTrue(approved)
        b = approved[0]
        # 備份之後撤回：資料庫跟著壞掉，只剩撤回紀錄檔
        maintenance.log_revocation(self.state, b, t0 + 100)
        self.con.close()
        path = maintenance.db_path(self.state)
        with open(path, "wb") as f:
            f.write(b"garbage")
        res = maintenance.restore(self.state, now=t0 + 200)
        self.assertEqual(res["revocations_replayed"], 1)
        self.assertTrue(os.path.exists(res["old_db"]))     # 壞庫改名留著
        con = db.connect(path)
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM approvals WHERE branch=? AND revoked_at IS NULL", (b,)).fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM runs WHERE status IN ('queued','running')").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM branches WHERE state IN ('selftesting','batching')").fetchone()[0], 0)
            self.assertGreaterEqual(con.execute("SELECT COUNT(*) FROM events WHERE kind='queue_error'").fetchone()[0], 1)
        finally:
            con.close()

    def test_restore_refuses_when_lock_held(self):
        lock = maintenance.StateLock(self.state)
        self.assertTrue(lock.acquire())
        try:
            with self.assertRaises(maintenance.MaintenanceError):
                maintenance.restore(self.state)
        finally:
            lock.release()

    def test_restore_without_good_backup(self):
        with self.assertRaises(maintenance.MaintenanceError):
            maintenance.restore(self.state)


class ConfigWatcherTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.env = make_env(self.d)
        self.path = self.env["config"]
        self.cfg = config.load(self.path)

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def _rewrite(self, fn):
        with open(self.path, encoding="utf-8") as f:
            data = json.load(f)
        fn(data)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(data, f)

    def test_unchanged_no_report(self):
        w = maintenance.ConfigWatcher(self.path, self.cfg)
        self.assertEqual(w.poll(), (self.cfg, None))

    def test_broken_file_keeps_old_and_reports_once(self):
        w = maintenance.ConfigWatcher(self.path, self.cfg)
        with open(self.path, "w") as f:
            f.write("{ not json")
        cfg, err = w.poll()
        self.assertIs(cfg, self.cfg)
        self.assertIsNotNone(err)
        self.assertIsNone(w.poll()[1])

    def test_restart_only_field_rejected(self):
        w = maintenance.ConfigWatcher(self.path, self.cfg)
        self._rewrite(lambda d: d["api"].update(listen="127.0.0.1:1"))
        cfg, err = w.poll()
        self.assertIs(cfg, self.cfg)
        self.assertIn("listen", err or "")


class GuardTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        for r in ("v1", "v2"):
            os.makedirs(os.path.join(self.d, "releases", r))

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def test_deploy_and_rollback(self):
        guard.deploy(self.d, "v1")
        res = guard.deploy(self.d, "v2")
        self.assertEqual(res, {"current": "v2", "previous": "v1"})
        self.assertEqual(guard.rollback(self.d)["current"], "v1")
        self.assertEqual(guard.rollback(self.d)["current"], "v2")   # 可以退回來

    def test_deploy_rejects_bad_names(self):
        for bad in ("", "..", "a/b", "nope"):
            with self.assertRaises(guard.GuardError):
                guard.deploy(self.d, bad)

    def test_rollback_without_previous(self):
        guard.deploy(self.d, "v1")
        with self.assertRaises(guard.GuardError):
            guard.rollback(self.d)

    def _sup(self, codes, clock):
        class P:
            def __init__(s, code):
                s.code = code

            def wait(s):
                clock[0] += 5
                return s.code

            def poll(s):
                return s.code
        it = iter(codes)
        return guard.Supervisor(self.d, self.d, lambda r: ["x"], clock=lambda: clock[0],
                                sleeper=lambda s: None, popen=lambda *a, **k: P(next(it)))

    def test_three_crashes_roll_back_then_clean_exit(self):
        guard.deploy(self.d, "v1")
        guard.deploy(self.d, "v2")
        sup = self._sup([1, 1, 1, 0], [0.0])
        self.assertEqual(sup.run(max_starts=10), 0)
        self.assertEqual(guard._target(self.d, "current"), "v1")
        with open(os.path.join(self.d, guard.EVENTS_FILE), encoding="utf-8") as f:
            self.assertIn("退回上一版", f.read())

    def test_stable_run_resets_count(self):
        guard.deploy(self.d, "v1")
        guard.deploy(self.d, "v2")
        clock = [0.0]
        sup = self._sup([1, 1, 0], clock)
        sup.stable_s = 1.0   # 每次都算活得夠久
        self.assertEqual(sup.run(max_starts=10), 0)
        self.assertEqual(guard._target(self.d, "current"), "v2")

    def test_ingest_events_offset(self):
        con = _con(make_env(self.d))
        try:
            guard.add_event(self.d, "queue_error", "a")
            guard.add_event(self.d, "other", "b")
            self.assertEqual(guard.ingest_events(con, self.d), 1)
            self.assertEqual(guard.ingest_events(con, self.d), 0)   # 不重複讀
            guard.add_event(self.d, "queue_error", "c")
            self.assertEqual(guard.ingest_events(con, self.d), 1)
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
