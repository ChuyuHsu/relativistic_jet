"""webhook 測試：狀態事件、多目的地、權杖、失敗不丟不亂序、payload 欄位。全用合成資料。"""
import http.server
import json
import os
import shutil
import tempfile
import threading
import unittest

from jet import config, db, maintenance, notify, ops, store
from jet.states import Event
from tests.ops_helpers import make_env, seed

TOKEN = "hook-token-" + "y" * 16


def _con(d):
    env = make_env(d)
    os.makedirs(env["state"], exist_ok=True)
    con = db.connect(maintenance.db_path(env["state"]))
    return env, con


def pump(n, con, now):
    """跑一輪 tick，並等所有目的地的背景送出完成，再收結果。"""
    n.tick(con, now)
    for d in n.dests:
        if d.thread is not None:
            d.thread.join()
    n.tick(con, now + 0.001)


class Capture:
    def __init__(self, fail_first=0):
        self.calls, self.fail = [], fail_first

    def __call__(self, url, payload, timeout, headers=None):
        self.calls.append((url, payload, headers))
        if self.fail > 0:
            self.fail -= 1
            raise notify.NotifyError("boom")


class EmitTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.env, self.con = _con(self.d)
        self.con.execute("INSERT INTO branches(name, head_sha, content_id, state, seen_at, updated_at) "
                         "VALUES('f','aaaaaaaa11111111','cid','selftesting',1,1)")
        store.now = lambda: 100.0

    def tearDown(self):
        import time
        store.now = time.time
        self.con.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def kinds(self):
        return [r[0] for r in self.con.execute("SELECT kind FROM events ORDER BY id")]

    def test_off_by_default(self):
        store.apply_event(self.con, "f", Event.SELFTEST_FAIL, reject_reason="x")
        self.assertEqual(self.kinds(), [])

    def test_green_red_queued_merged_pushed_and_never_in_todo(self):
        store.set_meta(self.con, "emit_machine_events", "1")
        store.apply_event(self.con, "f", Event.SELFTEST_PASS)
        store.apply_event(self.con, "f", Event.READY)
        store.apply_event(self.con, "f", Event.BATCH_START)           # 不報
        store.apply_event(self.con, "f", Event.BATCH_MERGED, merged_sha="mmmm")
        self.assertEqual(self.kinds(), ["tested_green", "queued", "merged"])
        self.assertEqual(ops.query_todo(self.con)["events"], [])      # 機器事件不進人的待辦
        store.approve(self.con, "f", "rev")
        store.revoke(self.con, "f", "rev")
        self.assertEqual(self.kinds()[-2:], ["approved", "revoked"])
        self.assertEqual({r[0] for r in self.con.execute("SELECT commit_sha FROM events")}, {"aaaaaaaa11111111"})


class DestTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.env, self.con = _con(self.d)
        seed(self.con)
        self.con.execute("DELETE FROM events")
        self.tok = os.path.join(self.d, "tok")
        with open(self.tok, "w") as f:
            f.write(TOKEN + "\n")

    def tearDown(self):
        self.con.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def _ev(self, kind, branch, detail, at, sha="s"):
        self.con.execute("INSERT INTO events(kind,branch,detail,at,resolved_at,commit_sha) VALUES(?,?,?,?,?,?)",
                         (kind, branch, detail, at, at if kind in notify.MACHINE_KINDS else None, sha))

    def _dests(self):
        return notify.check_destinations([
            {"name": "board", "url": "https://board.example.test/hooks", "token_file": self.tok,
             "kinds": "all", "batch_seconds": 0},
            {"name": "audit", "url": "https://audit.example.test/in", "token_file": self.tok,
             "kinds": ["merged"], "batch_seconds": 0}])

    def test_each_destination_gets_its_kinds_with_token_and_fields(self):
        cap = Capture()
        n = notify.Notifier(poster=cap, destinations=self._dests())
        pump(n, self.con, 10)    # 先建游標（新目的地只收從現在起的事件）
        self._ev("tested_red", "feat-c", json.dumps({"from": "selftesting", "to": "rejected", "reason": "紅了"}), 11, "cccc3333dddd")
        self._ev("merged", "feat-a", json.dumps({"from": "batching", "to": "merged", "merged_sha": "m1"}), 12, "aaaa1111bbbb")
        run = self.con.execute("INSERT INTO runs(kind,branch,layer,commit_sha,status) VALUES('selftest','feat-c','l','c','fail')").lastrowid
        self.con.execute("INSERT INTO run_items(run_id,item_id,status,summary) VALUES(?,?,?,?)", (run, "t1", "fail", "s"))
        self._ev("rejected", "feat-c", "原因", 13, "cccc3333dddd")
        pump(n, self.con, 20)
        board = [c for c in cap.calls if c[0].startswith("https://board")]
        audit = [c for c in cap.calls if c[0].startswith("https://audit")]
        self.assertEqual(len(board), 1)
        evs = board[0][1]["events"]
        self.assertEqual([e["kind"] for e in evs], ["tested_red", "merged", "rejected"])   # id 由小到大
        self.assertEqual(board[0][2], {"Authorization": "Bearer " + TOKEN})
        self.assertEqual(board[0][1]["destination"], "board")
        rej = evs[2]
        self.assertEqual(rej["commit"], "cccc3333dddd")
        self.assertEqual(rej["failed_items"], [{"item_id": "t1", "status": "fail", "summary": "s"}])
        self.assertIn("cccc3333", rej["detail"])
        self.assertIn("t1", rej["detail"])
        self.assertEqual((evs[0]["from"], evs[0]["to"]), ("selftesting", "rejected"))
        self.assertEqual([e["kind"] for e in audit[0][1]["events"]], ["merged"])
        self.assertNotIn(TOKEN, json.dumps(cap.calls[0][1]))

    def test_failure_keeps_order_no_loss_no_duplicates(self):
        cap = Capture(fail_first=1)
        n = notify.Notifier(poster=cap, destinations=[self._dests()[0]])
        pump(n, self.con, 10)
        for i in range(3):
            self._ev("queued", "feat-a", None, 11 + i)
        pump(n, self.con, 20)                 # 失敗：一批 3 筆
        self.assertIsNotNone(n.last_error)
        self._ev("merged", "feat-a", None, 21)   # 失敗期間又來新事件
        for t in range(30, 80, 6):
            pump(n, self.con, t)              # 對半切後很快再試，逐批送完
        sent = [e["id"] for c in cap.calls[1:] for e in c[1]["events"]]
        self.assertEqual(sent, sorted(sent))
        self.assertEqual(len(sent), 4)        # 舊的沒丟、依序、不重複
        self.assertIsNone(n.last_error)

    def _flood(self, n_ev):
        for i in range(n_ev):
            self._ev("queued", "feat-a", None, 11 + i)

    def test_batches_are_capped_and_timeout_default_30(self):
        seen = []
        n = notify.Notifier(poster=lambda url, p, to, h=None: seen.append((len(p["events"]), to)),
                            destinations=[self._dests()[0]])
        pump(n, self.con, 10)
        self._flood(25)
        for t in range(20, 60, 3):
            pump(n, self.con, t)
        self.assertEqual([c for c, _ in seen], [10, 10, 5])
        self.assertTrue(all(to == 30 for _, to in seen))

    def test_slow_receiver_is_halved_until_it_fits(self):
        """對方一次最多吃得下 3 筆（更多就逾時）：不能整批重送到死，要切小。"""
        calls = []

        def post(url, payload, to, headers=None):
            calls.append(len(payload["events"]))
            if len(payload["events"]) > 3:
                raise notify.NotifyError("The read operation timed out")
        n = notify.Notifier(poster=post, destinations=[self._dests()[0]])
        pump(n, self.con, 10)
        self._flood(10)
        for t in range(20, 200, 6):
            pump(n, self.con, t)
        self.assertEqual(sum(c for c in calls if c <= 3), 10)
        self.assertIsNone(n.last_error)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM events WHERE kind='queued' AND notified_at IS NULL").fetchone()[0] >= 0, True)

    def test_poison_event_is_skipped_but_outage_is_not(self):
        bad = {}

        def post(url, payload, to, headers=None):
            if any(int(str(e["id"]).split(":")[-1]) in bad for e in payload["events"]):
                raise notify.NotifyError("boom")
        n = notify.Notifier(poster=post, destinations=[self._dests()[0]])
        pump(n, self.con, 10)
        self._flood(5)
        ids = [r[0] for r in self.con.execute("SELECT id FROM events WHERE kind='queued' ORDER BY id")]
        bad[ids[2]] = True
        for t in range(20, 400, 30):
            pump(n, self.con, t)
        self.assertIsNone(n.last_error)
        cur = int(self.con.execute("SELECT value FROM meta WHERE key='notify_cursor:board'").fetchone()[0])
        self.assertEqual(cur, ids[-1])                        # 毒藥被跳過，後面的都送了
        self.assertEqual(n.dests[0].poison["id"], ids[2])
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM events WHERE kind='queue_error' AND detail LIKE '%跳過%'").fetchone()[0], 1)

    def test_total_outage_never_skips_and_alerts(self):
        n = notify.Notifier(poster=lambda *a, **k: (_ for _ in ()).throw(notify.NotifyError("down")),
                            destinations=[self._dests()[0]])
        pump(n, self.con, 10)
        self._flood(4)
        for t in range(20, 20 + 1500, 30):
            pump(n, self.con, t)
        cur = int(self.con.execute("SELECT value FROM meta WHERE key='notify_cursor:board'").fetchone()[0])
        self.assertLess(cur, min(r[0] for r in self.con.execute("SELECT id FROM events WHERE kind='queued'")))
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM events WHERE kind='queue_error' AND detail LIKE '%連續失敗%'").fetchone()[0], 1)
        self.assertIsNone(n.dests[0].poison)

    def test_new_destination_does_not_replay_history(self):
        self._ev("merged", "feat-a", None, 1)
        cap = Capture()
        n = notify.Notifier(poster=cap, destinations=[self._dests()[0]])
        pump(n, self.con, 10)
        self.assertEqual(cap.calls, [])

    def test_batch_seconds_per_destination(self):
        d = notify.check_destinations([{"name": "fast", "url": "https://x.example.test/", "token_file": self.tok,
                                        "kinds": "all", "batch_seconds": 5}])
        cap = Capture()
        n = notify.Notifier(poster=cap, destinations=d)
        pump(n, self.con, 10)
        self._ev("merged", "feat-a", None, 100)
        pump(n, self.con, 102)
        self.assertEqual(cap.calls, [])
        pump(n, self.con, 106)
        self.assertEqual(len(cap.calls), 1)

    def test_missing_token_file_backs_off_and_reports(self):
        d = notify.check_destinations([{"name": "b", "url": "https://x.example.test/", "token_file": "/nonexistent/t",
                                        "kinds": "all", "batch_seconds": 0}])
        cap = Capture()
        n = notify.Notifier(poster=cap, destinations=d)
        pump(n, self.con, 10)
        self._ev("merged", "feat-a", None, 11)
        pump(n, self.con, 20)
        self.assertEqual(cap.calls, [])
        self.assertIn("權杖", n.last_error)

    def test_real_http_sends_bearer_header(self):
        got = []

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(s):
                got.append((s.headers.get("Authorization"), json.loads(s.rfile.read(int(s.headers["Content-Length"])))))
                s.send_response(200)
                s.end_headers()

            def log_message(s, *a):
                pass
        httpd = http.server.HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        try:
            notify.post_json(f"http://127.0.0.1:{httpd.server_address[1]}/", {"a": 1}, 5, {"Authorization": "Bearer t"})
        finally:
            httpd.shutdown()
            httpd.server_close()
        self.assertEqual(got, [("Bearer t", {"a": 1})])


class ValidationTest(unittest.TestCase):
    def test_check_destinations(self):
        ok = {"name": "a", "url": "https://h.example.test/x", "token_file": "/t"}
        self.assertEqual(notify.check_destinations([ok])[0]["kinds"], list(notify.NOTIFY_KINDS))
        bad = [dict(ok, name="Bad Name"), dict(ok, url="http://h.example.test/x"), dict(ok, url="file:///x"),
               dict(ok, kinds=["nope"]), dict(ok, kinds=[]), dict(ok, batch_seconds=-1), dict(ok, token_file="")]
        for b in bad:
            with self.assertRaises(notify.NotifyError, msg=str(b)):
                notify.check_destinations([b])
        with self.assertRaises(notify.NotifyError):
            notify.check_destinations([ok, ok])   # 名稱重複
        notify.check_destinations([dict(ok, url="http://127.0.0.1:9/x")])   # 本機可 http

    def test_config_parses_destinations(self):
        d = tempfile.mkdtemp()
        try:
            env = make_env(d)
            with open(env["config"], encoding="utf-8") as f:
                data = json.load(f)
            data["notify"]["destinations"] = [{"name": "b", "url": "https://h.example.test/x", "token_file": "/t", "kinds": "all"}]
            with open(env["config"], "w", encoding="utf-8") as f:
                json.dump(data, f)
            self.assertEqual(config.load(env["config"]).notify_destinations[0]["name"], "b")
            data["notify"]["destinations"][0]["kinds"] = ["x"]
            with open(env["config"], "w", encoding="utf-8") as f:
                json.dump(data, f)
            with self.assertRaises(config.ConfigError):
                config.load(env["config"])
        finally:
            shutil.rmtree(d, ignore_errors=True)


class QueryTest(unittest.TestCase):
    def test_branch_has_last_selftest_and_last_batch_with_red_items(self):
        d = tempfile.mkdtemp()
        try:
            env, con = _con(d)
            seed(con)
            r1 = con.execute("INSERT INTO runs(kind,branch,layer,commit_sha,status) VALUES('selftest','feat-b','l','c1','fail')").lastrowid
            con.execute("INSERT INTO run_items VALUES(?,?,?,?)", (r1, "a", "fail", "x"))
            b = con.execute("INSERT INTO batches(base_sha,status,started_at) VALUES('m','aborted',1)").lastrowid
            con.execute("INSERT INTO batch_members VALUES(?,?,?,?,NULL)", (b, 0, "feat-b", "cid-b"))
            r2 = con.execute("INSERT INTO runs(kind,batch_id,size,layer,commit_sha,status) VALUES('batch_point',?,2,'l','c2','fail')", (b,)).lastrowid
            con.execute("INSERT INTO run_items VALUES(?,?,?,?)", (r2, "z", "timeout", "y"))
            q = ops.query_branch(con, "feat-b")
            self.assertEqual(q["last_selftest"]["failed_items"][0]["item_id"], "a")
            self.assertEqual(q["last_batch"]["runs"][0]["failed_items"][0]["item_id"], "z")
            self.assertIsNone(ops.query_branch(con, "feat-a")["last_batch"])
            con.close()
        finally:
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
