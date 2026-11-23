import json, os, tempfile, unittest, urllib.error, urllib.request
from jet import db, maintenance
from tests.ops_helpers import Running, make_env, seed


def call(url, path, key_hdr=None, key=None, body=None, method=None):
    req = urllib.request.Request(url + path, method=method or ("POST" if body is not None else "GET"),
                                 data=None if body is None else json.dumps(body).encode())
    if key_hdr:
        req.add_header(key_hdr, key)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = make_env(self.tmp.name)
        os.makedirs(self.env["state"], exist_ok=True)
        con = db.connect(maintenance.db_path(self.env["state"]))
        seed(con)
        con.close()

    def tearDown(self):
        self.tmp.cleanup()

    def test_auth_separation(self):
        e = self.env
        with Running(e) as r:
            self.assertEqual(call(r.url, "/status")[0], 401)                       # 沒鑰匙
            self.assertEqual(call(r.url, "/status", "X-Jet-Read-Key", "wrong")[0], 401)
            self.assertEqual(call(r.url, "/status", "X-Jet-Read-Key", e["read_key"])[0], 200)
            self.assertEqual(call(r.url, "/approve", "X-Jet-Read-Key", e["read_key"], {"branch": "feat-b"})[0], 401)  # 讀鑰匙不能寫
            self.assertEqual(call(r.url, "/approve", "X-Jet-Write-Key", "wrong", {"branch": "feat-b"})[0], 401)
            self.assertEqual(call(r.url, "/status", "X-Jet-Write-Key", e["write_key"])[0], 200)   # 寫鑰匙也能讀

    def test_page_is_public_and_has_no_data(self):
        with Running(self.env) as r:
            code, body = call(r.url, "/")
            html = body.decode()
            self.assertEqual(code, 200)
            self.assertIn("viewport", html)
            self.assertNotIn("feat-a", html)
            self.assertNotIn(self.env["read_key"], html)
            self.assertNotRegex(html, r"https?://(?!127\.0\.0\.1)")   # 不載外部資源

    def test_reads(self):
        e = self.env
        with Running(e) as r:
            rd = lambda p: json.loads(call(r.url, p, "X-Jet-Read-Key", e["read_key"])[1])
            self.assertEqual(rd("/status")["queue_len"], 1)
            self.assertEqual({b["name"] for b in rd("/branches")}, {"feat-a", "feat-b", "feat-c"})
            self.assertEqual([b["name"] for b in rd("/branches?state=rejected")], ["feat-c"])
            self.assertEqual(rd("/branches/feat-a")["children"], ["feat-b"])
            self.assertEqual(rd("/todo")["waiting"][0]["name"], "feat-c")
            self.assertIn("merged_24h", rd("/metrics"))
            self.assertEqual(rd("/merged"), [])
            self.assertEqual(call(r.url, "/branches/nope", "X-Jet-Read-Key", e["read_key"])[0], 404)
            self.assertEqual(call(r.url, "/nothing", "X-Jet-Read-Key", e["read_key"])[0], 404)

    def test_write_goes_through_main_loop_and_audits(self):
        e = self.env
        with Running(e) as r:
            code, body = call(r.url, "/approve", "X-Jet-Write-Key", e["write_key"], {"branch": "feat-b"})
            self.assertEqual(code, 200, body)
            self.assertTrue(json.loads(body)["approved"])
            code, body = call(r.url, "/revoke", "X-Jet-Write-Key", e["write_key"], {"branch": "feat-b"})
            self.assertEqual(code, 200)
            self.assertEqual(call(r.url, "/priority", "X-Jet-Write-Key", e["write_key"], {"branch": "x y", "value": 1})[0], 400)
            self.assertEqual(call(r.url, "/priority", "X-Jet-Write-Key", e["write_key"], {"branch": "nope", "value": 1})[0], 404)
        os.makedirs(e["state"], exist_ok=True)
        con = db.connect(maintenance.db_path(e["state"]))
        acts = [r[0] for r in con.execute("SELECT action FROM audit ORDER BY id")]
        self.assertEqual(acts[:2], ["approve", "revoke"])
        self.assertIn("priority:rejected", acts)
        self.assertTrue(os.path.exists(os.path.join(e["state"], maintenance.REVOCATION_LOG)))   # 撤回有同步記文字檔

    def test_bad_bodies(self):
        e = self.env
        with Running(e) as r:
            h = ("X-Jet-Write-Key", e["write_key"])
            req = urllib.request.Request(r.url + "/approve", data=b"{not json", method="POST")
            req.add_header(*h)
            with self.assertRaises(urllib.error.HTTPError) as cm:
                urllib.request.urlopen(req, timeout=10)
            self.assertEqual(cm.exception.code, 400)
            self.assertEqual(call(r.url, "/approve", h[0], h[1], [1, 2])[0], 400)       # 不是物件
            big = {"branch": "feat-b", "pad": "x" * 70000}
            self.assertEqual(call(r.url, "/approve", h[0], h[1], big)[0], 413)           # 太大
            self.assertEqual(call(r.url, "/nonsense", h[0], h[1], {})[0], 404)

    def test_binds_loopback_by_default_and_rejects_same_keys(self):
        from jet import api, ops
        with self.assertRaises(api.KeyError_):
            api.ApiServer("x", ops.WriteQueue(), "k" * 20, "k" * 20)
        with Running(self.env) as r:
            self.assertEqual(r.srv.api.address[0], "127.0.0.1")
        with self.assertRaises(ValueError):
            api.parse_listen("no-port")

    def test_key_files_required(self):
        from jet import api
        with self.assertRaises(api.KeyError_):
            api.read_key_file(None, "讀取")
        with self.assertRaises(api.KeyError_):
            api.read_key_file(os.path.join(self.tmp.name, "missing"), "讀取")
        short = os.path.join(self.tmp.name, "short")
        open(short, "w").write("abc")
        with self.assertRaises(api.KeyError_):
            api.read_key_file(short, "讀取")


if __name__ == "__main__":
    unittest.main()
