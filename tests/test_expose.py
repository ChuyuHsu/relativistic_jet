"""對外開放測試：假通道指令＋本機假登記伺服器，全程合成資料。"""
import http.server
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from jet import config, expose
from tests.ops_helpers import Running, make_env, seed

TOKEN = "tok-" + "x" * 20


class FakeRegistry:
    def __init__(self):
        self.calls = []
        self.fail = 0           # 接下來這麼多次回 500
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def do_PUT(s):
                body = json.loads(s.rfile.read(int(s.headers["Content-Length"])))
                outer.calls.append({"path": s.path, "auth": s.headers.get("Authorization"), "url": body.get("url")})
                if outer.fail > 0:
                    outer.fail -= 1
                    s.send_response(500)
                else:
                    s.send_response(200)
                s.end_headers()

            def log_message(s, *a):
                pass
        self.httpd = http.server.HTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/relay/demo"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def wait_for(cond, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


class ExposeTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.tok = os.path.join(self.d, "tok")
        with open(self.tok, "w") as f:
            f.write(TOKEN + "\n")
        self.reg = FakeRegistry()
        self.counter = os.path.join(self.d, "n")
        # 每次啟動印一個不同的網址（第 n 次啟動 → n.example.test），然後睡覺
        self.script = (
            "import os,sys,time\n"
            f"p={self.counter!r}\n"
            "n=int(open(p).read())+1 if os.path.exists(p) else 1\n"
            "open(p,'w').write(str(n))\n"
            "print('starting',flush=True)\n"
            "print('your url is https://t%d.example.test ok'%n,flush=True)\n"
            "time.sleep(60)\n")
        self.old = (expose.RESTART_BASE_S, expose.BACKOFF_BASE_S)
        expose.RESTART_BASE_S, expose.BACKOFF_BASE_S = 0.1, 0.2

    def tearDown(self):
        expose.RESTART_BASE_S, expose.BACKOFF_BASE_S = self.old
        self.reg.close()
        shutil.rmtree(self.d, ignore_errors=True)

    def _ex(self, reregister_s=600):
        return expose.Exposer([sys.executable, "-c", self.script], r"https://\S+\.example\.test",
                              self.reg.url, self.tok, reregister_s, poll_s=0.05)

    def test_registers_url_with_bearer_token(self):
        ex = self._ex()
        ex.start()
        try:
            self.assertTrue(wait_for(lambda: self.reg.calls))
            c = self.reg.calls[0]
            self.assertEqual((c["path"], c["auth"], c["url"]), ("/relay/demo", "Bearer " + TOKEN, "https://t1.example.test"))
        finally:
            ex.stop()

    def test_tunnel_death_restarts_and_reregisters(self):
        ex = self._ex()
        ex.start()
        try:
            self.assertTrue(wait_for(lambda: self.reg.calls))
            ex._proc.kill()
            self.assertTrue(wait_for(lambda: any(c["url"] == "https://t2.example.test" for c in self.reg.calls)))
            self.assertGreaterEqual(ex.restarts, 1)
            self.assertTrue(any("重啟" in m for m in ex.drain_events()))
        finally:
            ex.stop()

    def test_register_failure_backs_off_retries_and_reports(self):
        self.reg.fail = 2
        ex = self._ex()
        ex.start()
        try:
            self.assertTrue(wait_for(lambda: len(self.reg.calls) >= 3))
            self.assertTrue(wait_for(lambda: ex.registered_url == "https://t1.example.test"))
            ev = ex.drain_events()
            self.assertTrue(any("失敗" in m for m in ev))
            self.assertTrue(any("恢復" in m for m in ev))
            self.assertFalse(any(TOKEN in m for m in ev))   # 回報裡不能有權杖
        finally:
            ex.stop()

    def test_periodic_reregister(self):
        ex = self._ex(reregister_s=10)
        ex.reregister_s = 0.3   # 只有測試縮短（設定檔下限是 10 秒）
        ex.start()
        try:
            self.assertTrue(wait_for(lambda: len(self.reg.calls) >= 3))
            self.assertEqual({c["url"] for c in self.reg.calls}, {"https://t1.example.test"})
        finally:
            ex.stop()

    def test_stop_kills_tunnel(self):
        ex = self._ex()
        ex.start()
        self.assertTrue(wait_for(lambda: ex._proc is not None and ex.url))
        p = ex._proc
        ex.stop()
        self.assertIsNotNone(p.poll())

    def test_bad_settings_rejected(self):
        ok = (["x"], r"https://\S+", "http://127.0.0.1:1/x", "/t", 60)
        expose.check_settings(*ok)
        for i, bad in ((0, []), (1, "("), (2, "file:///x"), (3, ""), (4, 1)):
            args = list(ok)
            args[i] = bad
            with self.assertRaises(expose.ExposeError):
                expose.check_settings(*args)

    def test_non_http_match_ignored(self):
        ex = expose.Exposer([sys.executable, "-c", "print('ftp://x.example.test',flush=True);import time;time.sleep(5)"],
                            r"\S+://\S+", self.reg.url, self.tok, 60, poll_s=0.05)
        ex.start()
        try:
            time.sleep(0.5)
            self.assertEqual(self.reg.calls, [])
        finally:
            ex.stop()


class ConfigAndServerTest(unittest.TestCase):
    def test_config_block_optional_and_validated(self):
        d = tempfile.mkdtemp()
        try:
            env = make_env(d)
            self.assertIsNone(config.load(env["config"]).expose)
            with open(env["config"], encoding="utf-8") as f:
                data = json.load(f)
            data["expose"] = {"tunnel_command": ["x"], "url_pattern": "(", "register_url": "http://h/x",
                              "register_token_file": "/t"}
            with open(env["config"], "w", encoding="utf-8") as f:
                json.dump(data, f)
            with self.assertRaises(config.ConfigError):
                config.load(env["config"])
            data["expose"]["url_pattern"] = r"https://\S+"
            with open(env["config"], "w", encoding="utf-8") as f:
                json.dump(data, f)
            self.assertEqual(config.load(env["config"]).expose["reregister_seconds"], 600)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_server_runs_exposer_and_writes_events_in_main(self):
        d = tempfile.mkdtemp()
        reg = FakeRegistry()
        try:
            env = make_env(d)
            tok = os.path.join(d, "tok")
            with open(tok, "w") as f:
                f.write(TOKEN)
            with open(env["config"], encoding="utf-8") as f:
                data = json.load(f)
            data["expose"] = {"tunnel_command": [sys.executable, "-c",
                                                 "print('https://a.example.test',flush=True);import time;time.sleep(60)"],
                              "url_pattern": r"https://\S+", "register_url": reg.url,
                              "register_token_file": tok}
            with open(env["config"], "w", encoding="utf-8") as f:
                json.dump(data, f)
            with Running(env) as r:
                self.assertTrue(wait_for(lambda: reg.calls))
                self.assertEqual(reg.calls[0]["url"], "https://a.example.test")
            self.assertIsNotNone(r.srv.exposer._thread)
            self.assertFalse(r.srv.exposer._thread.is_alive())   # 排隊器結束時一起收掉
        finally:
            reg.close()
            shutil.rmtree(d, ignore_errors=True)


class ApiExposureTest(unittest.TestCase):
    def _get(self, url, key=None):
        req = urllib.request.Request(url)
        if key:
            req.add_header("X-Jet-Read-Key", key)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    def test_all_data_needs_key_and_no_local_paths_or_keys_leak(self):
        d = tempfile.mkdtemp()
        try:
            env = make_env(d)
            from jet import db, maintenance
            os.makedirs(env["state"], exist_ok=True)
            con = db.connect(maintenance.db_path(env["state"]))
            seed(con)
            con.execute("UPDATE branches SET reject_reason=? WHERE name='feat-c'",
                        ("fail at %s/x with %s" % (env["state"], env["read_key"]),))
            con.close()
            with Running(env) as r:
                for p in ("/status", "/branches", "/branches/feat-a", "/merged", "/todo", "/metrics"):
                    self.assertEqual(self._get(r.url + p)[0], 401, p)
                    self.assertEqual(self._get(r.url + p, "wrong-key-wrong-key")[0], 401, p)
                code, body = self._get(r.url + "/branches", env["read_key"])
                self.assertEqual(code, 200)
                self.assertNotIn(env["state"], body)
                self.assertNotIn(env["read_key"], body)
                # 頁面本身只是外殼：不含資料、路徑、鑰匙
                code, page = self._get(r.url + "/")
                self.assertEqual(code, 200)
                self.assertNotIn(env["state"], page)
                self.assertNotIn("feat-a", page)
        finally:
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
