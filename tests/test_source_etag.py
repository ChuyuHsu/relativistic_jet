"""JET-04：拉清單帶 etag（If-None-Match），304 沿用上次清單；推 main 前的確認維持無條件讀取。"""
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from jet.source import SourcePoller


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        st = self.server.state
        st["seen"].append(self.headers.get("If-None-Match"))
        if st["status"] is not None:
            self.send_response(st["status"]); self.end_headers(); return
        etag = st["etag"]
        if etag and self.headers.get("If-None-Match") == etag:
            self.send_response(304); self.send_header("ETag", etag); self.end_headers(); return
        body = json.dumps(st["data"]).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        if etag:
            self.send_header("ETag", etag)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class SourceEtagTest(unittest.TestCase):
    def setUp(self):
        self.srv = HTTPServer(("127.0.0.1", 0), Handler)
        self.srv.state = {"etag": 'W/"1"', "status": None, "seen": [], "data": [{"branch": "a"}]}
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.now = [100.0]
        self.p = SourcePoller(f"http://127.0.0.1:{self.srv.server_port}/x", poll_seconds=0, timeout=5,
                              clock=lambda: self.now[0], threaded=False)

    def tearDown(self):
        self.srv.shutdown(); self.srv.server_close()

    def poll(self):
        self.now[0] += 1
        return self.p.poll()

    def test_second_poll_sends_etag_and_keeps_list(self):
        self.assertIsNone(self.poll())
        self.assertTrue(self.p.new_data)
        self.p.new_data = False
        ver = self.p.drops_ver
        first = self.p.entries
        self.assertIsNone(self.poll())
        self.assertEqual(self.srv.state["seen"], [None, 'W/"1"'])
        self.assertFalse(self.p.new_data)                  # 304 不當新資料
        self.assertEqual(self.p.drops_ver, ver)
        self.assertIs(self.p.entries, first)
        self.assertEqual(self.p.ok_at, self.now[0])        # 但「最後成功時間」更新了
        self.assertEqual(self.p.age(), 0)

    def test_changed_etag_gets_new_list(self):
        self.poll(); self.p.new_data = False
        self.srv.state.update(etag='W/"2"', data=[{"branch": "a"}, {"branch": "b"}])
        self.poll()
        self.assertTrue(self.p.new_data)
        self.assertEqual([e["branch"] for e in self.p.entries], ["a", "b"])
        self.poll()
        self.assertEqual(self.srv.state["seen"][-1], 'W/"2"')

    def test_server_without_etag_unchanged_behaviour(self):
        self.srv.state["etag"] = None
        self.poll(); self.p.new_data = False
        self.poll()
        self.assertEqual(self.srv.state["seen"], [None, None])
        self.assertTrue(self.p.new_data)                   # 每次都當新資料，跟以前一樣

    def test_refresh_now_is_unconditional(self):
        self.poll(); self.p.new_data = False
        self.assertIsNone(self.p.refresh_now())
        self.assertIsNone(self.srv.state["seen"][-1])
        self.assertTrue(self.p.new_data)
        self.poll()                                         # 之後恢復條件式
        self.assertEqual(self.srv.state["seen"][-1], 'W/"1"')

    def test_error_keeps_old_list(self):
        self.poll()
        first = self.p.entries
        self.srv.state["status"] = 500
        self.assertIn("500", self.poll())
        self.assertIs(self.p.entries, first)
        self.assertIsNotNone(self.p.error)


if __name__ == "__main__":
    unittest.main()
