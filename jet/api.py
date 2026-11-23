"""HTTP 查詢／操作介面（DESIGN 第 7 節）。

讀（X-Jet-Read-Key）：GET /status /branches /branches/<name> /merged /todo /metrics
人看的進度頁：GET /（頁面本身不含任何資料，資料用讀鑰匙另外抓）
寫（X-Jet-Write-Key）：POST /approve /revoke /priority /set-parent /adhoc-run /pause /resume /resolve /stop

規矩：
- 讀、寫鑰匙分開：讀鑰匙只能讀；寫鑰匙可以寫，也可以讀。鑰匙只放檔案，不放設定值。
- 預設只聽 127.0.0.1（設定檔 api.listen）；要不要對外開由部署的人決定。
- 讀用唯讀連線，不會寫資料庫。寫入一律放進 WriteQueue，由排隊器主行程執行（這裡沒有寫入者），
  執行時主行程寫 audit。
"""
import hmac
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

from . import ops
from .page import PAGE_HTML

READ_ROUTES = ["/status", "/branches", "/branches/<name>", "/merged", "/todo", "/metrics", "/"]
WRITE_ROUTES = ["/approve", "/revoke", "/priority", "/set-parent", "/adhoc-run", "/pause", "/resume", "/resolve", "/stop", "/resume-docs", "/main-red-ack"]

MAX_BODY = 64 * 1024          # 請求內容上限，擋掉亂塞
WRITE_WAIT_S = 10.0           # 等主行程處理的時間；超過就回 202，命令仍在佇列裡


class KeyError_(Exception):
    pass


def read_key_file(path: Optional[str], what: str) -> str:
    """讀鑰匙檔；沒設、讀不到、或空的都拒絕啟動（預設安全，不提供「免鑰匙」模式）。"""
    if not path:
        raise KeyError_(f"設定檔沒有 {what} 鑰匙檔路徑")
    try:
        with open(path, encoding="utf-8") as f:
            key = f.read().strip()
    except OSError as e:
        raise KeyError_(f"讀不到 {what} 鑰匙檔：{e.strerror or e}")
    if len(key) < 16:
        raise KeyError_(f"{what} 鑰匙太短（至少 16 字元）")
    return key


def _same(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def parse_listen(listen: str) -> Tuple[str, int]:
    host, _, port = listen.rpartition(":")
    if not host or not port.isdigit() or not 0 <= int(port) <= 65535:
        raise ValueError(f"api.listen 格式要是 主機:埠：{listen}")
    return host.strip("[]"), int(port)


class ApiServer:
    """把 HTTP 服務包起來：start() 開執行緒、stop() 關。"""

    def __init__(self, db_path: str, wq: ops.WriteQueue, read_key: str, write_key: str,
                 listen: str = "127.0.0.1:0", hide: Optional[list] = None):
        if _same(read_key, write_key):
            raise KeyError_("讀、寫鑰匙不能相同")
        self.db_path, self.wq = db_path, wq
        self._read_key, self._write_key = read_key, write_key
        # 回應裡不能出現的字串（本機路徑、鑰匙）：對外開放時就算測試輸出夾帶也會被遮掉
        self._hide = sorted({h for h in (hide or []) + [read_key, write_key] if isinstance(h, str) and len(h) >= 4},
                            key=len, reverse=True)
        host, port = parse_listen(listen)
        handler = self._make_handler()
        self.httpd = ThreadingHTTPServer((host, port), handler)
        self.httpd.daemon_threads = True
        self.thread: Optional[threading.Thread] = None

    @property
    def address(self) -> Tuple[str, int]:
        return self.httpd.server_address[0], self.httpd.server_address[1]

    def start(self) -> None:
        self.thread = threading.Thread(target=self.httpd.serve_forever, name="jet-api", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        if self.thread:
            self.thread.join(timeout=5)

    # ------------------------------------------------------------ 內部

    def _role(self, headers) -> Optional[str]:
        """回 'write'、'read' 或 None。"""
        w = headers.get("X-Jet-Write-Key")
        if w is not None and _same(w, self._write_key):
            return "write"
        r = headers.get("X-Jet-Read-Key")
        if r is not None and _same(r, self._read_key):
            return "read"
        return None

    def _make_handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "jet"
            sys_version = ""
            timeout = 15   # 連線太久沒動就斷，擋慢速連線佔著不放

            def log_message(self, fmt, *args):   # 不把請求寫到 stderr（鑰匙雖然在標頭，仍不留）
                pass

            # ---- 小工具
            def _send(self, code: int, body: bytes, ctype: str) -> None:
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                self.wfile.write(body)

            def _json(self, code: int, obj: Any) -> None:
                text = json.dumps(obj, ensure_ascii=False)
                for h in server._hide:
                    if h in text:
                        text = text.replace(h, "[已遮蔽]")
                self._send(code, text.encode("utf-8"), "application/json; charset=utf-8")

            def _err(self, code: int, msg: str) -> None:
                self._json(code, {"error": msg})

            # ---- 讀
            def do_GET(self):
                url = urlparse(self.path)
                path = url.path.rstrip("/") or "/"
                if path == "/":
                    return self._send(200, PAGE_HTML.encode("utf-8"), "text/html; charset=utf-8")
                role = server._role(self.headers)
                if role is None:
                    return self._err(401, "需要鑰匙")
                try:
                    con = ops.open_readonly(server.db_path)
                except Exception:
                    return self._err(503, "資料庫暫時打不開")
                try:
                    q = parse_qs(url.query)
                    if path == "/status":
                        return self._json(200, ops.query_status(con))
                    if path == "/branches":
                        return self._json(200, ops.query_branches(con, (q.get("state") or [None])[0]))
                    m = re.match(r"^/branches/(.+)$", path)
                    if m:
                        return self._json(200, ops.query_branch(con, unquote(m.group(1))))
                    if path == "/merged":
                        lim = (q.get("limit") or ["50"])[0]
                        return self._json(200, ops.query_merged(con, int(lim) if lim.isdigit() else 50))
                    if path == "/todo":
                        return self._json(200, ops.query_todo(con))
                    if path == "/metrics":
                        return self._json(200, ops.query_metrics(con))
                    return self._err(404, "沒有這個路徑")
                except ops.OpError as e:
                    return self._err(e.code, str(e))
                except Exception:
                    return self._err(500, "內部錯誤")
                finally:
                    con.close()

            # ---- 寫
            def do_POST(self):
                path = (urlparse(self.path).path.rstrip("/") or "/").lstrip("/")
                if "/" + path not in WRITE_ROUTES:
                    return self._err(404, "沒有這個路徑")
                if server._role(self.headers) != "write":
                    return self._err(401, "需要寫入鑰匙")
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    return self._err(400, "Content-Length 不對")
                if n > MAX_BODY:
                    return self._err(413, "內容太大")
                raw = self.rfile.read(n) if n else b"{}"
                try:
                    args = json.loads(raw.decode("utf-8") or "{}")
                except (ValueError, UnicodeDecodeError):
                    return self._err(400, "內容不是合法 JSON")
                if not isinstance(args, dict):
                    return self._err(400, "內容必須是 JSON 物件")
                actor = f"http:{self.client_address[0]}"
                try:
                    pending = server.wq.submit(path, args, actor)
                except ops.OpError as e:
                    return self._err(e.code, str(e))
                if not pending.wait(WRITE_WAIT_S):
                    return self._json(202, {"queued": True, "note": "主行程還沒處理，命令仍在佇列裡"})
                if pending.error is not None:
                    return self._err(pending.error.code, str(pending.error))
                return self._json(200, pending.result)

        return Handler
