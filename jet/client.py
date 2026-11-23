"""命令列用的小客戶端：用標準庫打排隊器的 HTTP 介面。寫入也是走 HTTP，所以命令列不是另一個寫入者。"""
import json
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple


class ClientError(Exception):
    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class Client:
    def __init__(self, base_url: str, read_key: Optional[str] = None, write_key: Optional[str] = None,
                 timeout: float = 20.0):
        self.base = base_url.rstrip("/")
        self.read_key, self.write_key, self.timeout = read_key, write_key, timeout

    def _do(self, req: urllib.request.Request) -> Tuple[int, Any]:
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return r.status, json.loads(r.read().decode("utf-8") or "null")
        except urllib.error.HTTPError as e:
            try:
                body = json.loads(e.read().decode("utf-8"))
                msg = body.get("error", str(body))
            except Exception:
                msg = e.reason
            raise ClientError(f"{e.code}：{msg}", e.code)
        except (urllib.error.URLError, OSError) as e:
            raise ClientError(f"連不上排隊器（{getattr(e, 'reason', e)}）")

    def get(self, path: str) -> Any:
        key = self.read_key or self.write_key
        if not key:
            raise ClientError("沒有讀取鑰匙")
        req = urllib.request.Request(self.base + path, headers={"X-Jet-Read-Key": key})
        return self._do(req)[1]

    def post(self, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        if not self.write_key:
            raise ClientError("沒有寫入鑰匙")
        req = urllib.request.Request(self.base + path, data=json.dumps(body or {}).encode("utf-8"),
                                     method="POST", headers={"X-Jet-Write-Key": self.write_key,
                                                             "Content-Type": "application/json"})
        return self._do(req)[1]
