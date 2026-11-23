"""E 串（介面與維運）測試共用的合成環境：全部是假資料，不碰任何真實專案。"""
import json
import os
import threading
import time

from jet import db, ops, serve


def make_env(tmp: str, listen: str = "127.0.0.1:0", notify_url=None) -> dict:
    state = os.path.join(tmp, "state")
    keys = os.path.join(tmp, "keys")
    os.makedirs(keys)
    rk, wk = os.path.join(keys, "read"), os.path.join(keys, "write")
    serve.init_keys(rk, wk)
    cfg = {
        "git": {"remote": "synthetic.git"},
        "paths": {"state_dir": state, "work_dir": os.path.join(tmp, "work")},
        "cores": {"pool": "0-3"},
        "commands": {"test": ["true"]},
        "notify": {"url": notify_url, "batch_seconds": 0},
        "api": {"listen": listen, "read_key_file": rk, "write_key_file": wk},
    }
    path = os.path.join(tmp, "jet.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f)
    return {"config": path, "state": state, "rk": rk, "wk": wk, "cfg": cfg,
            "read_key": open(rk).read().strip(), "write_key": open(wk).read().strip()}


def seed(con, now: float = 1000.0) -> None:
    """三條合成分支：a 自測綠（已放行）、b 排隊（疊在 a 上）、c 退回。"""
    rows = [
        ("feat-a", "aaaaaaaa11111111", "cid-a", None, "selftest_green", None),
        ("feat-b", "bbbbbbbb22222222", "cid-b", "feat-a", "queued", now - 50),
        ("feat-c", "cccccccc33333333", "cid-c", None, "rejected", None),
    ]
    for name, sha, cid, parent, state, q in rows:
        con.execute("INSERT INTO branches(name, head_sha, content_id, parent, state, queued_at, updated_at, seen_at)"
                    " VALUES(?,?,?,?,?,?,?,?)", (name, sha, cid, parent, state, q, now, now))
    con.execute("UPDATE branches SET reject_reason='synthetic red' WHERE name='feat-c'")
    con.execute("INSERT INTO approvals(branch, content_id, by, at) VALUES('feat-a','cid-a','t',?)", (now,))


class Running:
    """在背景跑主迴圈（Server），測完關掉。"""

    def __init__(self, env: dict, scheduler_factory=None):
        self.env, self.factory = env, scheduler_factory
        self.srv = None
        self._ready = threading.Event()
        self._err = None
        self.t = threading.Thread(target=self._main, daemon=True)

    def _main(self):
        # 資料庫連線只能在建立它的執行緒用，所以整個 Server 在背景執行緒裡建、跑、關
        try:
            self.srv = serve.Server(self.env["config"], self.factory)
        except BaseException as e:
            self._err = e
            self._ready.set()
            return
        self._ready.set()
        self.srv.run_forever()

    def __enter__(self):
        self.t.start()
        self._ready.wait(10)
        if self._err:
            raise self._err
        host, port = self.srv.api.address
        self.url = f"http://{host}:{port}"
        return self

    def __exit__(self, *a):
        self.srv.stop()
        self.t.join(timeout=10)
