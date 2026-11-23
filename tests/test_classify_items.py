"""分類指令在被測專案的檢出裡跑（相對路徑以專案根為準）；紅項清單帶到事件。"""
import contextlib, os, stat, tempfile, unittest

from jet import notify, store
from tests.fakes import FakeRepo
from tests.test_scheduler import Base

SCRIPT = """#!/usr/bin/env python3
import json, sys
files = open(sys.argv[1]).read().split()
print(json.dumps({"verdict": "docs_only" if all(f.startswith("docs/") for f in files) else "test"}))
"""


class CheckoutRepo(FakeRepo):
    """worktree 給一個真的目錄（裡面有專案的分類腳本），模擬「被測專案的檢出」。"""
    def __init__(self, root):
        super().__init__()
        self.root = root
        self.opened = 0

    @contextlib.contextmanager
    def worktree(self, sha):
        if ">" not in sha and sha == self.main:
            self.opened += 1
            yield self.root
        else:
            yield sha


class ClassifyCwd(Base):
    def setup_repo(self, script=True):
        self.proj = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(self.proj, True))
        os.makedirs(os.path.join(self.proj, "tools"))
        if script:
            p = os.path.join(self.proj, "tools", "classify.py")
            open(p, "w").write(SCRIPT)
        self.mk(commands={"test": ["t"], "classify": ["python3", "tools/classify.py", "{files}"]})
        self.repo = CheckoutRepo(self.proj)
        self.s.repo = self.repo
        self.ex.repo = self.repo

    def test_relative_classify_runs_in_project_checkout(self):
        self.setup_repo()
        self.repo.add("d", files=("docs/x.md",))
        self.repo.add("c", files=("src/a.py",))
        self.s.tick()
        self.assertEqual(store.get_branch(self.con, "d")["docs_only"], 1)    # 相對路徑找得到
        self.assertEqual(store.get_branch(self.con, "c")["docs_only"], 0)
        d1 = self.s._cls_dir
        self.s.tick()
        self.assertEqual(self.s._cls_dir, d1)                                 # main 沒換就共用同一份檢出
        self.assertEqual(self.con.execute("SELECT count(*) FROM events WHERE kind='queue_error'").fetchone()[0], 0)

    def test_classify_failure_is_reported_once_and_treated_as_test(self):
        self.setup_repo(script=False)                                         # 指令找不到（rc=2）
        self.repo.add("d", files=("docs/x.md",))
        self.repo.add("e", files=("docs/y.md",))
        self.s.tick()
        self.assertEqual(store.get_branch(self.con, "d")["docs_only"], 0)     # 不當純文件
        ev = [r["detail"] for r in self.con.execute("SELECT detail FROM events WHERE kind='queue_error'")]
        self.assertEqual(len([d for d in ev if "分類指令" in d]), 1)          # 只記一次


class FailedItemsInEvents(Base):
    def test_rejected_event_carries_failed_items_array(self):
        self.mk()
        store.set_meta(self.con, "emit_machine_events", "1")
        self.repo.add("a", reds={"t1", "t2"})
        self.go(5)
        self.assertEqual(self.state("a"), "rejected")
        rows = self.con.execute("SELECT id,kind,branch,detail,at,commit_sha,approved_sha,retest FROM events "
                                "WHERE branch='a' AND kind IN ('rejected','tested_red')").fetchall()
        self.assertTrue(rows)
        out = notify.build_payload([notify._enrich(self.con, r) for r in rows], 1.0)["events"]
        for e in out:
            self.assertEqual(sorted(x["item_id"] for x in e["failed_items"]), ["t1", "t2"])

    def test_known_red_items_not_listed(self):
        self.mk()
        self.repo.main_red = {"old"}
        self.repo.add("a", reds={"t1"})
        self.go(5)
        r = self.con.execute("SELECT id,kind,branch,detail,at,commit_sha,approved_sha,retest FROM events "
                             "WHERE branch='a' AND kind='rejected'").fetchone()
        e = notify._enrich(self.con, r)
        self.assertEqual([x["item_id"] for x in e["failed_items"]], ["t1"])


if __name__ == "__main__":
    unittest.main()
