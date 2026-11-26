"""孤兒測試行程：work_dir 之外的測試暫存目錄也要認得；排隊器定期掃，預設只回報、設定打開才殺。"""
import os
import subprocess
import tempfile
import time
import unittest

from jet import config, store
from jet.cores import own_leftovers
from tests.test_scheduler import Base


class _Spawn:
    def __init__(self, cwd):
        r = subprocess.run(["setsid", "sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $!"], cwd=cwd, capture_output=True, text=True)
        self.pid = int(r.stdout.strip())
        time.sleep(0.2)

    def alive(self):
        try:
            st = open(f"/proc/{self.pid}/stat").read().split(")")[-1].split()[0]
            return st not in ("Z", "X")
        except OSError:
            return False

    def kill(self):
        try:
            os.kill(self.pid, 9)
        except OSError:
            pass


class OrphanTest(Base):
    def test_multiple_roots(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            p = _Spawn(b)
            try:
                self.assertNotIn(p.pid, {x.pid for x, _ in own_leftovers(a)})
                self.assertIn(p.pid, {x.pid for x, _ in own_leftovers([a, b])})
            finally:
                p.kill()

    def test_extra_root_needs_pinned_to_pool(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as big:
            p = _Spawn(big)                                          # cwd 在暫存碟底下、沒綁核、父行程 init：使用者自己起的，不能算孤兒
            try:
                allowed = sorted(os.sched_getaffinity(0))
                self.assertNotIn(p.pid, {x.pid for x, _ in own_leftovers(a, extra_roots=[big], pool=allowed)})
                os.sched_setaffinity(p.pid, {allowed[-1]})            # 釘在池內一顆核：才算
                self.assertIn(p.pid, {x.pid for x, _ in own_leftovers(a, extra_roots=[big], pool=allowed)})
                self.assertNotIn(p.pid, {x.pid for x, _ in own_leftovers(a, extra_roots=[big], pool=[])})   # 沒給池就不認額外目錄
            finally:
                p.kill()

    def test_leftover_roots_include_tmpdir(self):
        s = self.mk()
        self.cfg = config.parse({"git": {"remote": "x"}, "paths": {"state_dir": self.t.name + "/st", "work_dir": "/w"},
                                 "cores": {"pool": "0-3", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 1},
                                 "commands": {"test": ["t"]}, "env": {"TMPDIR": "/big/tmp"}})
        self.assertEqual(config.leftover_roots(self.cfg), ("/w",))
        self.assertEqual(config.leftover_extra(self.cfg), ("/big/tmp",))

    def test_reports_once_and_kills_only_when_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            s = self.mk(env={"TMPDIR": tmp})                         # 先建排隊器（它啟動時會清上一代遺留），再生出孤兒
            p = _Spawn(tmp)
            os.sched_setaffinity(p.pid, {sorted(os.sched_getaffinity(0))[-1]})
            s.cfg.pool = sorted(os.sched_getaffinity(0))
            try:
                s._orphan_step()
                s._orphan_step()                                   # 同一組 pid 不重複送（也還沒到間隔）
                ev = self.con.execute("SELECT detail FROM events WHERE kind='orphan_found'").fetchall()
                self.assertEqual(len(ev), 1)
                self.assertIn(str(p.pid), ev[0][0])
                self.assertTrue(p.alive())                         # 預設不殺
                s._orphan_at = 0
                s.cfg.kill_orphans = True
                s._orphan_step()
                time.sleep(0.3)
                self.assertFalse(p.alive())
                self.assertEqual(len(self.con.execute("SELECT 1 FROM events WHERE kind='orphan_killed'").fetchall()), 1)
            finally:
                p.kill()


if __name__ == "__main__":
    unittest.main()
