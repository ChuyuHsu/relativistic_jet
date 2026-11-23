"""上一代遺留的測試行程：領頭已死、子行程還活著也要認得出來並清掉；別人的行程不碰。"""
import os
import subprocess
import tempfile
import time
import unittest

from jet.cores import own_leftovers, kill_leftovers


class ReapTest(unittest.TestCase):
    def test_finds_and_kills_only_own(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as other:
            wt = os.path.join(root, "wt", "abc")
            os.makedirs(wt)
            mine = subprocess.Popen(["sleep", "60"], cwd=wt, start_new_session=True)
            theirs = subprocess.Popen(["sleep", "60"], cwd=other, start_new_session=True)
            try:
                found = {p.pid for p, _ in own_leftovers(root)}
                self.assertIn(mine.pid, found)
                self.assertNotIn(theirs.pid, found)
                self.assertNotIn(os.getpid(), found)
                self.assertEqual({p.pid for p, _ in own_leftovers(root, keep_pgids=[mine.pid])} & {mine.pid}, set())
                self.assertEqual(kill_leftovers([x for x in own_leftovers(root) if x[0].pid == mine.pid]), 1)
                mine.wait(5)
                self.assertIsNone(theirs.poll())
            finally:
                for p in (mine, theirs):
                    p.kill()
                    p.wait()

    def test_leader_dead_child_alive(self):
        with tempfile.TemporaryDirectory() as root:
            wt = os.path.join(root, "wt")
            os.makedirs(wt)
            r = subprocess.run(["setsid", "sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $!"], cwd=wt,
                               capture_output=True, text=True)
            pid = int(r.stdout.strip())
            try:
                time.sleep(0.2)
                self.assertIn(pid, {p.pid for p, _ in own_leftovers(root)})
                kill_leftovers(own_leftovers(root))
                time.sleep(0.3)
                self.assertEqual([p for p, _ in own_leftovers(root)], [])
            finally:
                try:
                    os.kill(pid, 9)
                except OSError:
                    pass
