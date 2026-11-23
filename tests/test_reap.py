"""上一代遺留的測試行程：領頭已死、子行程還活著也要認得出來並清掉；別人的行程不碰。"""
import os
import subprocess
import tempfile
import time
import unittest

from jet.cores import own_leftovers, kill_leftovers


class _Pid:
    def __init__(self, pid):
        self.pid = pid

    def kill(self):
        try:
            os.kill(self.pid, 9)
        except OSError:
            pass


class ReapTest(unittest.TestCase):
    def test_finds_and_kills_only_own(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as other:
            wt = os.path.join(root, "wt", "abc")
            os.makedirs(wt)
            r = subprocess.run(["setsid", "sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $!"], cwd=wt,
                               capture_output=True, text=True)
            mine = _Pid(int(r.stdout.strip()))
            time.sleep(0.2)
            theirs = subprocess.Popen(["sleep", "60"], cwd=other, start_new_session=True)
            try:
                found = {p.pid for p, _ in own_leftovers(root)}
                self.assertIn(mine.pid, found)
                self.assertNotIn(theirs.pid, found)
                self.assertNotIn(os.getpid(), found)
                self.assertEqual({p.pid for p, _ in own_leftovers(root, keep_pgids=[os.getpgid(mine.pid)])} & {mine.pid}, set())
                self.assertEqual(kill_leftovers([x for x in own_leftovers(root) if x[0].pid == mine.pid]), 1)
                time.sleep(0.3)
                self.assertFalse(os.path.exists(f"/proc/{mine.pid}") and
                                 open(f"/proc/{mine.pid}/stat").read().split(")")[-1].split()[0] != "Z")
                self.assertIsNone(theirs.poll())
            finally:
                mine.kill()
                theirs.kill()
                theirs.wait()

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


class LiveJobTest(unittest.TestCase):
    def test_live_child_of_running_parent_not_listed(self):
        """現役測試：最上面的行程的親是還活著的排隊器（這裡用測試行程本身頂替），整串不算遺留，
        即使它自己 setsid 脫離了行程群組與工作階段。"""
        with tempfile.TemporaryDirectory() as root:
            wt = os.path.join(root, "wt")
            os.makedirs(wt)
            live = subprocess.Popen(["sh", "-c", "sleep 60 & wait"], cwd=wt, start_new_session=True)
            try:
                time.sleep(0.3)
                self.assertEqual(own_leftovers(root), [])
                # 領頭 sh 一旦死掉，sleep 被 init 收養 → 才算遺留
                subprocess.run(["pkill", "-9", "-P", str(os.getpid()), "-f", "wait"], capture_output=True)
                live.kill()
                live.wait()
                time.sleep(0.3)
                found = own_leftovers(root)
                self.assertEqual(len(found), 1)
                kill_leftovers(found)
            finally:
                subprocess.run(["pkill", "-9", "-f", "^sleep 60$"], capture_output=True)
