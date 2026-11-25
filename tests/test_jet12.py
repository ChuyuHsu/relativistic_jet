"""JET-12：重推／父卡 rebase 時，每個提交「只看加減的行」的指紋沒變就沿用放行；二分對兇手候選重測沒再紅就結案成抖動。"""
import json
import unittest
from types import SimpleNamespace as NS

from jet import store
from tests.test_gitops import Base as GitBase, sh
from tests.test_scheduler import Base as SchedBase


class CommitIdsTest(GitBase):
    def _branch(self, name, files, start="main"):
        sh(self.dev, "checkout", "-q", start)
        sh(self.dev, "checkout", "-q", "-b", name)
        for f, t in files.items():
            self.write(f, t)
        sha = self.commit(name)
        sh(self.dev, "push", "-q", "origin", name)
        sh(self.dev, "checkout", "-q", "main")
        return sha

    def _lines(self, **ch):
        out = []
        for i in range(1, 31):
            out.append(ch.get(f"l{i}", f"line {i}\n"))
        return "".join(out)

    def test_rebase_after_context_change_keeps_ids_but_real_change_does_not(self):
        ap = self._branch("f", {"a.txt": self._lines(l10="CHANGED10\n")})
        self.repo.fetch()                                           # 排隊器在放行前就掃過這條分支，舊提交還在鏡像裡
        # main 動了同一檔旁邊的行（上下文變了，但不衝突）
        sh(self.dev, "checkout", "-q", "main")
        self.write("a.txt", self._lines(l8="MAIN8\n"))
        self.commit("main 動 8")
        sh(self.dev, "push", "-q", "origin", "main")
        # 開發串 rebase 重推：把 f 疊到新 main 上
        sh(self.dev, "checkout", "-q", "f")
        sh(self.dev, "rebase", "-q", "main")
        head = sh(self.dev, "rev-parse", "HEAD")
        sh(self.dev, "push", "-q", "-f", "origin", "f")
        sh(self.dev, "checkout", "-q", "main")
        self.repo.fetch()
        main = self.repo.main_sha()
        self.assertNotEqual(head, ap)
        ids_ap, ids_head = self.repo.commit_ids(ap, main), self.repo.commit_ids(head, main)
        self.assertTrue(ids_head and ids_head <= ids_ap)            # 加減的行沒變：沿用放行
        # 內容真的改了（多加一行）：新增的提交指紋不在放行版本裡
        sh(self.dev, "checkout", "-q", "f")
        self.write("b.txt", "bbb\nextra\n")
        head2 = self.commit("多加")
        sh(self.dev, "checkout", "-q", "main")
        sh(self.dev, "push", "-q", "-f", "origin", "f")
        self.repo.fetch()
        self.assertFalse(self.repo.commit_ids(head2, main) <= ids_ap)

    def test_empty_when_nothing_beyond_base(self):
        main = self.repo.main_sha()
        self.assertEqual(self.repo.commit_ids(main, main), frozenset())
        self.assertEqual(self.repo.commit_ids("0" * 40, main), frozenset())   # 讀不到回空集合


class JudgeTest(SchedBase):
    def _row(self):
        s = self.mk(workflow={"mode": "direct_merge"})
        self.repo.add("a")
        self.go(4)
        return s, store.get_branch(self.con, "a")

    def test_same_patches_subset_keeps_approval(self):
        s, row = self._row()
        s.main = "m1"
        self.repo.commit_ids = lambda sha, base: frozenset({"p1", "p2"}) if sha == "old" else frozenset({"p1"})
        self.assertTrue(s._same_patches(row, "old"))               # 現在的頭 {p1} ⊆ 放行版本 {p1,p2}
        self.repo.commit_ids = lambda sha, base: frozenset({"p1", "p2"}) if sha == "old" else frozenset({"p1", "p9"})
        s._same_cache = {}
        self.assertFalse(s._same_patches(row, "old"))                # 多了 p9：放行失效
        self.repo.commit_ids = lambda sha, base: frozenset()
        s._same_cache = {}
        self.assertFalse(s._same_patches(row, "old"))                # 空集合（讀不到）：不沿用
        del self.repo.commit_ids
        s._same_cache = {}
        self.assertFalse(s._same_patches(row, "old"))                # repo 沒這個能力：不沿用


class UnstableTest(SchedBase):
    def _st(self, **over):
        st = {"items": ["t1"], "cands": ["c0", "c1", "c2", "c3", "c4"], "lo": 3, "hi": 4, "phase": "bisect", "queue": [], "red_sha": "c4"}
        st.update(over)
        return st

    def test_candidate_retest_green_is_unstable_not_culprit(self):
        s = self.mk(workflow={"mode": "direct_merge"})
        store.set_meta(self.con, "loop_bisect", json.dumps(self._st()))
        s._on_loopprobe(NS(head_sha="c4"), NS(status="pass", failed_items=[]))
        self.assertEqual(store.get_meta(self.con, "loop_bisect"), "")
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM events WHERE kind='loop_culprit'").fetchone()[0], 0)
        d = json.loads(self.con.execute("SELECT detail FROM events WHERE kind='loop_green' ORDER BY id DESC").fetchone()[0])
        self.assertEqual((d["main_sha"], d["unstable_items"]), ("c4", ["t1"]))
        self.assertIn("抖動", d["detail"])
        self.assertEqual(store.get_meta(self.con, "loop_green"), "c4")

    def test_candidate_retest_red_confirms_then_culprit(self):
        s = self.mk(workflow={"mode": "direct_merge"})
        store.set_meta(self.con, "loop_bisect", json.dumps(self._st()))
        s._on_loopprobe(NS(head_sha="c4"), NS(status="fail", failed_items=["t1"]))
        st = json.loads(store.get_meta(self.con, "loop_bisect"))
        self.assertTrue(st["hi_ok"])
        s._loop_conclude(st)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM events WHERE kind='loop_culprit'").fetchone()[0], 1)

    def test_other_probe_results_unchanged(self):
        s = self.mk(workflow={"mode": "direct_merge"})
        store.set_meta(self.con, "loop_bisect", json.dumps(self._st(lo=-1)))
        s._on_loopprobe(NS(head_sha="c2"), NS(status="pass", failed_items=[]))
        st = json.loads(store.get_meta(self.con, "loop_bisect"))
        self.assertEqual((st["lo"], st["hi"]), (2, 4))
        self.assertFalse(st.get("hi_ok"))



class PrerunTest(SchedBase):
    def _states(self):
        return {r["name"]: r["state"] for r in store.list_branches(self.con)}

    def _setup(self, n_pre):
        s = self.mk(delay=6, policy={"order": "review_first", "prerun_unapproved": n_pre}, workflow={"mode": "direct_merge"})
        for n in "abcd":
            self.repo.add(n)
        self.go(2)                       # 還沒有任何放行：開了預跑名額（n_pre 條）才會有自測
        return s

    def test_off_nothing_runs_without_approval(self):
        self._setup(0)
        self.assertEqual([n for n, v in self._states().items() if v == "selftesting"], [])
        self.approve_all("d")
        self.go(2)
        self.assertEqual([n for n, v in self._states().items() if v == "selftesting"], ["d"])

    def test_on_prerun_limited_and_approved_not_limited(self):
        s = self._setup(2)
        st = self._states()
        self.assertEqual(sorted(n for n, v in st.items() if v == "selftesting"), ["a", "b"])   # 沒放行也先跑，但只開 2 條
        self.assertEqual(s._prerun, {"a", "b"})
        self.approve_all("d")
        self.go(2)
        st = self._states()
        self.assertEqual(sorted(n for n, v in st.items() if v == "selftesting"), ["a", "b", "d"])   # 放行的不受預跑名額限制
        self.assertEqual(s._prerun, {"a", "b"})

    def test_prerun_not_cancelled_and_green_waits_for_approval(self):
        s = self._setup(2)
        self.go(2)
        self.assertEqual(s._prerun, {"a", "b"})                       # _enforce_review_first 沒把預跑停掉
        self.go(60, until=lambda: all(v != "selftesting" for v in self._states().values()) and not s.jobs)
        st = self._states()
        self.assertEqual(self.merged(), set())                        # 沒放行的絕不合入，就算自測綠了
        self.assertTrue(all(st[n] in ("selftest_green", "pending") for n in "abcd"))
        self.assertEqual(s._prerun, set())                            # 自測結束就從預跑名單拿掉
        self.approve_all("a")
        self.go(60, until=lambda: "a" in self.merged())               # 放行一到，已綠的直接排隊合入（不用重測）

    def test_config(self):
        from jet import config
        base = {"git": {"remote": "x"}, "paths": {"state_dir": "/tmp/x"}, "commands": {"test": ["t"]},
                "cores": {"pool": "0-31", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 4}, "limits": {"batch_max": 16}}
        with self.assertRaises(config.ConfigError):
            config.parse({**base, "policy": {"prerun_unapproved": -1}})
        self.assertEqual(config.parse({**base, "policy": {"prerun_unapproved": 2}}).prerun_unapproved, 2)
        self.assertEqual(config.parse(base).prerun_unapproved, 0)


if __name__ == "__main__":
    unittest.main()
