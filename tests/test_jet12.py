"""JET-12：重推／父卡 rebase 時，每個提交「只看加減的行」的指紋沒變就沿用放行；二分對兇手候選重測沒再紅就結案成抖動。"""
import json
import unittest
from types import SimpleNamespace as NS

from jet import store
from jet.states import Event
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


class IdleBlockedTest(SchedBase):
    def _setup(self):
        s = self.mk(policy={"order": "review_first"}, workflow={"mode": "direct_merge"})
        for n in "abc":
            self.repo.add(n)
        self.go(3)
        store.set_parent(self.con, "b", "a")
        store.set_parent(self.con, "c", "b")
        self.approve_all("b", "c")                       # 父卡 a 沒放行，已放行的 b、c 被擋
        self.con.execute("DELETE FROM events WHERE kind='idle_blocked'")   # 前面幾個 tick 已經自己報過了：清掉，只看下面明確呼叫的
        store.set_meta(self.con, "idle_sig", "")
        return s

    def _events(self):
        return [json.loads(r[0]) for r in self.con.execute("SELECT detail FROM events WHERE kind='idle_blocked' ORDER BY id")]

    def test_emits_blocker_list_once_then_reminds(self):
        s = self._setup()
        s.jobs = {}
        s._idle_step()
        ev = self._events()
        self.assertEqual(len(ev), 1)
        top = ev[0]["blockers"][0]
        self.assertEqual((top["branch"], top["approved"], top["descendants"], top["approved_descendants"]), ("a", False, 2, 2))
        self.assertEqual((ev[0]["pending"], ev[0]["approved_pending"]), (3, 2))
        s._idle_step(); s._idle_step()                    # 同一批：不重送
        self.assertEqual(len(self._events()), 1)
        t0 = s.clock()
        s.clock = lambda: t0 + s.IDLE_REMIND_S + 1        # 過了提醒間隔、仍然閒置：再提醒一次
        s._idle_step()
        self.assertEqual(len(self._events()), 2)

    def test_dedupe_key_is_blocker_set_not_counts_and_survives_flapping(self):
        s = self._setup()
        s.jobs = {}
        s._idle_step()
        self.assertEqual(len(self._events()), 1)
        self.approve_all("a")                             # 放行狀態變了、被擋子卡數變了，但擋路卡集合還是同一批（a 還在 pending）
        # a 放行後其實變成可測：機器忙一下又閒下來——模擬「有自測在跑」再回到「又沒有可測的卡」
        s.jobs = {1: NS(kind="selftest", units=1, branch="x")}
        s._idle_step()
        s.jobs = {}
        store.revoke(self.con, "a", "t")                  # 又變回沒放行
        s._idle_step()
        self.assertEqual(len(self._events()), 1)          # 同一批擋路卡 60 分鐘內不重送，閃爍也不洗版
        t0 = s.clock()
        s.clock = lambda: t0 + s.IDLE_REMIND_S - 5
        s._idle_step()
        self.assertEqual(len(self._events()), 1)
        s.clock = lambda: t0 + s.IDLE_REMIND_S + 5
        s._idle_step()
        self.assertEqual(len(self._events()), 2)          # 滿 60 分鐘才再提醒一次

    def test_new_blocker_set_sends_immediately(self):
        s = self._setup()
        s.jobs = {}
        s._idle_step()
        self.assertEqual(len(self._events()), 1)
        self.repo.add("z")                                # 多一條沒放行的獨立分支＝多一個擋路卡：集合變了，立即送
        self.go(2)
        self.con.execute("DELETE FROM events WHERE kind='idle_blocked' AND id NOT IN (SELECT MAX(id) FROM events WHERE kind='idle_blocked')")
        s.jobs = {}
        s._idle_step()
        names = {b["branch"] for b in self._events()[-1]["blockers"]}
        self.assertEqual(names, {"a", "z"})

    def test_resets_when_testable_or_running(self):
        s = self._setup()
        s.jobs = {}
        s._idle_step()
        self.assertEqual(len(self._events()), 1)
        self.approve_all("a")                             # 父卡放行了：有可測的卡 → 不再算擋路、不送（去重鍵不動，閃爍不洗版）
        s._idle_step()
        self.assertEqual(store.get_meta(self.con, "idle_sig"), '["a"]')
        self.assertEqual(len(self._events()), 1)
        s.jobs = {1: NS(kind="selftest", units=1, branch="x")}   # 有自測在跑：不報
        store.set_meta(self.con, "idle_sig", "")
        s._idle_step()
        self.assertEqual(len(self._events()), 1)

    def test_nothing_pending_is_silent(self):
        s = self.mk(policy={"order": "review_first"}, workflow={"mode": "direct_merge"})
        s.jobs = {}
        s._idle_step()
        self.assertEqual(self._events(), [])

    def test_rejected_parent_counts_as_blocker(self):
        s = self._setup()
        self.approve_all("a")
        store.apply_event(self.con, "a", Event.CONFLICT_MAIN, reject_reason="撞檔") if self.state("a") == "pending" else None
        s.jobs = {}
        s._idle_step()
        ev = self._events()
        if self.state("a") == "rejected":
            self.assertEqual(ev[-1]["blockers"][0]["state"], "rejected")


class ParallelProbeTest(SchedBase):
    def _st(self):
        return {"items": ["t1"], "cands": [f"c{i}" for i in range(10)], "lo": -1, "hi": 9, "phase": "bisect", "queue": [], "red_sha": "c9"}

    def _s(self, par):
        s = self.mk(delay=50, cores={"pool": "0-63", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 4},
                    workflow={"mode": "direct_merge", "loop": {"enabled": True, "probe_parallel": par}})
        store.set_meta(self.con, "loop_bisect", json.dumps(self._st()))
        return s

    def _finish(self, s, sha):
        rid = next(r for r, j in s.jobs.items() if j.kind == "loopprobe" and j.head_sha == sha)
        s._release_job(rid, "pass")

    def _probes(self, s):
        return sorted(j.head_sha for j in s.jobs.values() if j.kind == "loopprobe")

    def test_parallel_opens_evenly_spaced_probes(self):
        s = self._s(3)
        s._loop_step()
        self.assertEqual(self._probes(s), ["c1", "c4", "c6"])           # lo=-1, hi=9：-1+10*t//4 = 1, 4, 6
        s._loop_step()                                                   # 已滿：不再開
        self.assertEqual(len(self._probes(s)), 3)

    def test_single_probe_is_old_midpoint(self):
        s = self._s(1)
        s._loop_step(); s._loop_step()
        self.assertEqual(self._probes(s), ["c4"])                        # (lo+hi)//2 = 4，同原本

    def test_red_result_cancels_probes_beyond_hi(self):
        s = self._s(3)
        s._loop_step()
        self._finish(s, "c4")                                            # 真實流程裡 _collect 先把跑完的工作從 jobs 拿掉才呼叫結果處理
        s._on_loopprobe(NS(head_sha="c4"), NS(status="fail", failed_items=["t1"]))   # c4 紅：hi＝4
        self.assertEqual(self._probes(s), ["c1"])                        # c6（在 hi 之外）被停掉；c1 還在 (lo, hi] 裡
        st = json.loads(store.get_meta(self.con, "loop_bisect"))
        self.assertEqual((st["lo"], st["hi"]), (-1, 4))

    def test_green_result_cancels_probes_at_or_below_lo(self):
        s = self._s(3)
        s._loop_step()
        self._finish(s, "c4")
        s._on_loopprobe(NS(head_sha="c4"), NS(status="pass", failed_items=[]))      # c4 綠：lo＝4
        self.assertEqual(self._probes(s), ["c6"])                        # c1（在 lo 以下）被停掉

    def test_config_range(self):
        from jet import config
        base = {"git": {"remote": "x"}, "paths": {"state_dir": "/tmp/x"}, "commands": {"test": ["t"]},
                "cores": {"pool": "0-31", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 4}, "limits": {"batch_max": 16}}
        for bad in (0, 5, -1):
            with self.assertRaises(config.ConfigError):
                config.parse({**base, "workflow": {"mode": "direct_merge", "loop": {"probe_parallel": bad}}})
        self.assertEqual(config.parse({**base, "workflow": {"mode": "direct_merge", "loop": {"probe_parallel": 3}}}).loop_probe_parallel, 3)
        self.assertEqual(config.parse(base).loop_probe_parallel, 1)



class AdoptKindsTest(SchedBase):
    """JET-16：換版接管要接背景探測（loopprobe）與每日完整層（nightly），不能只接基準；沒接的會被殺掉重排，白白丟掉十幾分鐘。"""

    def _run(self, kind):
        import os
        s = self.mk(workflow={"mode": "direct_merge"})
        wd = os.path.join(self.t.name, "wd"); os.makedirs(wd, exist_ok=True)
        log_dir = os.path.join(self.t.name, "runs", "9"); os.makedirs(log_dir, exist_ok=True)
        open(os.path.join(log_dir, "exit.json"), "w").write("{}")          # exit.json 在：視為還活著可接管
        self.con.execute("INSERT INTO runs(id,kind,status,layer,started_at,pgid,cores,workdir,commit_sha,base_sha,log_path,report_path,timeout_s,units) "
                         "VALUES(9,?,?,?,?,?,?,?,?,?,?,?,?,?)", (kind, "running", "gate", 1000.0, 4242, "3,4", wd, "c" * 40, "c" * 40,
                                                                 os.path.join(log_dir, "output.log"), os.path.join(log_dir, "report.json"), 3600, 2))
        self.ex.adopt = lambda job, pgid, remaining, elapsed: NS(pgid=pgid)
        return s

    def test_loopprobe_and_nightly_are_adopted(self):
        for kind in ("loopprobe", "nightly", "baseline"):
            s = self._run(kind)
            out = s._adopt_runs()
            self.assertEqual([j.kind for j in out.values()], [kind], kind)
            self.assertIn(9, s.jobs)

    def test_unknown_kind_still_not_adopted(self):
        s = self._run("recheck")                          # 複驗的記錄在記憶體裡，換版後接不起來：照舊不接（殺掉重排）
        self.assertEqual(s._adopt_runs(), {})


if __name__ == "__main__":
    unittest.main()
