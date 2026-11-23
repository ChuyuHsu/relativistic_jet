import os, tempfile, unittest
from jet import config, db, store
from jet.scheduler import Scheduler
from tests.fakes import FakeRepo, FakeExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CLASSIFY = os.path.join(HERE, "..", "examples", "toy_project", "classify.py")


class Base(unittest.TestCase):
    def mk(self, delay=0, **over):
        self.t = tempfile.TemporaryDirectory()
        self.addCleanup(self.t.cleanup)
        d = {"git": {"remote": "x"}, "paths": {"state_dir": os.path.join(self.t.name, "st")},
             "cores": {"pool": "0-31", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 4},
             "commands": {"test": ["t"]}, "limits": {"batch_max": 16}}
        for k, v in over.items():
            d.setdefault(k, {}).update(v) if isinstance(v, dict) else d.__setitem__(k, v)
        self.cfg = config.parse(d)
        self.con = db.connect(os.path.join(self.t.name, "a.sqlite"))
        self.addCleanup(self.con.close)
        self.repo = FakeRepo()
        self.ex = FakeExecutor(self.repo, delay)
        self.s = Scheduler(self.cfg, self.con, self.repo, self.ex, clock=lambda: 1000.0,
                           foreign_busy=lambda cores, pgids: set())     # 不受開發機上別人的行程影響
        return self.s

    def state(self, n):
        r = store.get_branch(self.con, n)
        return r["state"] if r else None

    def go(self, n=60, until=None):
        for _ in range(n):
            self.s.tick()
            if until and until():
                return
        if until:
            self.fail("條件沒達成：" + str({r["name"]: r["state"] for r in store.list_branches(self.con)}))

    def merged(self):
        return {r["name"] for r in store.list_branches(self.con, "merged")}

    def approve_all(self, *names):
        for n in names:
            store.approve(self.con, n, "t")


class HappyPath(Base):
    def test_merge_all(self):
        self.mk()
        for n in "abc":
            self.repo.add(n)
        self.s.tick()
        self.approve_all("a", "b", "c")
        self.go(until=lambda: self.merged() == {"a", "b", "c"})
        self.assertEqual(sorted(self.repo.deleted), ["a", "b", "c"])
        self.assertEqual(len(self.repo.ff_log), 1)           # 一批一次快轉
        self.assertEqual(self.repo.main, "m0>a+b+c")

    def test_needs_approval(self):
        self.mk()
        self.repo.add("a")
        self.go(10)
        self.assertEqual(self.state("a"), "selftest_green")   # 自測綠但沒放行，不排隊
        self.approve_all("a")
        self.go(until=lambda: self.merged() == {"a"})

    def test_approval_first_then_selftest(self):
        self.mk()
        self.repo.add("a")
        self.s.tick(); self.approve_all("a")                  # 放行先到
        self.go(until=lambda: self.merged() == {"a"})

    def test_no_approval_policy(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("a")
        self.go(until=lambda: self.merged() == {"a"})

    def test_selftest_red_rejected(self):
        self.mk()
        self.repo.add("a", reds={"t1"})
        self.go(5)
        self.assertEqual(self.state("a"), "rejected")
        self.assertIn("t1", store.get_branch(self.con, "a")["reject_reason"])
        self.assertEqual(self.con.execute("SELECT count(*) FROM events WHERE kind='rejected'").fetchone()[0], 1)

    def test_selftest_timeout_is_red(self):
        self.mk()
        self.repo.add("a")
        self.s.tick()
        self.ex.force["m0>a"] = "timeout"
        self.go(5)
        self.assertEqual(self.state("a"), "rejected")
        self.assertIn("逾時", store.get_branch(self.con, "a")["reject_reason"])

    def test_infra_retries(self):
        self.mk()
        self.repo.add("a")
        self.s.tick()
        self.ex.force["m0>a"] = "infra"
        self.go(3)
        self.assertIn(self.state("a"), ("pending", "selftesting"))
        self.ex.force.clear()
        self.go(until=lambda: self.state("a") == "selftest_green")


class Culprits(Base):
    def test_two_red_in_one_batch(self):
        self.mk(policy={"require_approval": False}, limits={"batch_max": 8})
        for i in range(1, 9):
            self.repo.add(f"b{i}", gate_reds={"red"} if i in (3, 6) else ())
        self.go(80, until=lambda: len(self.merged()) == 6 and self.state("b3") == "rejected"
                 and self.state("b6") == "rejected")
        self.assertEqual(self.merged(), {"b1", "b2", "b4", "b5", "b7", "b8"})
        self.assertEqual(self.state("b3"), "rejected")
        self.assertEqual(self.state("b6"), "rejected")

    def test_main_already_red_not_blocking(self):
        self.mk(policy={"require_approval": False})
        self.repo.main_red = {"old"}
        self.repo.add("a"); self.repo.add("b", gate_reds={"new"})
        self.go(40, until=lambda: self.state("a") == "merged" and self.state("b") == "rejected")

    def test_double_fail_halts(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("a", gate_reds={"red"})
        self.go(30, until=lambda: self.state("a") == "rejected")
        self.assertEqual(store.get_branch(self.con, "a")["fail_count"], 1)
        # 同內容只換 sha（重疊）→ 回待自測，紅次數保留，再批測紅就暫停
        self.repo.branches["a"]["sha"] = "a-2"
        self.go(40, until=lambda: self.state("a") == "halted")
        store.resume_branch(self.con, "a")
        self.assertEqual(self.state("a"), "pending")

    def test_batch_point_timeout_is_red_and_found(self):
        """測點逾時＝紅：找出兇手退回，其餘合入。"""
        self.mk(policy={"require_approval": False}, limits={"batch_max": 4})
        for n in ("a", "b", "c", "d"):
            self.repo.add(n)
        self.go(40, until=lambda: all(self.state(n) == "selftest_green" or self.state(n) == "queued"
                                      for n in "abcd") or self.state("a") == "batching")
        # b 疊進去的所有測點（前綴含 b）都逾時
        for k in range(2, 5):
            pass
        self.ex.force = {}
        orig_poll = self.ex.poll

        def poll(h):
            res = orig_poll(h)
            if res is not None and h.job.layer == "gate" and ">" in h.job.workdir \
                    and "b" in h.job.workdir.rsplit(">", 1)[1].split("+"):
                res.status, res.failed_items = "timeout", []
            return res
        self.ex.poll = poll
        self.go(80, until=lambda: self.state("b") == "rejected" and
                {self.state(n) for n in "acd"} == {"merged"})
        self.assertIn("逾時", store.get_branch(self.con, "b")["reject_reason"])

    def test_three_infra_pause_queue(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("a")
        orig = self.ex.poll
        self.ex.poll = lambda h: (lambda r: r and type(r)("infra", None))(orig(h))
        self.go(30)
        self.assertTrue(store.is_paused(self.con))
        self.assertEqual(self.con.execute("SELECT count(*) FROM events WHERE kind='queue_error'").fetchone()[0], 1)
        self.assertNotEqual(self.state("a"), "merged")


class Rules(Base):
    def test_rebase_same_content_keeps_state(self):
        self.mk()
        self.repo.add("a")
        self.go(8)
        self.approve_all("a")
        self.go(1)
        before = store.get_branch(self.con, "a")
        self.repo.branches["a"]["sha"] = "a-2"      # 內容不變、只是重疊
        self.s.tick()
        after = store.get_branch(self.con, "a")
        self.assertEqual(after["content_id"], before["content_id"])
        self.assertNotEqual(after["state"], "pending")
        self.assertTrue(store.is_approved(self.con, "a", after["content_id"]))

    def test_new_content_resets_and_voids_approval(self):
        self.mk()
        self.repo.add("a")
        self.go(5); self.approve_all("a")
        self.repo.push_new("a", reds={"zz"})
        self.go(5)
        self.assertEqual(self.state("a"), "rejected")       # 新內容重新自測（這次紅）
        r = store.get_branch(self.con, "a")
        self.assertFalse(store.is_approved(self.con, "a", r["content_id"]))

    def test_conflict_with_main_rejected_at_batch(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("a", conflict_main=True)
        self.go(10, until=lambda: self.state("a") == "rejected")
        self.assertEqual(self.con.execute("SELECT count(*) FROM events WHERE kind='conflict'").fetchone()[0], 1)

    def test_conflict_within_batch_waits_next_batch(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("a", conflict_with={"b"}); self.repo.add("b")
        self.go(30, until=lambda: self.merged() == {"a", "b"})
        self.assertEqual(len(self.repo.ff_log), 2)

    def test_queue_conflict_after_merge_rejected_immediately(self):
        self.mk(policy={"require_approval": False}, limits={"batch_max": 1})
        self.repo.add("a"); self.repo.add("x")
        store_prio = lambda: store.set_priority(self.con, "a", 9)
        self.go(1); store_prio()
        self.go(40, until=lambda: self.state("a") == "batching" and self.state("x") == "queued")
        self.repo.branches["x"]["conflict_main"] = True      # a 合入後 x 才跟 main 撞
        self.go(40, until=lambda: self.state("a") == "merged")
        self.assertEqual(self.state("x"), "rejected")
        d = self.con.execute("SELECT detail FROM events WHERE kind='conflict'").fetchone()[0]
        self.assertIn("合入後", d)

    def test_priority_orders_batch(self):
        self.mk(policy={"require_approval": False}, limits={"batch_max": 1})
        for n in "abc":
            self.repo.add(n)
        self.s.tick()
        store.set_priority(self.con, "c", 5)
        self.go(60, until=lambda: len(self.merged()) == 3)
        self.assertEqual(self.repo.ff_log[0], "m0>c")

    def test_paused_starts_nothing(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("a")
        store.set_paused(self.con, True)
        self.go(5)
        self.assertEqual(self.ex.started, [])
        store.set_paused(self.con, False)
        self.go(until=lambda: self.merged() == {"a"})

    def test_resources_low_pauses(self):
        self.mk(policy={"require_approval": False})
        self.s.resources_ok = lambda: False
        self.repo.add("a")
        self.go(5)
        self.assertEqual(self.ex.started, [])

    def test_cores_never_shared(self):
        self.mk(delay=2, policy={"require_approval": False})
        for i in range(30):
            self.repo.add(f"n{i}")
        self.go(200, until=lambda: len(self.merged()) == 30)   # FakeExecutor 內有共用核斷言

    def test_selftest_borrowing_respects_reserve(self):
        self.mk(delay=3, cores={"pool": "0-31", "reserve_for_selftest": 2})
        for i in range(10):
            self.repo.add(f"n{i}")
        self.go(8)  # 沒排隊的：可借全部核（超過保留的 2 顆）
        self.assertGreaterEqual(len(self.ex.started), 10)

    def test_docs_only_merges_without_batch(self):
        self.mk(commands={"test": ["t"], "classify": ["python3", CLASSIFY, "{files}"]})
        self.repo.add("d", files=("docs/a.md",))
        self.go(5)
        self.assertEqual(self.state("d"), "pending")         # 沒放行不合
        self.approve_all("d")
        self.go(5)
        self.assertEqual(self.state("d"), "merged")
        self.assertEqual(self.repo.deleted, ["d"])

    def test_recover_resets_inflight(self):
        self.mk(delay=5)
        self.repo.add("a")
        self.go(30, until=lambda: self.state("a") == "selftesting")
        s2 = Scheduler(self.cfg, self.con, self.repo, FakeExecutor(self.repo), clock=lambda: 1.0)
        self.assertEqual(self.state("a"), "pending")
        self.assertEqual(self.con.execute("SELECT count(*) FROM runs WHERE status='running'").fetchone()[0], 0)

    def test_vanished_branch_dropped(self):
        self.mk()
        self.repo.add("a")
        self.go(2)
        del self.repo.branches["a"]
        self.s.tick()
        self.assertIsNone(self.state("a"))


class Stacked(Base):
    def test_parent_before_child_and_child_detaches(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("p"); self.repo.add("c")
        self.s.tick()
        store.set_parent(self.con, "c", "p")
        self.go(40, until=lambda: self.merged() == {"p", "c"})
        self.assertEqual(self.repo.main, "m0>p+c")
        self.assertIsNone(store.get_branch(self.con, "c")["parent"])

    def test_child_waits_for_parent_approval(self):
        self.mk()
        self.repo.add("p"); self.repo.add("c")
        self.s.tick()
        store.set_parent(self.con, "c", "p")
        self.go(8)
        self.approve_all("c")
        self.go(5)
        self.assertEqual(self.state("c"), "selftest_green")   # 父還沒排隊，子不能排
        self.approve_all("p")
        self.go(30, until=lambda: self.merged() == {"p", "c"})

    def test_parent_culprit_sends_child_back(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("p", gate_reds={"r"}); self.repo.add("c")
        self.s.tick()
        store.set_parent(self.con, "c", "p")
        self.go(40, until=lambda: self.state("p") == "rejected")
        self.go(3)
        self.assertEqual(self.state("c"), "pending")           # 等父處理好再測
        self.assertEqual(self.merged(), set())


if __name__ == "__main__":
    unittest.main()


class Recovery(Base):
    def test_killed_after_push_before_bookkeeping(self):
        """推 main 成功、狀態還沒記就被砍：重啟後補記合入，不誤退、不留分支。"""
        self.mk(policy={"require_approval": False})
        self.repo.add("a"); self.repo.add("b")
        orig = self.repo.fast_forward_main

        class Killed(Exception):
            pass

        def ff(sha, expect):
            orig(sha, expect)
            raise Killed()
        self.repo.fast_forward_main = ff
        with self.assertRaises(Killed):
            self.go(60)
        self.repo.fast_forward_main = orig
        self.assertEqual(self.state("a"), "batching")
        Scheduler(self.cfg, self.con, self.repo, FakeExecutor(self.repo), clock=lambda: 1.0)
        self.assertEqual(self.merged(), {"a", "b"})
        self.assertEqual(sorted(self.repo.deleted), ["a", "b"])

    def test_killed_before_push_requeues(self):
        self.mk(delay=2, policy={"require_approval": False})
        self.repo.add("a")
        self.go(60, until=lambda: self.state("a") == "batching")
        Scheduler(self.cfg, self.con, self.repo, FakeExecutor(self.repo), clock=lambda: 1.0)
        self.assertEqual(self.state("a"), "queued")
        self.assertEqual(self.repo.ff_log, [])

    def test_push_refused_requeues_without_merge(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("a")
        from jet.gitops import MainMoved
        orig = self.repo.fast_forward_main
        fails = [1]

        def ff(sha, expect):
            if fails:
                fails.pop()
                raise MainMoved()
            orig(sha, expect)
        self.repo.fast_forward_main = ff
        self.go(60, until=lambda: self.state("a") == "merged")
        self.assertEqual(len(self.repo.ff_log), 1)


class StackConflict(Base):
    def test_rejected_parent_takes_children_out_of_batch(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("p", conflict_main=True); self.repo.add("c")
        self.s.tick()
        store.set_parent(self.con, "c", "p")
        self.go(30, until=lambda: self.state("p") == "rejected")
        self.go(5)
        self.assertEqual(self.merged(), set())
        self.assertNotIn("m0>p", self.repo.main)
        self.assertEqual(self.state("c"), "pending")


class ManyCulprits(Base):
    def test_three_culprits_and_all_red(self):
        self.mk(policy={"require_approval": False}, limits={"batch_max": 16})
        for i in range(1, 13):
            self.repo.add(f"b{i}", gate_reds={"r"} if i in (2, 5, 9) else ())
        self.go(150, until=lambda: len(self.merged()) == 9)
        self.assertEqual({n for n in ("b2", "b5", "b9") if self.state(n) == "rejected"}, {"b2", "b5", "b9"})

    def test_all_red_batch(self):
        self.mk(policy={"require_approval": False}, limits={"batch_max": 4})
        for i in range(1, 5):
            self.repo.add(f"b{i}", gate_reds={"r"})
        self.go(120, until=lambda: all(self.state(f"b{i}") == "rejected" for i in range(1, 5)))
        self.assertEqual(self.repo.ff_log, [])


class TrackedScope(Base):
    def mk_t(self, **kw):
        return self.mk(git={"scope": "tracked"}, policy={"require_approval": False}, **kw)

    def test_untracked_branches_ignored(self):
        self.mk_t()
        self.repo.add("a"); self.repo.add("b")
        store.track(self.con, "a")
        self.go(40, until=lambda: self.state("a") == "merged")
        self.assertIsNone(self.state("b"))                      # 沒登記的完全不理
        self.assertEqual(self.repo.main, "m0>a")

    def test_track_later_picks_up(self):
        self.mk_t()
        self.repo.add("a")
        self.go(5)
        self.assertEqual(store.list_branches(self.con), [])
        store.track(self.con, "a")
        self.go(40, until=lambda: self.state("a") == "merged")

    def test_track_idempotent_and_events(self):
        self.mk_t()
        self.assertTrue(store.track(self.con, "a")["created"])
        self.assertFalse(store.track(self.con, "a")["created"])
        self.assertTrue(store.untrack(self.con, "a"))
        self.assertFalse(store.untrack(self.con, "a"))
        kinds = [r[0] for r in self.con.execute("SELECT kind FROM events ORDER BY id")]
        self.assertEqual(kinds, ["tracked", "untracked"])
        self.assertEqual(self.con.execute("SELECT count(*) FROM events WHERE resolved_at IS NULL").fetchone()[0], 0)

    def test_untrack_cancels_selftest_and_clears_queue(self):
        self.mk_t(delay=3)
        self.repo.add("a"); self.repo.add("q")
        store.track(self.con, "a"); store.track(self.con, "q")
        self.go(60, until=lambda: self.state("a") == "selftesting")
        store.untrack(self.con, "a")
        self.go(2)
        self.assertIsNone(self.state("a"))
        self.assertEqual([j for j in self.s.jobs.values() if j.branch == "a"], [])
        self.assertGreaterEqual(len(self.ex.cancelled), 1)
        self.go(80, until=lambda: self.state("q") == "merged")  # 其他的照常

    def test_untrack_queued_and_batching(self):
        self.mk_t(delay=2)
        for n in "abc":
            self.repo.add(n); store.track(self.con, n)
        self.go(80, until=lambda: self.state("a") == "batching")
        store.untrack(self.con, "b")
        self.go(80, until=lambda: self.merged() == {"a", "c"})
        self.assertIsNone(self.state("b"))
        self.assertNotIn("b", self.repo.main)

    def test_approve_untracked_does_nothing(self):
        self.mk_t()
        self.repo.add("a")
        self.go(3)
        with self.assertRaises(store.NoSuchBranch):
            store.approve(self.con, "a", "t")                   # 沒登記就沒有這條分支
        self.go(10)
        self.assertEqual(self.ex.started and [j.layer for j in self.ex.started if j.layer != "gate"], [])

    def test_track_with_parent(self):
        self.mk_t()
        self.repo.add("p"); self.repo.add("c")
        with self.assertRaises(store.TrackError):
            store.track(self.con, "c", "p")                     # 父沒登記
        store.track(self.con, "p"); store.track(self.con, "c", "p")
        self.go(60, until=lambda: self.merged() == {"p", "c"})
        self.assertEqual(self.repo.main, "m0>p+c")
        with self.assertRaises(store.TrackError):
            store.track(self.con, "x", "x")

    def test_untrack_parent_with_children_refused(self):
        self.mk_t()
        store.track(self.con, "p"); store.track(self.con, "c", "p")
        with self.assertRaises(store.TrackError):
            store.untrack(self.con, "p")
        store.untrack(self.con, "c"); store.untrack(self.con, "p")

    def test_all_mode_unchanged(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("a")
        self.go(40, until=lambda: self.state("a") == "merged")


class FlakyBaseline(Base):
    def test_flaky_in_history_not_new_red(self):
        """最近幾次 main 紅過的項目（抖動項）不算新紅：不會讓無辜分支被退。"""
        self.mk(policy={"require_approval": False})
        store.set_baseline(self.con, "old-main", "gate", {"flk"})        # 以前量到 main 紅過
        self.repo.add("a", gate_reds={"flk"})                              # 這次又抖出 flk
        self.go(60, until=lambda: self.state("a") in ("merged", "rejected"))
        self.assertEqual(self.state("a"), "merged")

    def test_new_item_still_blamed(self):
        self.mk(policy={"require_approval": False})
        store.set_baseline(self.con, "old-main", "gate", {"flk"})
        self.repo.add("a", gate_reds={"brand_new"})
        self.go(60, until=lambda: self.state("a") in ("merged", "rejected"))
        self.assertEqual(self.state("a"), "rejected")

    def test_history_window(self):
        self.mk(policy={"require_approval": False}, limits={"baseline_history": 2})
        store.set_baseline(self.con, "m-1", "gate", {"ancient"})
        store.set_baseline(self.con, "m-2", "gate", set())
        store.set_baseline(self.con, "m-3", "gate", set())
        self.assertEqual(store.known_red(self.con, "gate", 2), set())     # 太舊的不算
        self.assertEqual(store.known_red(self.con, "gate", 5), {"ancient"})

    def test_flaky_event_once_and_listed(self):
        self.mk(policy={"require_approval": False})
        store.set_baseline(self.con, "m-1", "gate", {"flk"})
        self.repo.add("a")
        self.go(60, until=lambda: self.state("a") == "merged")          # 合入後記一份新基準（無 flk）→ flk 時紅時綠
        n = self.con.execute("SELECT count(*) FROM events WHERE kind='flaky'").fetchone()[0]
        self.assertEqual(n, 1)
        from jet import ops
        self.assertEqual(ops.query_status(self.con)["flaky"], ["flk"])
        self.s._check_flaky()
        self.assertEqual(self.con.execute("SELECT count(*) FROM events WHERE kind='flaky'").fetchone()[0], 1)

    def test_docs_copy_not_counted_in_history(self):
        self.mk()
        store.set_baseline(self.con, "m-1", "gate", {"x"})
        store.set_baseline(self.con, "m-2", "gate", {"x"}, copied=True)
        store.set_baseline(self.con, "m-3", "gate", set(), copied=True)
        self.assertEqual(store.flaky_items(self.con, "gate", 5), set())   # 只有一次真量測


class Slots(Base):
    def test_max_selftest_slots_hard_cap(self):
        self.mk(delay=3, cores={"pool": "0-31", "max_selftest_slots": 3, "reserve_for_selftest": 32})
        for i in range(10):
            self.repo.add(f"n{i}")
        peak = 0
        for _ in range(12):
            self.s.tick()
            peak = max(peak, sum(1 for j in self.s.jobs.values() if j.kind == "selftest"))
        self.assertEqual(peak, 3)

    def test_physical_unit_config_uses_physical_pool(self):
        from jet.cores import PhysicalPool
        self.mk(cores={"pool": "0-31", "unit": "physical", "threads": "one"})
        self.assertIsInstance(self.s.pool, PhysicalPool)

    def test_auto_pause_reason_visible(self):
        self.mk(policy={"require_approval": False})
        self.repo.add("a")
        orig = self.ex.poll
        self.ex.poll = lambda h: (lambda r: r and type(r)("infra", None))(orig(h))
        self.go(30)
        from jet import ops
        st = ops.query_status(self.con)
        self.assertTrue(st["paused"])
        self.assertIn("連續基礎設施錯誤", st["pause_reason"])
        self.assertEqual(st["paused_by"], "scheduler")


class Overload(Base):
    def _timeout_overloaded(self, flag):
        orig = self.ex.poll

        def poll(h):
            r = orig(h)
            if r is not None and h.job.layer == "selftest":
                r.status, r.failed_items, r.overloaded, r.cpu_s = "timeout", [], flag, 12.0
            return r
        self.ex.poll = poll

    def test_overloaded_timeout_is_retried_not_rejected(self):
        self.mk()
        self.repo.add("a")
        self._timeout_overloaded(True)
        self.go(4)
        self.assertNotEqual(self.state("a"), "rejected")
        self.assertGreaterEqual(self.con.execute("SELECT count(*) FROM events WHERE kind='overload'").fetchone()[0], 1)

    def test_repeated_overload_timeout_finally_rejected(self):
        self.mk()
        self.repo.add("a")
        self._timeout_overloaded(True)
        self.go(30, until=lambda: self.state("a") == "rejected")
        self.assertIn("逾時", store.get_branch(self.con, "a")["reject_reason"])

    def test_plain_timeout_still_rejected(self):
        self.mk()
        self.repo.add("a")
        self._timeout_overloaded(False)
        self.go(10, until=lambda: self.state("a") == "rejected")
