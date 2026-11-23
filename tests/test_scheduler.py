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
        self.s = Scheduler(self.cfg, self.con, self.repo, self.ex, clock=lambda: 1000.0)
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

    def test_batch_timeout_requeues(self):
        self.mk(policy={"require_approval": False}, delay=2)
        self.repo.add("a")
        self.go(40, until=lambda: self.state("a") == "batching")
        self.ex.force["m0>a"] = "timeout"
        self.go(40, until=lambda: self.con.execute(
            "SELECT count(*) FROM batches WHERE status='aborted'").fetchone()[0] >= 1)
        self.assertIn(self.state("a"), ("queued", "batching"))
        self.ex.force.clear()
        self.go(until=lambda: self.merged() == {"a"})


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
