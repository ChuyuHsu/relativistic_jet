"""先審後測＋外部來源（docs/CARD_FLOW.md）。每個測試名對應文檔的一條規則，見 docs/CARD_FLOW_TESTS.md。"""
import json
import unittest

from jet import store
from jet.source import SourcePoller, SourceError, parse_list, parse_payload
from tests.test_scheduler import Base


class CF(Base):
    """假清單：self.list 是來源上的內容；self.t 是假時間，每 tick 前進 16 秒（一定會輪詢）。"""

    def mk(self, delay=0, order="review_first", poll=True, **over):
        over.setdefault("git", {})["scope"] = "tracked"
        over.setdefault("policy", {})["order"] = order
        over["source"] = {"url": "http://127.0.0.1:9/x", "max_age_s": 600, "poll_seconds": 15}
        s = super().mk(delay, **over)
        self.t = [1000.0]
        self.list, self.src_err, self.fetches, self.drops = [], None, 0, []
        s.clock = lambda: self.t[0]

        def fetch():
            self.fetches += 1
            if self.src_err:
                raise SourceError(self.src_err)
            return parse_payload(json.loads(json.dumps({"entries": self.list, "drop_checks": self.drops})))
        s.source = SourcePoller("http://127.0.0.1:9/x", None, 15, 1, fetcher=fetch, clock=lambda: self.t[0],
                                threaded=False)
        self.store_emit()
        return s

    def store_emit(self):
        store.set_meta(self.con, "emit_machine_events", "1")

    def ent(self, branch, parent=None, ap=None, retest=0):
        for e in self.list:
            if e["branch"] == branch:
                self.list.remove(e)
                break
        e = {"branch": branch, "retest": retest}
        if parent:
            e["parent"] = parent
        if ap:
            e["approved_sha"] = ap
        self.list.append(e)

    def sha(self, n):
        return self.repo.branches[n]["sha"]

    def appr(self, n, parent=None, retest=0):
        self.ent(n, parent, self.sha(n), retest)

    def tick(self, n=1):
        for _ in range(n):
            self.t[0] += 16
            self.s.tick()

    def go(self, n=60, until=None):
        for _ in range(n):
            self.tick()
            if until and until():
                return
        if until:
            self.fail("條件沒達成：" + str({r["name"]: r["state"] for r in store.list_branches(self.con)}))

    def events(self, kind, branch=None):
        q = "SELECT * FROM events WHERE kind=?" + (" AND branch=?" if branch else "")
        return self.con.execute(q, (kind, branch) if branch else (kind,)).fetchall()

    def freeze_poll(self):
        self.s.source._next = 1e18          # 只剩「推 main 前重拉」會去拿新清單

    def selftests(self):
        return sum(1 for j in self.ex.started if j.layer == "selftest" and not j.only)

    def row(self, n):
        return store.get_branch(self.con, n)


# ---------------- 文檔第 4 節：一串卡怎麼走到 main ----------------
class ChainToMain(CF):
    def chain(self, names="abc", **kw):
        prev = None
        for n in names:
            self.repo.add(n, **kw.get(n, {}))
            self.appr(n, prev)
            prev = n

    def test_chain_merges_in_one_batch_in_order(self):
        self.mk()
        self.chain()
        self.go(until=lambda: self.merged() == {"a", "b", "c"})
        self.assertEqual(self.repo.main, "m0>a+b+c")
        self.assertEqual(len(self.repo.ff_log), 1)

    def test_red_is_C_a_and_b_merge(self):
        self.mk()
        self.chain(c={"gate_reds": {"x"}})
        self.go(until=lambda: self.state("c") == "rejected" and self.merged() == {"a", "b"})
        self.assertEqual(self.repo.main, "m0>a+b")

    def test_red_is_B_a_merges_c_waits_not_red_no_count(self):
        self.mk()
        self.chain(b={"gate_reds": {"x"}})
        self.go(until=lambda: self.state("b") == "rejected" and self.merged() == {"a"})
        self.go(10)
        self.assertNotEqual(self.state("c"), "rejected")
        self.assertEqual(self.row("c")["fail_count"], 0)
        self.assertEqual(self.events("rejected", "c"), [])
        self.assertEqual(self.row("b")["fail_count"], 1)

    def test_red_is_A_b_and_c_wait_not_red_no_count(self):
        self.mk()
        self.chain(a={"gate_reds": {"x"}})
        self.go(until=lambda: self.state("a") == "rejected")
        self.go(10)
        self.assertEqual(self.merged(), set())
        for n in "bc":
            self.assertNotEqual(self.state(n), "rejected")
            self.assertEqual(self.row(n)["fail_count"], 0)
            self.assertEqual(self.events("rejected", n), [])

    def test_child_does_not_selftest_until_parent_queued_or_merged(self):
        self.mk(delay=50)
        self.repo.add("a"); self.repo.add("b")
        self.ent("a", None, None)                 # 父卡還沒放行
        self.appr("b", "a")
        self.tick(8)
        self.assertEqual(self.state("b"), "pending")
        self.assertFalse(any(j.branch == "b" for j in self.s.jobs.values()))

    def test_parent_unapproved_returned_card_child_keeps_waiting(self):
        self.mk()
        self.repo.add("a"); self.repo.add("b")
        self.appr("a"); self.appr("b", "a")
        self.go(until=lambda: self.merged() == {"a", "b"})            # 先正常走一遍
        self.mk()
        self.repo.add("a"); self.repo.add("b")
        self.ent("a")                              # 父卡被還卡：清單上沒有 approved_sha
        self.appr("b", "a")
        self.tick(12)
        self.assertEqual(self.state("b"), "pending")
        self.assertEqual(self.state("a"), "pending")
        self.assertEqual(self.events("rejected"), [])
        self.appr("a")                             # 父卡重新做完放行 → 整串走完
        self.go(until=lambda: self.merged() == {"a", "b"})

    def test_chain_longer_than_batch_limit_front_first(self):
        self.mk(limits={"batch_max": 2})
        prev = None
        for n in "abcde":
            self.repo.add(n); self.appr(n, prev); prev = n
        self.go(until=lambda: self.merged() == set("abcde"))
        self.assertEqual(self.repo.main.split(">")[1:], ["a+b", "c+d", "e"])   # 前段先進、後段下一批

    def test_parent_merged_child_becomes_plain_approval_kept(self):
        self.mk(delay=2)
        self.repo.add("a"); self.repo.add("b")
        self.appr("a"); self.ent("b", "a", None)           # b 先沒放行
        self.go(until=lambda: self.state("a") == "merged")
        self.appr("b", "a")
        self.tick(2)
        self.assertIsNone(self.row("b")["parent"])
        self.assertTrue(store.is_approved(self.con, "b", self.row("b")["content_id"]))

    def test_branch_without_card_is_ignored(self):
        self.mk()
        self.repo.add("a"); self.repo.add("stray")
        self.appr("a")
        self.go(until=lambda: self.merged() == {"a"})
        self.assertIsNone(self.row("stray"))


# ---------------- 文檔第 6 節：每個狀態會遇到什麼事 ----------------
class StateEvents(CF):
    def test_no_selftest_without_approval(self):
        self.mk(delay=0)
        self.repo.add("a"); self.ent("a")
        self.tick(6)
        self.assertEqual(self.state("a"), "pending")
        self.assertEqual(self.selftests(), 0)

    def test_approval_arrives_then_selftest_and_merge(self):
        self.mk()
        self.repo.add("a"); self.ent("a")
        self.tick(3)
        self.appr("a")
        self.go(until=lambda: self.merged() == {"a"})

    def test_selftest_red_rejected(self):
        self.mk()
        self.repo.add("a", reds={"t1"}); self.appr("a")
        self.go(until=lambda: self.state("a") == "rejected")
        self.assertIn("t1", self.row("a")["reject_reason"])

    def test_selftest_timeout_rejected(self):
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.tick()
        self.ex.force["m0>a"] = "timeout"
        self.go(until=lambda: self.state("a") == "rejected")

    def test_conflict_with_main_rejected(self):
        self.mk()
        self.repo.add("a", conflict_main=True); self.appr("a")
        self.go(until=lambda: self.state("a") == "rejected")
        self.assertTrue(self.events("conflict", "a"))

    def test_revoke_during_selftest_stops_it_back_to_waiting(self):
        self.mk(delay=40)
        self.repo.add("a"); self.appr("a")
        self.go(until=lambda: self.state("a") == "selftesting")
        self.ent("a")                                       # 撤回放行
        self.tick(2)
        self.assertEqual(self.state("a"), "pending")
        self.assertTrue(self.ex.cancelled)
        self.assertEqual(self.s.jobs, {})
        self.tick(6)
        self.assertEqual(self.state("a"), "pending")          # 沒放行就不測

    def test_revoke_while_batching_goes_back_to_waiting_not_merged(self):
        self.mk(delay=1)
        self.repo.add("a"); self.appr("a")
        self.go(until=lambda: self.state("a") == "batching")
        self.ent("a")                                       # 撤回放行
        self.tick(4)
        self.assertEqual(self.repo.ff_log, [])
        self.assertEqual(self.state("a"), "selftest_green")      # 自測結果還在，等重新放行
        self.appr("a")
        self.go(until=lambda: self.merged() == {"a"})

    def test_own_segment_changed_means_superseded_once_and_unapproved(self):
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.go(until=lambda: self.state("a") in ("selftest_green", "queued", "batching"))
        self.repo.push_new("a", files=("src/other.py",))          # 自己那一段又改了
        self.tick(4)
        self.assertEqual(len(self.events("superseded", "a")), 1)
        self.assertEqual(self.state("a"), "pending")
        self.assertFalse(store.is_approved(self.con, "a", self.row("a")["content_id"]))
        ev = self.events("superseded", "a")[0]
        self.assertEqual(ev["approved_sha"], "a-1")               # 事件帶它依據的放行版本
        self.assertEqual(ev["commit_sha"], "a-2")
        self.tick(4)
        self.assertEqual(len(self.events("superseded", "a")), 1)   # 同一個新版本只發一次
        self.assertEqual(self.selftests(), 1)                       # 沒再重新放行就沒再測
        self.appr("a")                                            # 重新送審、放行新版本
        self.go(until=lambda: self.merged() == {"a"})

    def test_pure_rebase_keeps_approval_no_superseded_no_retest_needed(self):
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.go(until=lambda: self.state("a") in ("selftest_green", "queued", "batching"))
        self.repo.push_new("a")                                  # 內容一樣，只是換了提交
        self.tick(3)
        self.assertEqual(self.events("superseded", "a"), [])
        self.assertTrue(store.is_approved(self.con, "a", self.row("a")["content_id"]))
        self.go(until=lambda: self.merged() == {"a"})

    def test_unresolvable_approved_sha_that_is_remote_tip_or_before_first_scan_is_not_superseded(self):
        """重啟後放行版本的內容 id 暫時算不出來：放行版本就是遠端現在的頭、或本程序還沒掃過遠端，都不能發 superseded、不能撤放行。"""
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.go(until=lambda: self.state("a") in ("selftest_green", "queued", "batching"))
        row = self.row("a")
        self.repo.content_id = lambda sha, base: (_ for _ in ()).throw(RuntimeError("物件暫時取不到"))
        for tips in (None, {"a": "a-9"}):                         # ①重啟後沒掃過 ②放行版本＝遠端頭（資料庫的頭還是舊的）
            self.s._tips = tips; self.s._unres.clear(); self.s._unres_since.clear()
            t = [1000.0]
            self.s.clock = lambda: t[0]
            for _ in range(10):
                t[0] += 100                                      # 遠超過寬限
                self.assertEqual(self.s._judge_approved(row, "a-9"), "unknown")
        self.assertEqual(self.events("superseded", "a"), [])

    def test_rejected_then_pure_rebase_goes_back_with_same_approved_sha(self):
        self.mk()
        self.repo.add("a", conflict_main=True); self.appr("a")
        self.go(until=lambda: self.state("a") == "rejected")
        self.repo.push_new("a", conflict_main=False)              # 重新接到最新主線，內容沒變
        self.tick(1)
        self.ent("a", None, "a-1")                               # 看板上仍是原本的放行版本
        self.go(until=lambda: self.merged() == {"a"})
        pushed = self.events("pushed", "a")
        self.assertTrue(pushed and pushed[-1]["approved_sha"] == "a-1")
        self.assertEqual(self.events("superseded", "a"), [])

    def test_rebase_that_changed_own_part_needs_new_approval(self):
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.tick(1)
        self.repo.push_new("a", reds=())
        self.repo.branches["a"]["files"] = ["src/changed.py"]
        self.tick(3)
        self.assertEqual(len(self.events("superseded", "a")), 1)

    def test_retest_clears_rejected_and_keeps_approval(self):
        self.mk()
        self.repo.add("a", conflict_main=True); self.appr("a")
        self.go(until=lambda: self.state("a") == "rejected")
        self.repo.branches["a"]["conflict_main"] = False
        self.tick(3)
        self.assertEqual(self.state("a"), "rejected")             # 沒按重測：不自己重跑
        self.appr("a", retest=1)
        self.go(until=lambda: self.merged() == {"a"})

    def test_retest_clears_halted(self):
        self.mk(limits={"double_fail_halt": 1})
        self.repo.add("a", gate_reds={"x"}); self.appr("a")
        self.go(until=lambda: self.state("a") == "halted")
        self.repo.branches["a"]["gate_reds"] = set()               # 不是它的問題（例如機器抖動）
        self.tick(4)
        self.assertEqual(self.state("a"), "halted")                # 沒按重測：不自己重跑
        self.appr("a", retest=1)
        self.go(until=lambda: self.merged() == {"a"})

    def test_retest_counter_not_larger_does_nothing(self):
        self.mk()
        self.repo.add("a", conflict_main=True); self.appr("a", retest=3)
        self.go(until=lambda: self.state("a") == "rejected")
        self.repo.branches["a"]["conflict_main"] = False
        self.appr("a", retest=3)
        self.tick(5)
        self.assertEqual(self.state("a"), "rejected")

    def test_merged_and_dropped_ignored(self):
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.go(until=lambda: self.merged() == {"a"})
        self.list = []                                            # 已合併的卡不在清單上：不影響
        self.tick(3)
        self.assertEqual(self.state("a"), "merged")

    # ---- 父卡的事 ----
    def test_parent_selftest_red_child_waits_not_red(self):
        self.mk()
        self.repo.add("a", reds={"x"}); self.repo.add("b")
        self.appr("a"); self.appr("b", "a")
        self.go(until=lambda: self.state("a") == "rejected")
        self.tick(8)
        self.assertEqual(self.state("b"), "pending")
        self.assertEqual(self.events("rejected", "b"), [])
        self.assertEqual(self.row("b")["fail_count"], 0)

    def test_parent_rebased_child_own_part_same_keeps_approval(self):
        self.mk()
        self.repo.add("a"); self.repo.add("b")
        self.appr("a"); self.appr("b", "a")
        self.go(until=lambda: self.state("a") in ("queued", "batching", "merged"))
        self.repo.push_new("a", files=("src/new.py",))            # 父卡改了內容
        self.ent("a", None, self.sha("a"))                        # 重新放行
        self.repo.push_new("b")                                   # 子卡重新接上去，自己那一段沒變
        self.tick(3)
        self.assertEqual(self.events("superseded", "b"), [])
        self.assertTrue(store.is_approved(self.con, "b", self.row("b")["content_id"]))
        self.go(until=lambda: self.merged() == {"a", "b"})

    def test_parent_content_changed_child_goes_back_to_waiting(self):
        self.mk(delay=3)
        self.repo.add("a"); self.repo.add("b")
        self.appr("a"); self.appr("b", "a")
        self.go(until=lambda: self.state("b") in ("selftesting", "selftest_green", "queued", "batching"))
        self.repo.push_new("a", files=("src/new.py",))
        self.tick(2)
        self.assertEqual(self.state("b"), "pending")

    def test_parent_dropped_child_blocked_event_once_then_reparent_to_main(self):
        self.mk()
        self.repo.add("a"); self.repo.add("b")
        self.appr("a"); self.appr("b", "a")
        self.tick(1)
        self.list = [e for e in self.list if e["branch"] != "a"]   # 父卡被放棄：不在清單上
        for e in self.list:
            if e["branch"] == "b":
                e["parent_status"] = "dropped"
        self.tick(6)
        self.assertEqual(len(self.events("parent_blocked", "b")), 1)
        self.assertNotIn(self.state("b"), ("selftesting", "merged"))
        self.tick(4)
        self.assertEqual(len(self.events("parent_blocked", "b")), 1)   # 只發一次
        self.appr("b")                                             # 管理者把父卡改成接 main：放行保留
        self.go(until=lambda: "b" in self.merged())
        self.assertEqual(self.events("superseded", "b"), [])

    def test_parent_changed_in_list_keeps_approval_restarts_from_pending(self):
        self.mk(delay=2)
        self.repo.add("a"); self.repo.add("b"); self.repo.add("c")
        self.appr("a"); self.appr("c"); self.appr("b", "a")
        self.go(until=lambda: self.state("b") in ("selftesting", "selftest_green", "queued", "batching"))
        self.appr("b", "c")                                       # 管理者改父卡
        self.tick(2)
        self.assertEqual(self.row("b")["parent"], "c")
        self.assertTrue(store.is_approved(self.con, "b", self.row("b")["content_id"]))
        self.assertEqual(self.events("superseded", "b"), [])
        self.go(until=lambda: self.merged() == {"a", "b", "c"})


# ---------------- 文檔第 5、7 節：規則 ----------------
class Rules(CF):
    def test_events_carry_approved_sha_retest_and_increasing_ids(self):
        self.mk()
        self.repo.add("a", reds={"t"}); self.appr("a", retest=2)
        self.go(until=lambda: self.state("a") == "rejected")
        evs = self.con.execute("SELECT * FROM events WHERE branch='a' ORDER BY id").fetchall()
        self.assertTrue(evs)
        for e in evs:
            if e["kind"] in ("tracked", "untracked"):
                continue
            self.assertEqual(e["approved_sha"], "a-1")
            self.assertEqual(e["retest"], 2)
        ids = [e["id"] for e in evs]
        self.assertEqual(ids, sorted(set(ids)))

    def test_notify_payload_has_approved_sha_and_retest(self):
        from jet import notify
        self.mk()
        self.repo.add("a", reds={"t"}); self.appr("a", retest=1)
        self.go(until=lambda: self.state("a") == "rejected")
        rows = self.con.execute("SELECT id,kind,branch,detail,at,commit_sha,approved_sha,retest FROM events "
                                "WHERE kind='rejected'").fetchall()
        ev = [notify._enrich(self.con, r) for r in rows]
        out = notify.build_payload(ev, 1.0)["events"][0]
        self.assertEqual(out["approved_sha"], "a-1")
        self.assertEqual(out["retest"], 1)

    def test_repull_before_push_revoked_member_and_children_stay(self):
        self.mk(delay=1)
        for n, p in (("a", None), ("b", "a"), ("c", "b")):
            self.repo.add(n); self.appr(n, p)
        self.go(until=lambda: self.state("c") == "batching" and self.state("a") == "batching")
        self.freeze_poll()
        self.ent("c", "b", None)                                  # 推前 c 被撤回放行
        self.go(until=lambda: self.merged() == {"a", "b"})
        self.assertEqual(self.repo.main, "m0>a+b")                # 最大不含它的已測綠點
        self.assertNotEqual(self.state("c"), "merged")

    def test_repull_before_push_revoked_in_the_middle_whole_requeue_not_push(self):
        self.mk(delay=1)
        for n, p in (("a", None), ("b", "a"), ("c", "b")):
            self.repo.add(n); self.appr(n, p)
        self.go(until=lambda: self.state("c") == "batching" and self.state("a") == "batching")
        self.freeze_poll()
        self.ent("a", None, None)                                 # 第一條被撤回：不能推任何含它的點
        self.tick(6)
        self.assertEqual(self.repo.ff_log, [])
        self.assertNotIn(self.state("a"), ("merged",))
        for n in "abc":
            self.assertNotEqual(self.state(n), "merged")
        self.ent("a", None, self.sha("a"))                        # 重新放行 → 後來整串一起進
        self.freeze_poll(); self.s.source._next = 0
        self.go(until=lambda: self.merged() == {"a", "b", "c"})

    def test_repull_before_push_content_changed_is_not_pushed(self):
        self.mk(delay=1)
        self.repo.add("a"); self.repo.add("b")
        self.appr("a"); self.appr("b")
        self.go(until=lambda: self.state("a") == "batching" and self.state("b") == "batching")
        self.freeze_poll()
        self.ent("b", None, "b-0")                                # 看板說放行的版本不是現在這個
        self.go(until=lambda: "a" in self.merged())
        self.assertNotIn("b", self.merged())

    def test_stale_list_tests_but_does_not_push(self):
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.tick(1)
        self.src_err = "連不上"
        self.t[0] += 0
        for _ in range(60):                                       # 16 秒一輪，過了 600 秒
            self.tick()
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.tick(1)
        self.go(until=lambda: self.state("a") in ("queued", "batching"))
        self.freeze_poll()
        self.src_err = "連不上"
        self.s.source._next = 0
        self.t[0] += 1000                                         # 清單超過 600 秒沒更新
        self.tick(8)
        self.assertEqual(self.repo.ff_log, [])
        self.assertTrue(self.events("queue_error"))
        self.src_err = None
        self.s.source._next = 0
        self.go(until=lambda: self.merged() == {"a"})             # 來源恢復後自己繼續

    def test_fetch_failure_keeps_status_quo(self):
        self.mk(delay=30)
        self.repo.add("a"); self.appr("a")
        self.go(until=lambda: self.state("a") == "selftesting")
        self.src_err = "HTTP 500"
        self.tick(6)
        self.assertEqual(self.state("a"), "selftesting")          # 沒有取消登記、沒有撤回放行
        self.assertTrue(store.is_tracked(self.con, "a"))
        self.assertTrue(store.is_approved(self.con, "a", self.row("a")["content_id"]))
        self.assertEqual(len(self.events("source_error")), 1)     # 同樣的錯只記一次
        self.src_err = None
        self.tick(2)
        self.assertEqual(self.state("a"), "selftesting")

    def test_empty_list_is_real(self):
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.tick(2)
        self.list = []
        self.tick(3)
        self.assertIsNone(self.row("a"))
        self.assertEqual(store.tracked_names(self.con), set())

    def test_bad_format_is_error_not_change(self):
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.tick(2)
        self.list = [{"nope": 1}]
        self.tick(3)
        self.assertTrue(store.is_tracked(self.con, "a"))
        self.assertTrue(self.events("source_error"))

    def test_cycle_in_list_rejected_by_parser(self):
        with self.assertRaises(SourceError):
            parse_list([{"branch": "a", "parent": "b"}, {"branch": "b", "parent": "a"}])
        with self.assertRaises(SourceError):
            parse_list([{"branch": "a", "parent": "a"}])

    def test_docs_only_card_merges_directly_after_approval(self):
        self.mk(commands={"test": ["t"], "classify": ["python3", "-c", "print()"]})
        self.s.cfg.classify_cmd = None
        self.repo.add("d", files=("docs/x.md",))
        self.appr("d")
        self.tick(1)
        self.con.execute("UPDATE branches SET docs_only=1 WHERE name='d'")
        self.go(until=lambda: self.merged() == {"d"})
        self.assertEqual(self.selftests(), 0)                     # 沒測就合

    def test_parallel_mode_without_source_tests_before_approval(self):
        Base.mk(self)                                              # 不分先後、本機模式：一推上來就測
        self.repo.add("a")
        for _ in range(8):
            self.s.tick()
        self.assertEqual(self.state("a"), "selftest_green")        # 沒放行也先測
        store.approve(self.con, "a", "t")
        for _ in range(30):
            self.s.tick()
        self.assertEqual(self.merged(), {"a"})

    def test_source_requires_review_first(self):
        from jet import config
        with self.assertRaises(config.ConfigError):
            Base.mk(self, git={"scope": "tracked"}, source={"url": "http://127.0.0.1:9/x"})

    def test_local_api_approval_without_source_in_review_first(self):
        self.mk()
        self.s.source = None
        self.repo.add("a")
        self.s.cfg.scope = "all"
        self.tick(5)
        self.assertEqual(self.state("a"), "pending")             # 本機模式也是先審後測
        store.approve(self.con, "a", "t")
        self.go(until=lambda: self.merged() == {"a"})


class AuditFixes(CF):
    def test_h1_parent_branch_not_pushed_child_never_merges_alone(self):
        self.mk()
        self.repo.add("b")                                         # 父卡 a 的分支還沒推上來
        self.ent("b", "a", self.sha("b"))
        for e in self.list:
            e["parent_status"] = "open"
        self.tick(20)
        self.assertEqual(self.merged(), set())
        self.assertEqual(self.selftests(), 0)                      # 不測、不進批
        self.assertEqual(self.events("parent_blocked"), [])        # open 只是等，不叫管理者
        self.repo.add("a")                                         # 父卡推上來、放行：整串進
        self.appr("a"); self.appr("b", "a")
        self.go(until=lambda: self.merged() == {"a", "b"})

    def test_h1_parent_card_returned_child_waits_and_keeps_parent(self):
        self.mk()
        self.repo.add("a"); self.repo.add("b")
        self.appr("a"); self.appr("b", "a")
        self.tick(1)
        self.list = [e for e in self.list if e["branch"] != "a"]   # 父卡被還卡：清單上沒有它
        self.ent("b", "a", self.sha("b"))
        self.list[-1]["parent_status"] = "open"
        self.tick(10)
        self.assertNotIn("b", self.merged())
        self.assertEqual(self.events("parent_blocked"), [])

    def test_h2_parent_updated_child_not_rebased_is_rejected_then_rebase_continues(self):
        self.mk()
        self.repo.add("a"); self.repo.add("b")
        self.appr("a"); self.appr("b", "a")
        self.tick(1)
        old_a = self.sha("a")
        self.repo.push_new("a")                                    # 父卡改版（內容一樣只是換提交）
        self.appr("a")
        self.repo.broken.add((self.sha("a"), self.sha("b")))       # 子卡沒接在新版父卡上
        self.go(until=lambda: self.state("b") == "rejected")
        self.assertIn("重新接到父卡", self.row("b")["reject_reason"])
        self.assertNotIn("b", self.merged())
        self.repo.push_new("b")                                    # 重新接上，自己那一段沒變
        self.repo.broken.clear()
        self.appr("b", "a")
        self.go(until=lambda: self.merged() == {"a", "b"})
        self.assertEqual(self.events("superseded", "b"), [])

    def test_m1_event_carries_approved_sha_of_test_start(self):
        self.mk(delay=3)
        self.repo.add("a", reds={"t"})
        self.appr("a", retest=0)
        self.go(until=lambda: self.state("a") == "selftesting")
        self.repo.push_new("a", reds={"t"})                        # 純 rebase：看板改放行新版本、重測加一
        self.appr("a", retest=0)
        self.tick(1)
        self.go(until=lambda: bool(self.events("rejected", "a")) or self.state("a") == "rejected")
        for e in self.events("tested_red", "a") + self.events("rejected", "a"):
            self.assertEqual(e["approved_sha"], "a-2")             # 舊放行的結果丟掉；留下的事件都是新放行測出來的

    def test_m8_repull_must_succeed_before_push(self):
        self.mk(delay=1)
        self.repo.add("a"); self.appr("a")
        self.go(until=lambda: self.state("a") == "batching")
        self.freeze_poll()
        self.src_err = "連不上"                                    # 重拉失敗
        self.tick(6)
        self.assertEqual(self.repo.ff_log, [])                     # 不能用舊清單推
        self.assertTrue(self.events("queue_error"))
        self.src_err = None
        self.go(until=lambda: self.merged() == {"a"})

    def test_m8_old_list_stops_opening_new_batches(self):
        self.mk()
        self.repo.add("a"); self.appr("a")
        self.tick(1)
        self.src_err = "down"
        self.t[0] += 2000                                          # 清單超過 600 秒沒更新成功
        self.tick(8)
        self.assertEqual(self.repo.ff_log, [])
        self.assertEqual(self.state("a"), "queued")                # 自測照做，但不開新批
        self.src_err = None
        self.go(until=lambda: self.merged() == {"a"})

    def test_m9_event_ids_carry_instance_prefix(self):
        from jet import notify
        self.mk()
        self.repo.add("a", reds={"t"}); self.appr("a")
        self.go(until=lambda: self.state("a") == "rejected")
        rows = self.con.execute("SELECT id,kind,branch,detail,at,commit_sha,approved_sha,retest FROM events").fetchall()
        out = notify.build_payload([notify._enrich(self.con, r) for r in rows], 1.0)["events"]
        iid = store.get_meta(self.con, "instance_id")
        self.assertTrue(iid)
        self.assertTrue(all(str(e["id"]).startswith(iid + ":") for e in out))

    def test_l1_parent_approval_revoked_child_not_counted_in_pipeline(self):
        self.mk(delay=0)
        self.repo.add("a"); self.repo.add("b")
        self.appr("a"); self.ent("b", "a", None)
        self.go(until=lambda: self.state("a") in ("batching", "queued"))
        self.appr("b", "a")
        self.ent("a")                                             # 父卡放行被撤
        self.tick(6)
        self.assertEqual(self.state("b"), "pending")

    def test_l2_dup_branch_in_list_does_not_kill_whole_list(self):
        self.mk()
        self.repo.add("a")
        self.list = [{"branch": "a", "retest": 0, "approved_sha": "a-1"}, {"branch": "a", "retest": 0}]
        self.go(until=lambda: self.merged() == {"a"})


class StaleResult(CF):
    def test_n1_selftest_result_from_old_approval_is_dropped(self):
        self.mk(delay=3)
        self.repo.add("a", reds={"t"}); self.appr("a")
        self.go(until=lambda: self.state("a") == "selftesting")
        self.repo.push_new("a", reds={"t"})                       # 單純 rebase：內容一樣
        self.appr("a")                                            # 重新放行新版本
        old_ap = "a-1"
        for _ in range(30):
            self.tick(1)
            if self.state("a") == "rejected":
                break
        self.assertEqual(self.state("a"), "rejected")             # 新放行的版本自己測出紅，才算退回
        for e in self.events("rejected", "a"):
            self.assertEqual(e["approved_sha"], "a-2")
            self.assertNotEqual(e["approved_sha"], old_ap)
        self.assertEqual(self.selftests(), 2)                     # 舊的丟掉、新的重測一次

    def test_n1_selftest_drop_does_not_reject_when_new_run_green(self):
        self.mk(delay=3)
        self.repo.add("a", reds={"t"}); self.appr("a")
        self.go(until=lambda: self.state("a") == "selftesting")
        self.repo.push_new("a", reds=())                          # 內容其實變了（紅項沒了）→ superseded，不是這條路徑
        self.appr("a")
        self.go(until=lambda: self.merged() == {"a"})

    def test_n1_batch_culprit_from_old_approval_is_not_rejected(self):
        self.mk(delay=3)
        self.repo.add("a", gate_reds={"g"}); self.appr("a")
        self.go(until=lambda: self.state("a") == "batching")
        self.repo.push_new("a", gate_reds={"g"})
        self.appr("a")                                            # 換了放行版本、內容相同
        self.freeze_poll()
        self.s.source._next = 0
        rej = []
        for _ in range(6):
            self.tick(1)
            rej += [e["approved_sha"] for e in self.events("rejected", "a")]
            if rej:
                break
        self.assertNotIn("a-1", rej)                              # 舊放行測出來的紅不能當成退回
        self.go(until=lambda: self.state("a") == "rejected")
        for e in self.events("rejected", "a"):
            self.assertEqual(e["approved_sha"], "a-2")


class FirstPull(CF):
    def test_no_stale_error_before_first_successful_pull(self):
        self.mk()
        self.repo.add("a")
        self.src_err = "還沒好"                                   # 重啟後第一次拉取還沒成功
        self.tick(10)
        self.assertEqual(self.events("queue_error"), [])         # 不算過期，不發事件
        self.assertEqual(self.repo.ff_log, [])
        self.src_err = None
        self.appr("a")
        self.go(until=lambda: self.merged() == {"a"})


class BoardPriority(CF):
    """清單帶 priority（小的先）→ 自測照這個順序派；手動 jet priority（大的先）仍然優先。"""
    def _first_started(self, prios, manual=None):
        self.mk(cores={"pool": "0-31", "max_selftest_slots": 1, "reserve_for_selftest": 32})
        names = list(prios)
        for n in names:
            self.repo.add(n)
            self.ent(n)                                   # 先上清單但還沒放行，讓排隊器先掃到
        self.tick(2)
        for n, v in (manual or {}).items():
            store.set_priority(self.con, n, v, "t")
        for n in names:                                   # 同一輪放行，順序全看排序鍵
            self.appr(n)
            for e in self.list:
                if e["branch"] == n and prios[n] is not None:
                    e["priority"] = prios[n]
        self.tick(1)
        return [j.workdir.split(">")[-1] for j in self.ex.started if j.layer == "selftest" and not j.only][:1]

    def test_smaller_board_priority_first(self):
        first = self._first_started({"a": 100, "b": 5, "c": 50})
        self.assertEqual(first, ["b"])

    def test_missing_priority_goes_last(self):
        first = self._first_started({"a": None, "b": 900})
        self.assertEqual(first, ["b"])

    def test_manual_priority_wins_then_board_breaks_ties(self):
        first = self._first_started({"a": 100, "b": 5, "c": 50}, manual={"a": 1})
        self.assertEqual(first, ["a"])


class UnknownGrace(CF):
    def test_unresolvable_approved_sha_becomes_superseded_after_grace(self):
        self.mk()
        self.repo.add("a")
        self.ent("a", None, "ghost-v1")                   # 放行的版本遠端拿不到（已被新提交蓋掉）
        self.tick(3)
        self.assertEqual(self.events("superseded", "a"), [])   # 還在寬限期：先等 fetch
        self.assertEqual(self.state("a"), "pending")
        self.tick(10)                                       # 每輪 16 秒，超過 120 秒
        self.assertEqual(len(self.events("superseded", "a")), 1)
        self.tick(5)
        self.assertEqual(len(self.events("superseded", "a")), 1)


class DropCheckFlowTest(CF):
    """清單帶 drop_checks → 排隊器查（只讀）→ drop_checked 事件。"""

    def setUp(self):
        super().setUp()
        self.s = self.mk()

    def res(self, **kw):
        base = dict(branch="x", target="main", branch_sha="s1", target_sha="m", main_sha="m",
                    unmerged=True, covered=False, missing_count=1, missing_files=["a.py"])
        base.update(kw)
        return base

    def test_payload_object_form(self):
        ents, drops = parse_payload({"entries": [{"branch": "a"}],
                                     "drop_checks": [{"card": "C1", "branch": "x", "superseded_by_branch": None},
                                                     {"card": "C1", "branch": "dup"}]})
        self.assertEqual(len(ents), 1)
        self.assertEqual(drops, [{"card": "C1", "branch": "x", "superseded_by_branch": None}])
        e2, _ = parse_payload({"cards": [{"branch": "b"}], "drop_checks": []})     # 看板實際用的鍵名
        self.assertEqual(e2[0]["branch"], "b")
        with self.assertRaises(SourceError):
            parse_payload({"entries": [], "drop_checks": [{"card": "C"}]})

    def test_emits_once_and_again_when_result_changes(self):
        self.repo.drop_res["x"] = self.res()
        self.drops = [{"card": "C1", "branch": "x", "superseded_by_branch": "y"}]
        self.tick(3)
        ev = self.events("drop_checked", "x")
        self.assertEqual(len(ev), 1)
        d = json.loads(ev[0]["detail"])
        self.assertEqual((d["card"], d["covered"], d["unmerged"], d["missing_files"]), ("C1", False, True, ["a.py"]))
        self.assertEqual(self.repo.drop_calls[0], ("x", "y"))
        self.tick(3)
        self.assertEqual(len(self.events("drop_checked", "x")), 1)     # 結論沒變，不重發
        self.repo.drop_res["x"] = self.res(covered=True, unmerged=False, missing_count=0, missing_files=[],
                                           main_sha="m2")
        self.repo.main = "m2"
        self.tick(2)
        self.assertEqual(len(self.events("drop_checked", "x")), 2)

    def test_dropping_at_echoed_and_new_round_reemits(self):
        from jet import notify
        self.repo.drop_res["x"] = self.res()
        self.drops = [{"card": "C1", "branch": "x", "dropping_at": 111}]
        self.tick(2)
        ev = self.events("drop_checked", "x")
        self.assertEqual(json.loads(ev[0]["detail"])["dropping_at"], 111)
        p = notify.build_payload([notify._enrich(self.con, ev[0])], 1.0)["events"][0]
        self.assertEqual(p["dropping_at"], 111)
        self.drops = [{"card": "C1", "branch": "x", "dropping_at": 222}]   # 重新放棄一次：結論同也要回報
        self.tick(2)
        self.assertEqual(len(self.events("drop_checked", "x")), 2)

    def test_not_marked_done_when_event_not_written(self):
        self.repo.drop_res["x"] = self.res()
        self.drops = [{"card": "C1", "branch": "x"}]
        store.set_meta(self.con, "emit_machine_events", "0")      # 還沒有目的地要機器事件
        self.tick(3)
        self.assertEqual(len(self.events("drop_checked")), 0)
        store.set_meta(self.con, "emit_machine_events", "1")
        self.tick(2)
        self.assertEqual(len(self.events("drop_checked", "x")), 1)  # 補上設定後補發，不會被「已查過」吞掉

    def test_second_drop_round_same_result_reemits(self):
        self.repo.drop_res["x"] = self.res()
        self.drops = [{"card": "C1", "branch": "x", "dropping_at": 1}]
        self.tick(2)
        self.drops = [{"card": "C1", "branch": "x", "dropping_at": 2, "superseded_by_branch": None}]
        self.tick(2)
        self.assertEqual(len(self.events("drop_checked", "x")), 2)

    def test_long_file_list_not_broken_by_detail_truncation(self):
        """檔案清單很長時 detail 會超過 500 字：結構化欄位照樣要齊。"""
        from jet import notify
        files = [f"some/deep/dir/with/long/path/file_{i:03d}.gd" for i in range(20)]
        self.repo.drop_res["x"] = self.res(missing_count=20, missing_files=files)
        self.drops = [{"card": "C1", "branch": "x", "dropping_at": 5}]
        self.tick(2)
        row = self.events("drop_checked", "x")[0]
        self.assertGreater(len(row["detail"]), 500)
        p = notify.build_payload([notify._enrich(self.con, row)], 1.0)["events"][0]
        self.assertEqual((p["card"], p["target"], p["missing_count"], p["dropping_at"]), ("C1", "main", 20, 5))
        self.assertEqual(len(p["missing_files"]), 20)
        self.assertIn("20", p["detail"])

    def test_git_error_emits_nothing_and_retries(self):
        self.repo.drop_res["x"] = RuntimeError("boom")
        self.drops = [{"card": "C1", "branch": "x"}]
        self.tick(2)
        self.assertEqual(len(self.events("drop_checked")), 0)
        self.repo.drop_res["x"] = self.res()
        self.tick(2)
        self.assertEqual(len(self.events("drop_checked", "x")), 1)

    def test_notify_payload_has_fields(self):
        from jet import notify
        self.repo.drop_res["x"] = self.res()
        self.drops = [{"card": "C1", "branch": "x"}]
        self.tick(2)
        row = self.events("drop_checked", "x")[0]
        e = notify._enrich(self.con, row)
        p = notify.build_payload([e], 1.0)["events"][0]
        self.assertEqual((p["card"], p["target"], p["covered"], p["unmerged"], p["missing_count"]),
                         ("C1", "main", False, True, 1))
        self.assertEqual(p["missing_files"], ["a.py"])


class SourcePollerTest(unittest.TestCase):
    def test_parse_ok(self):
        out = parse_list([{"branch": "a"}, {"branch": "b", "parent": "a", "approved_sha": "x", "retest": 2}])
        self.assertEqual(out[1]["retest"], 2)
        self.assertIsNone(out[0]["approved_sha"])

    def test_parse_errors(self):
        self.assertEqual(len(parse_list([{"branch": "a"}, {"branch": "a"}])), 1)   # 同名重複：留第一筆，不拒收整份
        for bad in ({}, [1], [{"branch": ""}], [{"branch": "a", "parent_status": "x"}],
                    [{"branch": "a", "retest": -1}], [{"branch": "a", "approved_sha": 3}]):
            with self.assertRaises(SourceError):
                parse_list(bad)

    def test_poller_keeps_last_good_and_reports_error_once(self):
        t, state = [0.0], {"fail": False}

        def f():
            if state["fail"]:
                raise SourceError("壞了")
            return [{"branch": "a", "parent": None, "approved_sha": None, "retest": 0}]
        p = SourcePoller("http://127.0.0.1:1/x", None, 5, 1, fetcher=f, clock=lambda: t[0], threaded=False)
        self.assertIsNone(p.poll())
        self.assertEqual(len(p.entries), 1)
        state["fail"] = True
        t[0] += 6
        self.assertEqual(p.poll(), "壞了")
        t[0] += 6
        self.assertIsNone(p.poll())                  # 同樣的錯不重報
        self.assertEqual(len(p.entries), 1)          # 保留上次成功的
        self.assertEqual(p.age(), 12)

    def test_http_fetch_against_local_server(self):
        import http.server, threading, os, tempfile
        from jet.source import http_fetch
        seen = {}

        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen["auth"] = self.headers.get("Authorization")
                body = json.dumps([{"branch": "x", "retest": 1}]).encode()
                if self.path == "/bad":
                    self.send_response(500); self.end_headers(); return
                self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a): pass
        srv = http.server.HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.shutdown)
        with tempfile.TemporaryDirectory() as d:
            tf = os.path.join(d, "tok")
            open(tf, "w").write("sekret\n")
            url = f"http://127.0.0.1:{srv.server_address[1]}"
            self.assertEqual(http_fetch(url + "/ok", tf, 3)[0]["retest"], 1)
            self.assertEqual(seen["auth"], "Bearer sekret")
            with self.assertRaises(SourceError):
                http_fetch(url + "/bad", tf, 3)


if __name__ == "__main__":
    unittest.main()

