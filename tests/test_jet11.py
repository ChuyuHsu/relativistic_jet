"""JET-11：放行提交已在 main 就標已合入、快照帶 reject_reason、只改 .md 的提交不當嫌疑、計時類項目比相對值。"""
import json
import os
import unittest
from types import SimpleNamespace as NS

from jet import store
from jet.executor import JobResult
from jet.sched_common import docs_only_files, is_doc_path
from jet.states import Event
from tests.test_scheduler import Base

SHA = "a" * 40


class InMainTest(Base):
    def _tracked_branch(self):
        s = self.mk(workflow={"mode": "direct_merge"})
        self.repo.add("a")
        self.go(4)
        self.assertIn(self.state("a"), ("pending", "selftesting", "selftest_green", "queued"))
        return s

    def test_approved_commit_already_in_main_marks_merged(self):
        s = self._tracked_branch()
        store.set_meta(self.con, "emit_machine_events", "1")
        s._entries = {"a": {"branch": "a", "parent": None, "approved_sha": SHA, "retest": 0, "priority": None}}
        self.repo.is_ancestor = lambda a, b: a == SHA
        s.main = "mainsha"
        s._apply_source_approvals()
        row = store.get_branch(self.con, "a")
        self.assertEqual((row["state"], row["merged_sha"]), ("merged", SHA))
        ev = self.con.execute("SELECT detail FROM events WHERE kind='merged' AND branch='a'").fetchone()
        self.assertIsNotNone(ev)
        self.assertEqual(json.loads(ev[0])["merged_sha"], SHA)

    def test_not_in_main_is_untouched(self):
        s = self._tracked_branch()
        before = self.state("a")
        s._entries = {"a": {"branch": "a", "parent": None, "approved_sha": SHA, "retest": 0, "priority": None}}
        self.repo.is_ancestor = lambda a, b: False
        s.main = "mainsha"
        s._apply_source_approvals()
        self.assertNotEqual(self.state("a"), "merged")
        self.assertEqual(before, before)

    def test_snapshot_lists_untracked_in_main_as_merged_and_has_reject_reason(self):
        s = self._tracked_branch()
        store.apply_event(self.con, "a", Event.CONFLICT_MAIN, reject_reason="撞檔") if self.state("a") == "pending" else None
        s._entries = {"ghost": {"branch": "ghost", "approved_sha": SHA, "retest": 2},
                      "other": {"branch": "other", "approved_sha": "b" * 40, "retest": 0}}
        self.repo.is_ancestor = lambda a, b: a == SHA
        s.main = "mainsha"
        s._snap_at = None
        s._snapshot_step()
        d = json.loads(self.con.execute("SELECT detail FROM events WHERE kind='state_snapshot'").fetchone()[0])
        by = {b["branch"]: b for b in d["branches"]}
        self.assertEqual((by["ghost"]["state"], by["ghost"]["commit"], by["ghost"]["retest"]), ("merged", SHA, 2))
        self.assertNotIn("other", by)                               # 不在 main 的不列
        self.assertTrue(all("reject_reason" in b for b in d["branches"]))
        if self.state("a") == "rejected":
            self.assertEqual(by["a"]["reject_reason"], "撞檔")


class DocsOnlyTest(Base):
    def test_doc_path_rules(self):
        self.assertTrue(is_doc_path("README.md") and is_doc_path("a/b/c.md") and is_doc_path("docs/x.png"))
        self.assertFalse(is_doc_path("game/x.gd") or is_doc_path("tools/docs.py") or is_doc_path("data/a.json"))
        self.assertTrue(docs_only_files(["a.md", "docs/b.txt"]))
        self.assertFalse(docs_only_files(["a.md", "x.gd"]))
        self.assertFalse(docs_only_files([]))                       # 讀不到檔案清單＝不知道，不當純文件

    def test_bisect_candidates_skip_md_only_commits(self):
        s = self.mk(workflow={"mode": "direct_merge"})
        files = {"c0": ["a.gd"], "c1": ["README.md"], "c2": ["docs/x.md", "docs/y.png"], "c3": ["b.gd", "c.md"], "c4": []}
        self.repo.commit_files = lambda c: files[c]
        self.assertEqual(s._drop_docs_commits(["c0", "c1", "c2", "c3", "c4"]), ["c0", "c3", "c4"])
        del self.repo.commit_files                                  # 沒這個能力的 repo：名單不動
        self.assertEqual(s._drop_docs_commits(["c1"]), ["c1"])

    def test_pure_md_docs_merge_not_noted_but_misclassified_is_(self):
        s = self.mk(workflow={"mode": "direct_merge"})
        self.repo.files_between = lambda a, b: ["README.md", "docs/z.md"]
        s._note_docs_merge("n1", "new1", "before")
        self.assertIsNone(store.get_meta(self.con, "docs_merges"))
        self.repo.files_between = lambda a, b: ["README.md", "game/x.gd"]
        s._note_docs_merge("n2", "new2", "before")
        self.assertEqual([m["name"] for m in json.loads(store.get_meta(self.con, "docs_merges"))], ["n2"])


class RelativeTest(Base):
    def _run(self, s, rid, kind, summary, status="fail", items=("godot_loadtime_x",)):
        rp = os.path.join(self.t.name, f"rep{rid}.json")
        with open(rp, "w", encoding="utf-8") as f:
            json.dump({"items": [{"id": i, "status": status, "summary": summary} for i in items]}, f)
        self.con.execute("INSERT INTO runs(id,kind,status,layer,started_at,report_path,commit_sha,base_sha) VALUES(?,?,?,?,?,?,?,?)",
                         (rid, kind, "fail", "gate", 1.0, rp, "c", "b"))
        return s._apply_relative(rid, kind, JobResult(status=status, exit_code=1, failed_items=list(items) if status == "fail" else []))

    def test_off_by_default(self):
        s = self.mk()
        res = self._run(s, 1, "baseline", "ms=4885/4000")
        self.assertEqual((res.status, res.failed_items), ("fail", ["godot_loadtime_x"]))
        self.assertIsNone(store.get_meta(self.con, "rel_hist:godot_loadtime_x"))

    def test_relative_to_main_baseline(self):
        s = self.mk(policy={"relative_ratio": 1.5, "relative_min_extra": 500})
        r = self._run(s, 1, "baseline", "ms=4885/4000")                      # 沒有基準：照舊紅（絕對門檻），並記下來
        self.assertEqual((r.status, r.failed_items), ("fail", ["godot_loadtime_x"]))
        r = self._run(s, 2, "selftest", "ms=5200/4000")                      # 5200 ≤ 1.5×4885 且只多 315 < 500：不算紅
        self.assertEqual((r.status, r.failed_items), ("pass", []))
        r = self._run(s, 3, "selftest", "ms=8000/4000")                      # 超過 1.5 倍又多 >500：紅
        self.assertEqual((r.status, r.failed_items), ("fail", ["godot_loadtime_x"]))
        r = self._run(s, 4, "selftest", "ms=6000/4000")                      # 6000 ≤ 1.5×4885=7327：不紅
        self.assertEqual(r.status, "pass")
        self.assertEqual(json.loads(store.get_meta(self.con, "rel_hist:godot_loadtime_x")), [4885.0])   # 只有 baseline 才累積基準

    def test_min_extra_floor_and_non_timing_items_untouched(self):
        s = self.mk(policy={"relative_ratio": 1.5, "relative_min_extra": 3000})
        self._run(s, 1, "baseline", "ms=3000/2000")
        r = self._run(s, 2, "selftest", "ms=5000/2000")                      # 超過 1.5 倍（4500）但只多 2000 < 3000：不紅
        self.assertEqual(r.status, "pass")
        r = self._run(s, 3, "selftest", "boom", items=("u_core",))           # 非計時項目、摘要也沒數字：照舊
        self.assertEqual((r.status, r.failed_items), ("fail", ["u_core"]))
        r = self._run(s, 4, "selftest", "ms=9000/2000", items=("godot_loadtime_x", "u_core"))
        self.assertEqual(r.failed_items, ["godot_loadtime_x", "u_core"])     # 混著非計時紅項：計時的紅（9000>4500 且多 6000）、非計時的保留

    def test_median_resists_one_outlier(self):
        s = self.mk(policy={"relative_ratio": 1.5, "relative_keep": 3})
        for i, v in enumerate((4000, 9000, 4200), start=1):                  # 一輪被負載拖慢到 9000
            self._run(s, i, "baseline", f"ms={v}/4000", status="pass", items=("godot_loadtime_x",))
        self.assertEqual(sorted(json.loads(store.get_meta(self.con, "rel_hist:godot_loadtime_x"))), [4000.0, 4200.0, 9000.0])
        r = self._run(s, 10, "selftest", "ms=6500/4000")                     # 中位數 4200，×1.5＝6300 → 6500 紅
        self.assertEqual(r.status, "fail")
        r = self._run(s, 11, "selftest", "ms=6200/4000")
        self.assertEqual(r.status, "pass")

    def test_config_validation(self):
        from jet import config
        base = {"git": {"remote": "x"}, "paths": {"state_dir": "/tmp/x"}, "commands": {"test": ["t"]},
                "cores": {"pool": "0-31", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 4}, "limits": {"batch_max": 16}}
        for bad in ({"relative_ratio": 0.5}, {"relative_ratio": -1}, {"relative_keep": 0}, {"relative_ratio": 1.5, "relative_value_re": "["},
                    {"relative_ratio": 1.5, "relative_value_re": "ms=\\d+"}):
            with self.assertRaises(config.ConfigError):
                config.parse({**base, "policy": bad})
        config.parse({**base, "policy": {"relative_ratio": 1.5, "relative_min_extra": 500}})


if __name__ == "__main__":
    unittest.main()
