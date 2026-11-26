"""NIGHT-01：每日完整層。用假時鐘與假執行器。"""
import json
import os
import time
import unittest

from jet import config, store
from jet.executor import JobResult
from tests.test_scheduler import Base


def at(day, hh, mm):
    return time.mktime((2026, 11, day, hh, mm, 0, 0, 0, -1))


class NightlyTest(Base):
    def mk2(self, nightly=None, items=None, status="pass", **over):
        wf = {"mode": "direct_merge", "loop": {"enabled": False}}
        if nightly is not None:
            wf["nightly"] = nightly
        s = self.mk(workflow=wf, **over)
        self.now = [at(25, 2, 0)]
        s.clock = lambda: self.now[0]
        self.items = items if items is not None else [{"id": "a", "status": "pass"}, {"id": "b", "status": "pass"}]
        self.full_status = status
        ex = self.ex
        start, poll = ex.start, ex.poll

        def start2(job):
            h = start(job)
            if job.layer == "full" and self.items is not None:
                with open(job.report_path, "w", encoding="utf-8") as f:
                    json.dump({"items": self.items}, f)
            return h

        def poll2(h):
            if h.job.layer == "full":
                for c in h.job.cores:
                    ex.live_cores.remove(c)
                return JobResult(self.full_status, 0 if self.full_status == "pass" else 1)
            return poll(h)
        ex.start, ex.poll = start2, poll2
        return s

    def fulls(self):
        return [j for j in self.ex.started if j.layer == "full"]

    def results(self):
        return [json.loads(r[0]) for r in self.con.execute("SELECT detail FROM events WHERE kind='full_result' ORDER BY id")]

    def test_disabled_by_default(self):
        s = self.mk2()
        self.now[0] = at(25, 23, 0)
        for _ in range(5):
            s.tick()
        self.assertEqual(self.fulls(), [])

    def test_waits_for_time_then_runs_once_per_day(self):
        s = self.mk2({"at": "03:30", "cores": 2})
        for _ in range(3):
            s.tick()
        self.assertEqual(self.fulls(), [])                     # 還沒到點
        self.now[0] = at(25, 3, 31)
        for _ in range(6):
            s.tick()
        self.assertEqual(len(self.fulls()), 1)
        self.assertEqual(len(self.fulls()[0].cores), 2)
        self.now[0] = at(25, 22, 0)
        for _ in range(4):
            s.tick()
        self.assertEqual(len(self.fulls()), 1)                 # 同一天不再跑
        self.now[0] = at(26, 3, 31)
        for _ in range(6):
            s.tick()
        self.assertEqual(len(self.fulls()), 2)                 # 隔天再跑

    def test_green_result(self):
        s = self.mk2({"at": "03:30"})
        self.now[0] = at(25, 3, 31)
        for _ in range(4):
            s.tick()
        r = self.results()
        self.assertEqual(len(r), 1)
        self.assertEqual((r[0]["total"], r[0]["red"], r[0]["timeouts"]), (2, [], []))
        self.assertEqual(r[0]["main_sha"], self.repo.main)

    def test_red_and_timeout_items_named_and_nothing_blocked(self):
        items = [{"id": "x", "status": "fail", "summary": "boom"}, {"id": "y", "status": "timeout"}, {"id": "z", "status": "pass"}]
        s = self.mk2({"at": "03:30"}, items=items, status="fail")
        self.repo.add("a")
        self.now[0] = at(25, 3, 31)
        for _ in range(4):
            s.tick()
        r = self.results()[0]
        self.assertEqual(r["total"], 3)
        self.assertEqual(r["red"], [{"item": "x", "summary": "boom"}])
        self.assertEqual([t["item"] for t in r["timeouts"]], ["y"])
        self.assertFalse(store.is_paused(self.con))

    def test_whole_run_timeout_gets_a_name(self):
        s = self.mk2({"at": "03:30"}, items=None, status="timeout")
        self.now[0] = at(25, 3, 31)
        for _ in range(4):
            s.tick()
        r = self.results()[0]
        self.assertEqual([t["item"] for t in r["timeouts"]], ["完整層整體逾時"])

    def test_infra_retries_then_reports(self):
        s = self.mk2({"at": "03:30"}, status="infra")
        self.now[0] = at(25, 3, 31)
        for _ in range(4):
            s.tick()
        self.assertEqual(len(self.fulls()), 1)
        self.assertEqual(self.results(), [])                   # 還在等重試
        for _ in range(2):
            self.now[0] += 601
            for _ in range(4):
                s.tick()
        self.assertEqual(len(self.fulls()), 3)
        r = self.results()
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["red"][0]["item"], "基礎設施錯誤")

    def test_payload_fields(self):
        from jet import notify
        s = self.mk2({"at": "03:30"}, items=[{"id": "x", "status": "fail", "summary": "boom"}], status="fail")
        self.now[0] = at(25, 3, 31)
        for _ in range(4):
            s.tick()
        row = self.con.execute("SELECT * FROM events WHERE kind='full_result'").fetchone()
        p = notify.build_payload([notify._enrich(self.con, row)], 1.0)["events"][0]
        self.assertEqual((p["kind"], p["total"], p["red"][0]["item"], p["timeouts"]), ("full_result", 1, "x", []))
        self.assertIn("main_sha", p)
        self.assertIn("started_at", p)

    def test_missing_required_file_is_skipped_not_red(self):
        s = self.mk2({"at": "03:30", "cores": 2, "require": ["/nonexistent/engine"]})
        self.now[0] = at(25, 3, 31)
        for _ in range(4):
            s.tick()
        self.assertEqual(self.fulls(), [])
        r = self.results()
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["red"], [])
        self.assertIn("未測", r[0]["skipped"])

    def test_green_sha_range_and_log_keep(self):
        s = self.mk2({"at": "03:30", "cores": 2})
        self.now[0] = at(25, 3, 31)
        for _ in range(6):
            s.tick()
        r = self.results()
        self.assertIsNone(r[0]["since_green"])
        self.assertTrue(r[0]["ok"])
        self.assertTrue(store.get_meta(self.con, "nightly_green_sha"))
        import os
        self.assertTrue(os.path.isdir(os.path.join(self.cfg.state_dir, "nightly")))

    def test_starving_nightly_pauses_new_selftests(self):
        s = self.mk2({"at": "03:30", "cores": 2})
        self.now[0] = at(25, 3, 31)
        s._nightly_wait_since = self.now[0] - 3600
        self.assertTrue(s._nightly_starving())
        s._nightly_wait_since = self.now[0] - 60
        self.assertFalse(s._nightly_starving())
        s._nightly_wait_since = None
        self.assertFalse(s._nightly_starving())

    def test_config_validation(self):
        with self.assertRaises(config.ConfigError):
            self.mk(workflow={"nightly": {"at": "25:00"}})
        with self.assertRaises(config.ConfigError):
            self.mk(workflow={"nightly": {"at": "3:30"}})
        self.mk(workflow={"nightly": {"at": "03:30"}})


class SnapshotDedupeTest(Base):
    def test_only_latest_snapshot_kept(self):
        s = self.mk()
        now = [1000.0]
        s.clock = lambda: now[0]
        self.repo.add("a")
        for _ in range(5):
            s.tick()
            now[0] += 301
        n = self.con.execute("SELECT COUNT(*) FROM events WHERE kind='state_snapshot'").fetchone()[0]
        self.assertEqual(n, 1)


if __name__ == "__main__":
    unittest.main()


class NightlyCancelTest(NightlyTest):
    def test_cancel_requeues_then_reports_after_limit(self):
        s = self.mk2({"at": "03:30"}, status="cancelled")
        self.now[0] = at(25, 3, 31)
        for _ in range(4):
            s.tick()
        self.assertEqual(len(self.fulls()), 1)
        self.assertEqual(self.results(), [])                   # 被取消：重排，不是沒結果就算了
        self.assertEqual(store.get_meta(self.con, "nightly_day"), "")
        for _ in range(2):
            self.now[0] += 601
            for _ in range(4):
                s.tick()
        self.assertEqual(len(self.fulls()), 3)
        r = self.results()
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["red"][0]["item"], "完整層被取消")

    def test_recover_requeues_lost_nightly(self):
        s = self.mk2({"at": "03:30"})
        store.set_meta(self.con, "nightly_day", "2026-11-25")
        self.con.execute("INSERT INTO runs(id,kind,status,started_at,workdir,cores,timeout_s,units,layer,commit_sha) "
                         "VALUES(500,'nightly','running',1,'x','1',1,1,'full','m0')")
        s.recover()
        self.assertEqual(store.get_meta(self.con, "nightly_day"), "")
        self.now[0] = at(25, 3, 40)
        for _ in range(4):
            s.tick()
        self.assertTrue(self.fulls())


class NightlyRecoverLimitTest(NightlyTest):
    def test_recover_limit_reports_and_resets(self):
        s = self.mk2({"at": "03:30"})
        store.set_meta(self.con, "nightly_day", "2026-11-25")
        store.set_meta(self.con, "nightly_infra", "2")             # 已經用掉兩次
        self.con.execute("INSERT INTO runs(id,kind,status,started_at,workdir,cores,timeout_s,units,layer,commit_sha) "
                         "VALUES(501,'nightly','running',1,'x','1',1,1,'full','m0')")
        s.recover()
        r = self.results()
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["red"][0]["item"], "完整層被取消")
        self.assertEqual(r[0]["main_sha"], "m0")
        self.assertEqual(store.get_meta(self.con, "nightly_infra"), "0")
        self.assertEqual(store.get_meta(self.con, "nightly_day"), "2026-11-25")   # 今天不再排
        self.con.execute("INSERT INTO runs(id,kind,status,started_at,workdir,cores,timeout_s,units,layer,commit_sha) "
                         "VALUES(502,'nightly','running',1,'x','1',1,1,'full','m0')")
        s.recover()                                                 # 隔天第一次再遇到：重排，不沉默
        self.assertEqual(store.get_meta(self.con, "nightly_day"), "")
