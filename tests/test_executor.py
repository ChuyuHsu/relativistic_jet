"""執行器測試：全部用合成的假測試指令（python -c），不需要任何專案，雲端容器可跑。"""
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from jet import cores
from jet.executor import (CoreBusy, Executor, ExecutorError, Job, ResourceLow, fill_argv,
                          read_report)

PY = sys.executable
def _free_cores():
    """本行程可用的核裡，沒被別人綁住的（開發機上前幾顆核常被人釘住工作，拿來測會誤報核被佔用）；不夠就退回全部。"""
    allowed = sorted(os.sched_getaffinity(0))
    try:
        busy = cores.foreign_busy(allowed, set())
    except Exception:
        busy = set()
    free = [c for c in allowed if c not in busy]
    return free if len(free) >= 4 else allowed


ALLOWED = _free_cores()


def alive(pid: int) -> bool:
    """行程還活著（殭屍算死：它已經沒在跑，只是沒人回收）。"""
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
            txt = f.read()
    except OSError:
        return False
    return txt[txt.rindex(")") + 2:].split()[0] not in ("Z", "X")


def rd(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def wait_for(cond, secs=10.0):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


class Base(unittest.TestCase):
    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.tmp = self._t.name
        self.ex = Executor(grace_s=1.0)
        self.addCleanup(self._t.cleanup)
        self.addCleanup(self._kill_leftovers)
        self.handles = []

    def _kill_leftovers(self):
        for h in self.handles:
            if h.result is None:
                self.ex.cancel(h)

    def job(self, code, *, timeout=20, cores_=None, only=None, argv=None):
        n = len(self.handles)
        j = Job(layer="gate", workdir=self.tmp, cores=cores_ or [ALLOWED[0]], timeout_s=timeout,
                report_path=os.path.join(self.tmp, f"r{n}.json"), log_path=os.path.join(self.tmp, f"l{n}.log"),
                only=only, argv=argv or [PY, "-c", code])
        return j

    def ok_job(self, code, **kw):
        """跑完會寫一份全綠報告的工作（沒報告的 rc=0 會被判 infra）。"""
        j = self.job("", **kw)
        j.argv = [PY, "-c", code + ";import json;json.dump({'items':[{'id':'a','status':'pass'}]},open(%r,'w'))" % j.report_path]
        return j

    def run_job(self, j, secs=20.0):
        h = self.ex.start(j)
        self.handles.append(h)
        end = time.monotonic() + secs
        while time.monotonic() < end:
            r = self.ex.poll(h)
            if r is not None:
                return h, r
            time.sleep(0.02)
        self.fail("工作沒在時限內結束")


REPORT = ("import json,sys;json.dump({{'schema':1,'duration_s':0.1,'total':3,'items':{items}}},"
          "open(sys.argv[1],'w'));sys.exit({rc})")


class ResultTest(Base):
    def test_pass(self):
        j = self.job("")
        j.argv = [PY, "-c", "import json;print('hello');json.dump({'items':[{'id':'a','status':'pass'}]},open(%r,'w'))" % j.report_path]
        h, r = self.run_job(j)
        self.assertEqual((r.status, r.exit_code, r.failed_items), ("pass", 0, []))
        self.assertGreater(r.duration_s, 0)
        self.assertIn("hello", rd(j.log_path))
        self.assertIs(self.ex.poll(h), r)          # 之後每次回同一份
        self.assertEqual(self.ex.live_pgids(), set())

    def test_fail_reads_report_items(self):
        items = [{"id": "a", "status": "pass"}, {"id": "b", "status": "fail"}, {"id": "c", "status": "timeout"},
                 {"id": "d", "status": "skip"}]
        code = "import json;json.dump({'schema':1,'items':%r},open(%r,'w'));raise SystemExit(1)"
        j = self.job("")
        j.argv = [PY, "-c", code % (items, j.report_path)]
        _, r = self.run_job(j)
        self.assertEqual((r.status, r.exit_code, r.failed_items), ("fail", 1, ["b", "c"]))

    def test_exit_zero_with_red_item_is_still_fail(self):
        items = [{"id": "x", "status": "fail"}]
        j = self.job("")
        j.argv = [PY, "-c", "import json;json.dump({'items':%r},open(%r,'w'))" % (items, j.report_path)]
        _, r = self.run_job(j)
        self.assertEqual((r.status, r.failed_items), ("fail", ["x"]))

    def test_no_report_is_infra_never_green_never_empty_red(self):
        for rc in (0, 1):
            _, r = self.run_job(self.job("raise SystemExit(%d)" % rc))
            self.assertEqual((r.status, r.failed_items), ("infra", []), rc)

    def test_corrupt_report(self):
        j = self.job("")
        j.argv = [PY, "-c", "open(%r,'w').write('{oops');raise SystemExit(1)" % j.report_path]
        _, r = self.run_job(j)
        self.assertEqual((r.status, r.failed_items), ("infra", []))

    def test_report_without_items_or_empty_items_is_infra(self):
        for body in ({"schema": 1}, {"items": []}, {"items": "x"}, [1, 2]):
            for rc in (0, 1):
                j = self.job("")
                j.argv = [PY, "-c", "import json;json.dump(%r,open(%r,'w'));raise SystemExit(%d)" % (body, j.report_path, rc)]
                _, r = self.run_job(j)
                self.assertEqual((r.status, r.failed_items), ("infra", []), (body, rc))

    def test_exit_one_but_no_red_item_is_infra(self):
        j = self.job("")
        j.argv = [PY, "-c", "import json;json.dump({'items':[{'id':'a','status':'pass'}]},open(%r,'w'));raise SystemExit(1)" % j.report_path]
        _, r = self.run_job(j)
        self.assertEqual((r.status, r.failed_items), ("infra", []))

    def test_exit_zero_all_pass_report(self):
        j = self.job("")
        j.argv = [PY, "-c", "import json;json.dump({'items':[{'id':'a','status':'pass'},{'id':'b','status':'skip'}]},open(%r,'w'))" % j.report_path]
        _, r = self.run_job(j)
        self.assertEqual((r.status, r.failed_items), ("pass", []))

    def test_exit_two_is_infra(self):
        _, r = self.run_job(self.job("raise SystemExit(2)"))
        self.assertEqual((r.status, r.exit_code), ("infra", 2))

    def test_other_exit_codes_and_signals_are_infra(self):
        _, r = self.run_job(self.job("raise SystemExit(7)"))
        self.assertEqual(r.status, "infra")
        _, r = self.run_job(self.job("import os,signal;os.kill(os.getpid(),signal.SIGKILL)"))
        self.assertEqual((r.status, r.exit_code), ("infra", -9))

    def test_missing_command_is_infra(self):
        j = self.job("", argv=["/nonexistent/command-xyz"])
        with self.assertRaises(ExecutorError):
            self.ex.start(j)
        self.assertEqual(self.ex.live_pgids(), set())

    def test_stale_report_is_removed(self):
        j = self.job("raise SystemExit(1)")
        with open(j.report_path, "w") as f:
            json.dump({"items": [{"id": "old", "status": "fail"}]}, f)
        _, r = self.run_job(j)
        self.assertEqual((r.status, r.failed_items), ("infra", []))   # 舊報告被清掉，不會被誤讀


class PlaceholderTest(Base):
    def test_fill_argv(self):
        j = Job(layer="gate", workdir="/w", cores=[3, 5], timeout_s=1, report_path="/r.json", log_path="/l", only="t1")
        self.assertEqual(fill_argv(["x", "{layer}", "{cores}", "{ncores}", "{workdir}", "{report}", "{only}", "{a}"], j),
                         ["x", "gate", "3,5", "2", "/w", "/r.json", "t1", "{a}"])
        j.only = None
        self.assertEqual(fill_argv(["{only}"], j), [""])

    def test_template_from_constructor_and_cwd(self):
        ex = Executor(test_cmd=[PY, "-c", "import os,sys;print(os.getcwd(),sys.argv[1:])", "{layer}", "{ncores}", "{only}"])
        j = self.job("", only="foo")
        j.argv = None
        h = ex.start(j)
        end = time.monotonic() + 20
        while ex.poll(h) is None and time.monotonic() < end:
            time.sleep(0.02)
        out = rd(j.log_path)
        self.assertIn(os.path.realpath(self.tmp), os.path.realpath(out.split(" ", 1)[0]) or out)
        self.assertIn("['gate', '1', 'foo']", out)

    def test_no_command(self):
        j = self.job("")
        j.argv = None
        with self.assertRaises(ExecutorError):
            self.ex.start(j)


class AffinityTest(Base):
    def test_child_is_pinned_to_job_cores(self):
        j = self.ok_job("import os;print(sorted(os.sched_getaffinity(0)))", cores_=[ALLOWED[-1]])
        _, r = self.run_job(j)
        self.assertEqual(r.status, "pass")
        self.assertEqual(rd(j.log_path).strip(), str([ALLOWED[-1]]))

    def test_own_process_is_not_pinned(self):
        before = os.sched_getaffinity(0)
        self.run_job(self.ok_job("pass"))
        self.assertEqual(os.sched_getaffinity(0), before)

    def test_bad_core_raises(self):
        with self.assertRaises(ExecutorError):
            self.ex.start(self.job("pass", cores_=[100000]))

    def test_empty_or_duplicate_cores(self):
        for bad in ([], [ALLOWED[0], ALLOWED[0]]):
            j = self.job("pass")
            j.cores = bad
            with self.assertRaises(ExecutorError):
                self.ex.start(j)


class KillTest(Base):
    GRANDCHILD = ("import subprocess,sys,time;"
                  "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);"
                  "open(%r,'w').write(str(p.pid));time.sleep(%s)")

    def _pidfile(self):
        return os.path.join(self.tmp, "gc.pid")

    def test_timeout_kills_whole_group(self):
        j = self.job(self.GRANDCHILD % (self._pidfile(), 60), timeout=1)
        h, r = self.run_job(j, secs=30)
        self.assertEqual(r.status, "timeout")
        gc = int(rd(self._pidfile()))
        self.assertTrue(wait_for(lambda: not alive(gc)), "孫行程沒被殺")
        self.assertFalse(alive(h.pgid))

    def test_cancel_kills_whole_group(self):
        j = self.job(self.GRANDCHILD % (self._pidfile(), 60))
        h = self.ex.start(j)
        self.handles.append(h)
        self.assertTrue(wait_for(lambda: os.path.exists(self._pidfile()) and rd(self._pidfile())))
        self.assertIsNone(self.ex.poll(h))
        r = self.ex.cancel(h)
        self.assertEqual(r.status, "cancelled")
        gc = int(rd(self._pidfile()))
        self.assertTrue(wait_for(lambda: not alive(gc)))
        self.assertIs(self.ex.cancel(h), r)        # 再取消回原結果
        self.assertIs(self.ex.poll(h), r)
        self.assertEqual(self.ex.live_cores(), set())

    def test_sigterm_ignoring_child_gets_sigkill(self):
        code = ("import signal,time,os;signal.signal(signal.SIGTERM,signal.SIG_IGN);"
                "open(%r,'w').write('up');time.sleep(60)" % os.path.join(self.tmp, "up"))
        j = self.job(code, timeout=1)
        t0 = time.monotonic()
        _, r = self.run_job(j, secs=30)
        self.assertEqual(r.status, "timeout")
        self.assertLess(time.monotonic() - t0, 15)

    def test_leftover_after_normal_exit_is_swept(self):
        # 領頭的行程正常結束，但背景還留著一個孫行程：不准留孤兒
        j = self.ok_job(self.GRANDCHILD % (self._pidfile(), 0.3))
        _, r = self.run_job(j)
        self.assertEqual(r.status, "pass")
        gc = int(rd(self._pidfile()))
        self.assertTrue(wait_for(lambda: not alive(gc)), "背景孫行程沒被掃掉")

    def test_child_that_leaves_process_group_is_still_killed(self):
        code = ("import os,subprocess,sys,time;"
                "p=subprocess.Popen([sys.executable,'-c','import os,time;os.setpgid(0,0);time.sleep(60)']);"
                "open(%r,'w').write(str(p.pid));time.sleep(60)" % self._pidfile())
        h = self.ex.start(self.job(code))
        self.handles.append(h)
        self.assertTrue(wait_for(lambda: os.path.exists(self._pidfile()) and rd(self._pidfile())))
        time.sleep(0.3)   # 讓孫行程完成 setpgid
        gc = int(rd(self._pidfile()))
        self.assertNotEqual(os.getpgid(gc), h.pgid)
        self.ex.cancel(h)
        self.assertTrue(wait_for(lambda: not alive(gc)))


class CoreExclusivityTest(Base):
    def test_same_core_twice_is_refused(self):
        h = self.ex.start(self.job("import time;time.sleep(30)"))
        self.handles.append(h)
        with self.assertRaises(CoreBusy) as cm:
            self.ex.start(self.job("pass"))
        self.assertEqual(cm.exception.cores, {ALLOWED[0]})
        self.assertEqual(self.ex.live_cores(), {ALLOWED[0]})
        self.ex.cancel(h)
        h2 = self.ex.start(self.job("pass"))          # 放掉後就能再用
        self.handles.append(h2)

    @unittest.skipIf(len(ALLOWED) < 2, "只有一顆核，無法做『綁核的外人』")
    def test_two_jobs_on_different_cores(self):
        a = self.ex.start(self.job("import time;time.sleep(30)", cores_=[ALLOWED[0]]))
        b = self.ex.start(self.job("import time;time.sleep(30)", cores_=[ALLOWED[1]]))
        self.handles += [a, b]
        self.assertEqual(self.ex.live_pgids(), {a.pgid, b.pgid})

    @unittest.skipIf(len(ALLOWED) < 2, "只有一顆核，無法做『綁核的外人』")
    def test_foreign_pinned_process_blocks_start(self):
        core = ALLOWED[-1]
        outsider = subprocess.Popen([PY, "-c", "import time;time.sleep(60)"], start_new_session=True,
                                    preexec_fn=lambda: os.sched_setaffinity(0, {core}))
        self.addCleanup(lambda: (outsider.kill(), outsider.wait()))
        self.assertTrue(wait_for(lambda: core in cores.foreign_busy([core], set(), baseline=set(ALLOWED))))
        with self.assertRaises(CoreBusy) as cm:
            Executor(baseline_cpus=set(ALLOWED)).start(self.job("pass", cores_=[core]))
        self.assertEqual(cm.exception.cores, {core})
        self.assertEqual(self.ex.live_pgids(), set())


@unittest.skipIf(len(ALLOWED) < 2, "只有一顆核，無法做『綁核的外人』")
class ForeignBusyTest(unittest.TestCase):
    def setUp(self):
        self.core = ALLOWED[-1]
        self.base = set(ALLOWED)
        self.p = subprocess.Popen([PY, "-c", "import time;time.sleep(60)"], start_new_session=True,
                                  preexec_fn=lambda: os.sched_setaffinity(0, {self.core}))
        self.addCleanup(lambda: (self.p.kill(), self.p.wait()))
        self.assertTrue(wait_for(lambda: os.sched_getaffinity(self.p.pid) == {self.core}))

    def only(self, *extra):
        """只看這個測試自己開的行程，不受開發機上別的行程影響。"""
        pids = {self.p.pid} | {e.pid for e in extra}
        return [x for x in cores.scan_procs() if x.pid in pids]

    def test_detects_pinned_outsider(self):
        self.assertEqual(cores.foreign_busy([self.core], set(), baseline=self.base, procs=self.only()), {self.core})

    def test_other_cores_not_reported(self):
        self.assertEqual(cores.foreign_busy([ALLOWED[0]], set(), baseline=self.base, procs=self.only()), set())

    def test_own_group_is_ignored(self):
        self.assertEqual(cores.foreign_busy([self.core], {self.p.pid}, baseline=self.base, procs=self.only()), set())

    def test_unpinned_process_is_not_foreign(self):
        # 沒綁核的一般行程能跑在任何核上，不算佔用
        q = subprocess.Popen([PY, "-c", "import time;time.sleep(60)"], start_new_session=True)
        self.addCleanup(lambda: (q.kill(), q.wait()))
        self.p.kill()
        self.p.wait()
        time.sleep(0.1)
        self.assertEqual(cores.foreign_busy(ALLOWED, set(), baseline=self.base, procs=self.only(q)), set())

    def test_empty_cores(self):
        self.assertEqual(cores.foreign_busy([], set()), set())

    def test_parse_stat_with_odd_names(self):
        line = "123 (we ird) name)) S 1 77 88 0 -1 4194560 " + " ".join(["0"] * 29) + " 5 0"
        info = cores._parse_stat(123, line)
        self.assertEqual((info.pid, info.state, info.ppid, info.pgrp, info.sid, info.cpu), (123, "S", 1, 77, 88, 5))
        self.assertIsNone(cores._parse_stat(1, "garbage"))


class ResourceTest(Base):
    def test_low_memory_pauses(self):
        ex = Executor(min_free_mem_mb=10 ** 9)
        with self.assertRaises(ResourceLow) as cm:
            ex.start(self.job("pass"))
        self.assertIn("記憶體", cm.exception.reason)
        self.assertEqual(ex.live_pgids(), set())

    def test_low_disk_pauses(self):
        ex = Executor(min_free_disk_mb=10 ** 12)
        with self.assertRaises(ResourceLow) as cm:
            ex.start(self.job("pass"))
        self.assertIn("磁碟", cm.exception.reason)

    def test_watermark_zero_never_blocks(self):
        self.assertIsNone(Executor().check_resources(self.tmp))

    def test_ok_above_watermark(self):
        self.assertIsNone(Executor(min_free_mem_mb=1, min_free_disk_mb=1).check_resources(self.tmp))


LAUNCH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "jet", "_launch.py")


def launch(*args):
    return subprocess.run([PY, "-I", LAUNCH, *args], capture_output=True, text=True, timeout=30)


class LauncherTest(unittest.TestCase):
    def test_pins_then_execs_same_pid(self):
        r = launch(str(ALLOWED[0]), "--", PY, "-c", "import os;print(os.getpid(), sorted(os.sched_getaffinity(0)))")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn(str([ALLOWED[0]]), r.stdout)

    def test_bad_core_list(self):
        for spec in ("", "x", "100000"):
            r = launch(spec, "--", PY, "-c", "pass")
            self.assertEqual(r.returncode, 125, (spec, r.stderr))
        self.assertIn("綁核失敗", launch("100000", "--", PY).stderr)

    def test_bad_usage(self):
        self.assertEqual(launch(str(ALLOWED[0]), PY).returncode, 125)
        self.assertEqual(launch(str(ALLOWED[0]), "--").returncode, 125)

    def test_missing_and_non_executable_command(self):
        self.assertEqual(launch(str(ALLOWED[0]), "--", "/nonexistent/cmd-xyz").returncode, 127)
        with tempfile.TemporaryDirectory() as t:
            p = os.path.join(t, "plain.txt")
            open(p, "w").close()
            self.assertEqual(launch(str(ALLOWED[0]), "--", p).returncode, 126)

    def test_command_args_with_dashes_pass_through(self):
        r = launch(str(ALLOWED[0]), "--", PY, "-c", "import sys;print(sys.argv[1:])", "--", "-x")
        self.assertEqual(r.stdout.strip(), "['--', '-x']")


class ThreadSafetyTest(Base):
    """排隊器主行程有別的執行緒：fork 與 exec 之間不能跑 Python（不用 preexec_fn），連續起很多工作不卡。"""

    def test_no_preexec_fn(self):
        with mock.patch("jet.executor.subprocess.Popen", wraps=subprocess.Popen) as popen:
            self.run_job(self.ok_job("pass"))
        kw = popen.call_args.kwargs
        self.assertIsNone(kw.get("preexec_fn"))
        self.assertTrue(kw["start_new_session"])
        argv = popen.call_args.args[0]
        self.assertEqual(argv[1:3], ["-I", LAUNCH])

    def test_many_jobs_with_busy_threads(self):
        stop = threading.Event()
        log = logging.getLogger("jet_test_busy")
        log.addHandler(logging.NullHandler())
        log.propagate = False

        def churn():   # 一直拿 logging 的鎖、一直配置記憶體：fork 時最容易剛好握著鎖
            junk = []
            while not stop.is_set():
                log.warning("x" * 200)
                junk.append(bytearray(10000))
                if len(junk) > 200:
                    junk.clear()
                time.sleep(0.0005)   # 讓出 GIL：不讓測試本身被餓死，鎖與記憶體照樣高頻被握著

        threads = [threading.Thread(target=churn, daemon=True) for _ in range(3)]
        for t in threads:
            t.start()
        self.addCleanup(lambda: (stop.set(), [t.join(5) for t in threads]))
        t0 = time.monotonic()
        for _ in range(30):
            _, r = self.run_job(self.ok_job("pass"), secs=30)
            self.assertEqual(r.status, "pass")
        self.assertLess(time.monotonic() - t0, 120)

    def test_jobs_started_from_other_threads_do_not_hang(self):
        results, errors = [], []

        def worker(i):
            try:
                ex = Executor()
                core = ALLOWED[i % len(ALLOWED)]
                for k in range(5):
                    j = Job(layer="gate", workdir=self.tmp, cores=[core], timeout_s=120,
                            report_path=os.path.join(self.tmp, f"t{i}_{k}.json"), log_path=os.path.join(self.tmp, f"t{i}_{k}.log"),
                            argv=[PY, "-c", "import json,sys;json.dump({'items':[{'id':'a','status':'pass'}]},open(sys.argv[1],'w'))",
                                  "{report}"])
                    h = ex.start(j)
                    end = time.monotonic() + 120
                    r = None
                    while r is None and time.monotonic() < end:
                        r = ex.poll(h)
                        time.sleep(0.01)
                    results.append(r.status if r else "hang")
            except Exception as e:   # noqa: BLE001
                errors.append(repr(e))

        ts = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(300)
        self.assertEqual(errors, [])
        self.assertEqual(results, ["pass"] * 15)


class ReportTest(unittest.TestCase):
    def test_read_report_missing(self):
        self.assertIsNone(read_report("/nonexistent/x.json"))


if __name__ == "__main__":
    unittest.main()


class OverloadDetectTest(unittest.TestCase):
    def test_cpu_seconds_of_group(self):
        p = subprocess.Popen([PY, "-c", "import time\nt=time.time()\nwhile time.time()-t<0.6: pass"],
                             start_new_session=True)
        time.sleep(0.8)
        # 已結束但還沒回收（殭屍）不算活行程，所以這裡只驗「活著的忙行程量得到 CPU 時間」
        q = subprocess.Popen([PY, "-c", "while True: pass"], start_new_session=True)
        self.addCleanup(lambda: (q.kill(), q.wait(), p.wait()))
        time.sleep(0.7)
        self.assertGreater(cores.group_cpu_seconds(q.pid), 0.3)

    def test_pressure_parse(self):
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".psi", delete=False) as f:
            f.write("some avg10=42.50 avg60=10.00 avg300=1.00 total=123\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
        self.addCleanup(os.unlink, f.name)
        self.assertEqual(cores.cpu_pressure(f.name), 42.5)
        self.assertIsNone(cores.cpu_pressure("/nonexistent/psi"))

    def test_was_overloaded_needs_both_conditions(self):
        ex = Executor(overload_cpu_ratio=0.6)
        h = type("H", (), {})()
        h.started = time.monotonic() - 100
        h.job = type("J", (), {"cores": [0, 1, 2, 3]})()
        old = cores.system_overloaded
        try:
            cores.system_overloaded = lambda *a, **k: True
            self.assertTrue(ex._was_overloaded(h, 50.0))       # 4 核×100 秒只拿到 50 秒＝12%
            self.assertFalse(ex._was_overloaded(h, 390.0))     # 幾乎滿用：是測試自己慢
            cores.system_overloaded = lambda *a, **k: False
            self.assertFalse(ex._was_overloaded(h, 50.0))      # 機器不忙：單純卡住，算測試的錯
        finally:
            cores.system_overloaded = old


class EnvTest(Base):
    def test_job_sees_its_core_count(self):
        c = ALLOWED[:2] if len(ALLOWED) >= 2 else ALLOWED[:1]
        j = self.job("", cores_=c)
        j.argv = [PY, "-c", "import os,json;print(os.environ['JET_CORES'],os.environ['JET_NCORES']);"
                  "json.dump({'items':[{'id':'a','status':'pass'}]},open(%r,'w'))" % j.report_path]
        _, r = self.run_job(j)
        self.assertEqual(r.status, "pass")
        self.assertIn(",".join(map(str, sorted(c))) + " " + str(len(c)), rd(j.log_path))


class FailureExcerptTest(unittest.TestCase):
    def test_excerpt_from_log_and_report_summary(self):
        import json, os, tempfile
        from jet import executor as ex
        d = tempfile.mkdtemp()
        log = os.path.join(d, "all.log")
        with open(log, "w", encoding="utf-8") as f:
            f.write("開始\n" + "\n".join("noise %d" % i for i in range(50)) +
                    "\nFAIL unit_a: 期望 3 得到 4\n  at foo.py:12\n  at bar.py:7\nPASS unit_b\n")
        out = ex.failure_excerpt(log, "unit_a")
        self.assertTrue(out.startswith("FAIL unit_a"))
        self.assertIn("foo.py:12", out)
        tail = ex.failure_excerpt(log, "沒這項", lines=3)           # 記錄沒提到：附最後幾行
        self.assertIn("最後 3 行", tail)
        self.assertIn("PASS unit_b", tail)
        self.assertEqual(ex.failure_excerpt(os.path.join(d, "nope.log"), "x"), "")
        rep = os.path.join(d, "r.json")
        with open(rep, "w", encoding="utf-8") as f:
            json.dump({"items": [{"id": "u", "status": "fail", "summary": " 原因 "}, {"id": "v", "status": "fail"},
                                 {"id": "w", "status": "pass", "summary": "x"}]}, f)
        self.assertEqual(ex.read_report_summaries(rep), {"u": "原因"})


class CpuSecondsTest(Base):
    def test_cpu_seconds_recorded_on_normal_exit(self):
        j = self.ok_job("exec('import time\\nt=time.time()\\nwhile time.time()-t<0.6: pass')")
        _, r = self.run_job(j)
        self.assertEqual(r.status, "pass")
        self.assertIsNotNone(r.cpu_s)
        self.assertGreater(r.cpu_s, 0.2)
