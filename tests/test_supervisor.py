import os, subprocess, tempfile, unittest

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "deploy", "jet-supervisor.sh")


def release(root, name, code):
    d = os.path.join(root, "releases", name, "jet")
    os.makedirs(d)
    open(os.path.join(d, "__init__.py"), "w").close()
    with open(os.path.join(d, "__main__.py"), "w") as f:
        f.write(code)
    return os.path.join(root, "releases", name)


class SupervisorTest(unittest.TestCase):
    def run_sup(self, cur_code, prev_code):
        with tempfile.TemporaryDirectory() as t:
            v1, v2 = release(t, "v1", prev_code), release(t, "v2", cur_code)
            os.symlink(v2, os.path.join(t, "current"))
            if prev_code is not None:
                os.symlink(v1, os.path.join(t, "previous"))
            env = dict(os.environ, JET_HOME=t, CONFIG="x", RESTART_DELAY="0")
            p = subprocess.run(["bash", SCRIPT], env=env, capture_output=True, text=True, timeout=60)
            log = open(os.path.join(t, "supervisor.log")).read() if os.path.exists(
                os.path.join(t, "supervisor.log")) else ""
            return p, os.path.realpath(os.path.join(t, "current")), log

    def test_rollback_after_three_crashes(self):
        p, cur, log = self.run_sup("import sys; sys.exit(7)", "print('ok')")
        self.assertEqual(p.returncode, 0)
        self.assertTrue(cur.endswith("v1"))
        self.assertIn("自動退版", log)

    def test_clean_exit_no_rollback(self):
        p, cur, log = self.run_sup("print('ok')", "print('old')")
        self.assertTrue(cur.endswith("v2"))
        self.assertEqual(log, "")


if __name__ == "__main__":
    unittest.main()
