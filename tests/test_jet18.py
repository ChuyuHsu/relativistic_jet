"""設定檔 env：驗證格式、解析成 (名, 值) 清單。"""
import unittest

from jet import config

BASE = {"git": {"remote": "x"}, "paths": {"state_dir": "/tmp/x"}, "commands": {"test": ["t"]},
        "cores": {"pool": "0-31", "selftest_cores": 1, "batch_cores": 1, "reserve_for_selftest": 4}, "limits": {"batch_max": 16}}


class JobEnvTest(unittest.TestCase):
    def test_default_empty(self):
        self.assertEqual(config.parse(BASE).job_env, ())

    def test_parsed_sorted(self):
        c = config.parse({**BASE, "env": {"TMPDIR": "/data/tmp", "A1_TMP": "/data/tmp"}})
        self.assertEqual(c.job_env, (("A1_TMP", "/data/tmp"), ("TMPDIR", "/data/tmp")))

    def test_bad_env_rejected(self):
        for bad in ([1], {"A": 1}, {"": "x"}, "TMPDIR=/x"):
            with self.assertRaises(config.ConfigError):
                config.parse({**BASE, "env": bad})


if __name__ == "__main__":
    unittest.main()
