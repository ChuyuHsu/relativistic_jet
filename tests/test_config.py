import json, os, tempfile, unittest
from jet import config

GOOD = {"git": {"remote": "x"}, "cores": {"pool": "2-9"},
        "commands": {"test": ["t", "{layer}"]}}


class ConfigTest(unittest.TestCase):
    def test_parse_defaults(self):
        c = config.parse(GOOD)
        self.assertEqual(c.pool, list(range(2, 10)))
        self.assertEqual(c.batch_max, 16)
        self.assertTrue(c.require_approval)

    def test_missing_test_cmd(self):
        with self.assertRaises(config.ConfigError):
            config.parse({"git": {"remote": "x"}, "cores": {"pool": "2"}, "commands": {}})

    def test_bad_points(self):
        d = json.loads(json.dumps(GOOD)); d["limits"] = {"points": [8, 2]}
        with self.assertRaises(config.ConfigError):
            config.parse(d)

    def test_dup_cores(self):
        d = json.loads(json.dumps(GOOD)); d["cores"]["pool"] = "2-4,3"
        with self.assertRaises(config.ConfigError):
            config.parse(d)

    def test_safe_reload_keeps_old(self):
        old = config.parse(GOOD)
        with tempfile.TemporaryDirectory() as t:
            p = os.path.join(t, "c.json")
            with open(p, "w") as f:
                f.write("{壞的")
            self.assertIs(config.safe_reload(p, old), old)

    def test_example_config_valid(self):
        here = os.path.dirname(__file__)
        config.load(os.path.join(here, "..", "examples", "jet.example.json"))


if __name__ == "__main__":
    unittest.main()
