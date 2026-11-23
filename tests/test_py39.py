"""守門：全部程式必須能用 Python 3.9 的語法解析（不准用 match 等 3.10 才有的語法）。
執行期用法（zip(strict=)、dataclass slots=）由在 3.9 上跑全套測試把關。"""
import ast, os, unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class Py39Syntax(unittest.TestCase):
    def test_all_files_parse_as_39(self):
        bad = []
        for base in ("jet", "tests", "examples"):
            for dp, _, fs in os.walk(os.path.join(ROOT, base)):
                for f in fs:
                    if f.endswith(".py"):
                        p = os.path.join(dp, f)
                        try:
                            ast.parse(open(p, encoding="utf-8").read(), p, feature_version=(3, 9))
                        except SyntaxError as e:
                            bad.append(f"{p}: {e}")
        self.assertEqual(bad, [])


if __name__ == "__main__":
    unittest.main()
