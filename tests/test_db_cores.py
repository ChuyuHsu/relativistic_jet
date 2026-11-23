import os, tempfile, unittest
from jet import db
from jet.cores import CorePool, parse_cores


class DbTest(unittest.TestCase):
    def test_schema_and_backup(self):
        with tempfile.TemporaryDirectory() as t:
            con = db.connect(os.path.join(t, "a.sqlite"))
            con.execute("INSERT INTO branches(name,head_sha,content_id,state,updated_at)"
                        " VALUES('b','s','c','pending',0)")
            con.execute("INSERT INTO branches(name,head_sha,content_id,parent,state,updated_at)"
                        " VALUES('child','s2','c2','b','pending',0)")
            bk = os.path.join(t, "bk.sqlite")
            db.backup(con, bk)
            con2 = db.connect(bk)
            self.assertEqual(con2.execute("SELECT count(*) FROM branches").fetchone()[0], 2)

    def test_reopen_is_idempotent(self):
        with tempfile.TemporaryDirectory() as t:
            p = os.path.join(t, "a.sqlite")
            db.connect(p); db.connect(p)


class CoresTest(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(parse_cores("4-6,9"), [4, 5, 6, 9])

    def test_never_shared(self):
        pool = CorePool(range(4))
        a, b = pool.acquire(2), pool.acquire(2)
        self.assertFalse(set(a) & set(b))
        self.assertEqual(pool.acquire(1), [])
        pool.release(a)
        self.assertEqual(pool.acquire(2), a)


if __name__ == "__main__":
    unittest.main()
