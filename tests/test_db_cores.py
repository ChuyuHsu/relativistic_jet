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


class PhysicalPoolTest(unittest.TestCase):
    # 2 路×4 實體核×2 執行緒：cpu n 與 n+8 是同一個實體核
    TOPO = {c: frozenset([c % 8, c % 8 + 8]) for c in range(16)}

    def pool(self, cores, threads="all"):
        from jet.cores import PhysicalPool
        return PhysicalPool(cores, threads, self.TOPO)

    def test_siblings_never_split_between_jobs(self):
        p = self.pool(range(16))
        a, b = p.acquire(2), p.acquire(2)
        self.assertEqual(len(a), 4)
        phys = lambda cs: {c % 8 for c in cs}
        self.assertFalse(phys(a) & phys(b))                    # 兩個槽沒有共用任何實體核
        p.release(a)
        self.assertEqual(p.free_count(), 6)

    def test_partial_core_in_pool_is_excluded(self):
        p = self.pool(list(range(8, 16)) + [4])                 # cpu 4 的兄弟 12 在池裡，但 cpu 4 自己…（兄弟 4 在、12 在）
        self.assertIn(4, p.all)
        p2 = self.pool(list(range(0, 8)))                        # 只給前半：每個實體核的另一條在池外＝保留
        self.assertEqual(p2.all, [])
        self.assertEqual(sorted(p2.excluded), list(range(8)))

    def test_one_thread_mode_holds_sibling_idle(self):
        p = self.pool(range(16), "one")
        got = p.acquire(1)
        self.assertEqual(len(got), 1)
        sib = next(iter(self.TOPO[got[0]] - {got[0]}))
        self.assertIn(sib, p.busy)                               # 兄弟被占住，不會給別人
        self.assertEqual(p.free_count(), 7)

    def test_foreign_busy_thread_blocks_whole_core(self):
        p = self.pool(range(16))
        p.busy.add(3)                                            # 外人占了一條執行緒
        got = p.acquire(8)
        self.assertEqual(got, [])                                # 8 個實體核，剩 7 個，不部分給
        self.assertEqual(len(p.acquire(7)), 14)

    def test_read_topology_from_sysfs_layout(self):
        import tempfile
        from jet.cores import read_topology
        with tempfile.TemporaryDirectory() as t:
            for c, sib in ((0, "0,2"), (1, "1,3"), (2, "0,2"), (3, "1,3")):
                d = os.path.join(t, f"cpu{c}", "topology")
                os.makedirs(d)
                open(os.path.join(d, "thread_siblings_list"), "w").write(sib + "\n")
            topo = read_topology(t)
            self.assertEqual(topo[0], frozenset([0, 2]))
            self.assertEqual(topo[3], frozenset([1, 3]))
