"""JET-20：_collect 走訪途中，前一項收尾連帶放掉的 job 不再重收（queue_error KeyError）。"""
import unittest
from types import SimpleNamespace as NS

from jet.executor import JobResult
from tests.test_scheduler import Base


class CollectSkipTest(Base):
    def test_job_released_by_earlier_item_is_skipped(self):
        s = self.mk()
        j1, j2 = NS(handle="h1", kind="x"), NS(handle="h2", kind="x")
        s.jobs = {1: j1, 2: j2}
        polled = []

        class Ex:
            def poll(self, h):
                polled.append(h)
                return JobResult(status="pass", exit_code=0, failed_items=[])
        s.executor = Ex()

        def release(rid, status):
            s.jobs.pop(rid)
            s.jobs.pop(2, None)             # 收 1 時連帶放掉 2（過期探測被取消）
        s._release_job = release
        s.con.execute("INSERT INTO runs(id,kind,status,layer,started_at,commit_sha,base_sha) VALUES(1,'x','running','gate',1.0,'c','b')")
        s._collect()                        # 以前這裡會 KeyError: 2
        self.assertEqual(polled, ["h1"])
        self.assertEqual(s.jobs, {})


if __name__ == "__main__":
    unittest.main()
