"""JET-07：fetch 失敗先原地重試，都失敗才記錯誤、發警示。"""
import unittest

from jet import store
from tests.test_scheduler import Base


def errors(con):
    return [r[0] for r in con.execute("SELECT detail FROM events WHERE kind='queue_error'")]


class FetchRetryTest(Base):
    def flaky(self, fails):
        s = self.mk()
        s.FETCH_RETRY_WAIT_S = 0
        s.async_fetch = False
        self.calls = 0

        def fetch():
            self.calls += 1
            if self.calls <= fails:
                raise RuntimeError("Permission denied (publickey)")
        self.repo.fetch = fetch
        return s

    def test_first_fails_second_succeeds_no_alert(self):
        s = self.flaky(1)
        s.tick()
        self.assertEqual(self.calls, 2)
        self.assertEqual([e for e in errors(self.con) if "fetch" in e], [])

    def test_two_failures_still_recovers(self):
        s = self.flaky(2)
        s.tick()
        self.assertEqual(self.calls, 3)
        self.assertEqual([e for e in errors(self.con) if "fetch" in e], [])

    def test_all_fail_alerts_once(self):
        s = self.flaky(99)
        with self.assertRaises(RuntimeError):
            s.tick()
        self.assertEqual(self.calls, 3)                          # 一次加兩次重試
        self.assertEqual(len([e for e in errors(self.con) if "fetch" in e]), 1)

    def test_async_worker_retries_too(self):
        s = self.flaky(1)
        s._fetch_worker(0.0)
        self.assertEqual(self.calls, 2)
        self.assertTrue(s._fetch_res[0])


if __name__ == "__main__":
    unittest.main()
