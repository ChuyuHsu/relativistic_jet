import unittest
from jet.states import State, Event, transition, IllegalTransition


class StatesTest(unittest.TestCase):
    def test_happy_path(self):
        s = State.PENDING
        for ev, want in [(Event.SELFTEST_START, State.SELFTESTING),
                         (Event.SELFTEST_PASS, State.SELFTEST_GREEN),
                         (Event.READY, State.QUEUED),
                         (Event.BATCH_START, State.BATCHING),
                         (Event.BATCH_MERGED, State.MERGED)]:
            s = transition(s, ev)
            self.assertEqual(s, want)

    def test_new_push_resets_unless_merged(self):
        for s in (State.QUEUED, State.REJECTED, State.HALTED, State.BATCHING):
            self.assertEqual(transition(s, Event.PUSHED), State.PENDING)
        with self.assertRaises(IllegalTransition):
            transition(State.MERGED, Event.PUSHED)

    def test_selftest_must_be_green_before_queue(self):
        with self.assertRaises(IllegalTransition):
            transition(State.PENDING, Event.READY)

    def test_parent_changed_sends_child_back(self):
        self.assertEqual(transition(State.QUEUED, Event.PARENT_CHANGED), State.PENDING)

    def test_approve_does_not_change_state(self):
        self.assertEqual(transition(State.SELFTESTING, Event.APPROVE), State.SELFTESTING)

    def test_halt_and_resume(self):
        self.assertEqual(transition(State.BATCHING, Event.DOUBLE_FAIL), State.HALTED)
        self.assertEqual(transition(State.HALTED, Event.RESUME), State.PENDING)


if __name__ == "__main__":
    unittest.main()
