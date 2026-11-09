import random
import unittest
from functools import reduce
from operator import xor

import simpy

from src.fusion import FusionCluster
from src.network import Network
from src.node import Node, NodeUnavailable


class FusionTests(unittest.TestCase):
    def setUp(self):
        self.env = simpy.Environment()
        self.cluster = FusionCluster(self.env)

    def run_operation(self, operation):
        return self.env.run(until=self.env.process(operation))

    def assert_consistent(self):
        c = self.cluster
        keys = set().union(*(p.data for p in c.primaries.values()))
        self.assertEqual(c.backup.parity, {
            k: reduce(xor, (p.data.get(k, 0) for p in c.primaries.values()), 0)
            for k in keys
        })
        self.assertEqual(c.backup.members,
                         {p: set(n.data) for p, n in c.primaries.items()})
        self.assertFalse(c.client.pending)

    def seed(self):
        for p in range(3):
            self.run_operation(self.cluster.put(p, "shared", 10 + p))
            self.run_operation(self.cluster.put(p, f"only-{p}", -p))

    def test_primary_recovery_and_continued_writes(self):
        self.seed()
        expected = dict(self.cluster.primaries[1].data)
        self.cluster.crash(1)
        self.assertEqual(self.cluster.primaries[1].data, {})
        with self.assertRaises(NodeUnavailable):
            self.run_operation(self.cluster.get(1, "shared"))
        self.run_operation(self.cluster.put(0, "shared", 99))
        self.run_operation(self.cluster.delete(2, "shared"))
        self.run_operation(self.cluster.recover(1))
        self.assertEqual(self.cluster.primaries[1].data, expected)
        self.run_operation(self.cluster.put(1, "after-restart", 123))
        self.assertEqual(self.run_operation(self.cluster.get(1, "after-restart")), 123)
        self.assert_consistent()

    def test_backup_recovery(self):
        self.seed()
        expected = self.cluster.backup._snapshot(None)
        self.cluster.crash("backup")
        self.assertEqual(self.cluster.backup.parity, {})
        with self.assertRaises(NodeUnavailable):
            self.run_operation(self.cluster.put(0, "blocked", 10))
        self.assertEqual(self.run_operation(self.cluster.get(0, "shared")), 10)
        self.run_operation(self.cluster.recover("backup"))
        self.assertEqual(self.cluster.backup._snapshot(None), expected)
        self.run_operation(self.cluster.put(0, "continued", 2))
        self.assert_consistent()

    def test_zero_negative_values_deletes_and_empty_recovery(self):
        self.run_operation(self.cluster.put(0, "zero", 0))
        self.run_operation(self.cluster.put(1, "zero", 0))
        self.run_operation(self.cluster.put(0, "negative", -123456789))
        self.run_operation(self.cluster.delete(0, "zero"))
        self.cluster.crash(0)
        self.run_operation(self.cluster.recover(0))
        self.assertEqual(self.cluster.primaries[0].data, {"negative": -123456789})
        self.run_operation(self.cluster.delete(0, "negative"))
        self.cluster.crash(0)
        self.run_operation(self.cluster.recover(0))
        self.assertEqual(self.cluster.primaries[0].data, {})
        self.assert_consistent()

    def test_concurrent_calls_are_serialized(self):
        operations = [self.env.process(self.cluster.put(p % 3, "key", p))
                      for p in range(30)]
        self.env.run(until=self.env.all_of(operations))
        self.assertEqual([p.data["key"] for p in self.cluster.primaries.values()],
                         [27, 28, 29])
        self.assert_consistent()

    def test_crash_at_every_stage_of_write(self):
        for victim in (0, 1, "backup"):
            for delay in range(14):
                with self.subTest(victim=victim, delay=delay):
                    self.setUp()
                    self.seed()

                    def crash_later():
                        yield self.env.timeout(delay)
                        self.cluster.crash(victim)

                    crash = self.env.process(crash_later())
                    acknowledged = False
                    try:
                        self.run_operation(self.cluster.put(0, "shared", 100))
                        acknowledged = True
                    except NodeUnavailable:
                        pass
                    if not crash.processed:
                        self.env.run(until=crash)
                    self.run_operation(self.cluster.recover(victim))
                    self.assert_consistent()
                    actual = self.cluster.primaries[0].data["shared"]
                    self.assertIn(actual, (10, 100))
                    if acknowledged:
                        self.assertEqual(actual, 100)
                    self.run_operation(self.cluster.put(0, "continued", 200))
                    self.assert_consistent()

    def test_crash_at_every_stage_of_delete(self):
        for victim in (0, "backup"):
            for delay in range(14):
                with self.subTest(victim=victim, delay=delay):
                    self.setUp()
                    self.seed()

                    def crash_later():
                        yield self.env.timeout(delay)
                        self.cluster.crash(victim)

                    crash = self.env.process(crash_later())
                    acknowledged = False
                    try:
                        self.run_operation(self.cluster.delete(0, "shared"))
                        acknowledged = True
                    except NodeUnavailable:
                        pass
                    if not crash.processed:
                        self.env.run(until=crash)
                    self.run_operation(self.cluster.recover(victim))
                    if acknowledged:
                        self.assertNotIn("shared", self.cluster.primaries[0].data)
                    self.assert_consistent()

    def test_recovery_interrupted_by_another_crash_of_same_primary(self):
        self.seed()
        expected = dict(self.cluster.primaries[0].data)
        self.cluster.crash(0)

        def crash_again():
            yield self.env.timeout(13)
            self.cluster.crash(0)

        self.env.process(crash_again())
        with self.assertRaises(NodeUnavailable):
            self.run_operation(self.cluster.recover(0))
        self.assertFalse(self.cluster.primaries[0].ready)
        self.run_operation(self.cluster.recover(0))
        self.assertEqual(self.cluster.primaries[0].data, expected)
        self.assert_consistent()

    def test_in_flight_request_does_not_reach_restarted_incarnation(self):
        network = Network(self.env)
        sender, target = Node(self.env, network, "sender"), Node(self.env, network, "target")
        received = []
        target.register_handler("APPEND", received.append)
        reply = sender.send_message("target", "APPEND", 42)

        def restart_before_delivery():
            yield self.env.timeout(1)
            target.crash()
            target.restart()

        self.env.process(restart_before_delivery())
        with self.assertRaises(NodeUnavailable):
            self.env.run(until=reply)
        self.assertEqual(received, [])
        self.env.run(until=sender.send_message("target", "APPEND", 43))
        self.assertEqual(received, [43])
        self.assertFalse(sender.pending)

    def test_repeated_random_operations_and_recoveries(self):
        rng = random.Random(17)
        expected = {p: {} for p in range(3)}
        for _ in range(150):
            p, key = rng.randrange(3), str(rng.randrange(6))
            if key in expected[p] and rng.random() < 0.3:
                self.run_operation(self.cluster.delete(p, key))
                del expected[p][key]
            else:
                value = rng.randrange(-100, 100)
                self.run_operation(self.cluster.put(p, key, value))
                expected[p][key] = value
            if rng.random() < 0.25:
                victim = rng.choice([0, 1, 2, "backup"])
                self.cluster.crash(victim)
                self.run_operation(self.cluster.recover(victim))
            self.assertEqual({p: n.data for p, n in self.cluster.primaries.items()},
                             expected)
            self.assert_consistent()

    def test_two_failures_are_rejected(self):
        self.cluster.crash(0)
        self.cluster.crash(1)
        with self.assertRaisesRegex(NodeUnavailable, "only one"):
            self.run_operation(self.cluster.recover(0))

    def test_invalid_operations_leave_state_unchanged(self):
        with self.assertRaises(TypeError):
            self.run_operation(self.cluster.put(0, "key", "value"))
        with self.assertRaises(KeyError):
            self.run_operation(self.cluster.delete(0, "missing"))
        with self.assertRaisesRegex(ValueError, "Unknown request"):
            self.env.run(until=self.cluster.client.send_message(0, "INVALID"))
        self.assert_consistent()


if __name__ == "__main__":
    unittest.main()
