"""From the project root: python -B -m unittest -v chord.test.test_chord"""

import unittest
from unittest.mock import patch

import simpy

from chord.src.chord import Chord
from chord.src.network import Network
from chord.src.node import Node


class ChordTests(unittest.TestCase):
    def setUp(self):
        self.env = simpy.Environment()
        self.network = Network(self.env)

    def wait(self, event):
        self.env.run(until=event | self.env.timeout(100000))
        self.assertTrue(event.triggered, "Operation stalled waiting for a reply")
        return event.value

    def add_node(self, ip, bootstrap=None):
        node = Node(self.env, self.network, ip, bootstrap)
        self.wait(node.ready)
        return node

    def check_ring(self, nodes, keys):
        ids = sorted(node.chord.id for node in nodes)

        def successor(key):
            return next((node_id for node_id in ids if node_id >= key), ids[0])

        for node in nodes:
            chord = node.chord
            index = ids.index(chord.id)
            self.assertEqual(chord.predecessor, ids[index - 1])
            self.assertEqual(chord.successor, ids[(index + 1) % len(ids)])
            self.assertEqual(
                chord.fingerNode, [successor(start) for start in chord.fingerStart]
            )
            self.assertEqual(chord.fingerInterval[-1][1], chord.id)
            for key in keys:
                result = self.wait(self.env.process(
                    chord._find_successor(chord.id, key)
                ))
                self.assertEqual(result, successor(key % (2**chord.m)))
            self.assertFalse(node.pending)

    def test_singleton(self):
        node = self.add_node("first")
        self.check_ring([node], [0, node.chord.id, 2**160 - 1])

    def test_all_keys_and_fingers_after_each_join(self):
        # Deliberate boundary IDs, including bootstrap ID 0 and adjacent nodes.
        with patch.object(Chord, "m", 4), patch.object(
            Chord, "_hash_string", lambda self, value: int(value)
        ):
            nodes = []
            for node_id in [0, 14, 3, 7, 1, 15, 8, 11, 6]:
                nodes.append(self.add_node(
                    str(node_id), nodes[0].chord.id if nodes else None
                ))
                self.check_ring(nodes, range(16))

    def test_full_small_ring(self):
        with patch.object(Chord, "m", 3), patch.object(
            Chord, "_hash_string", lambda self, value: int(value)
        ):
            nodes = []
            for node_id in [4, 0, 7, 2, 5, 1, 6, 3]:
                nodes.append(self.add_node(
                    str(node_id), nodes[-1].chord.id if nodes else None
                ))
                self.check_ring(nodes, range(8))
            with self.assertRaisesRegex(ValueError, "full"):
                Node(self.env, self.network, "0", 0)

    def test_sha160_remote_and_concurrent_lookups(self):
        nodes = [self.add_node("first")]
        for ip in ["second", "third", "fourth"]:
            nodes.append(self.add_node(ip, nodes[-1].chord.id))
        keys = [0, 2**160 - 1] + [node.chord.id for node in nodes]
        self.check_ring(nodes, keys)
        ids = sorted(node.chord.id for node in nodes)
        queries = []
        for node in nodes:
            remote = next(other for other in nodes if other is not node)
            for key in keys:
                queries.append((key, self.env.process(
                    node.chord._find_successor(remote.chord.id, key)
                )))
        self.wait(self.env.all_of([event for _, event in queries]))
        for key, event in queries:
            expected = next((node_id for node_id in ids if node_id >= key), ids[0])
            self.assertEqual(event.value, expected)
        self.assertTrue(all(not node.pending for node in nodes))

    def test_collision_gets_unique_address(self):
        with patch.object(Chord, "m", 4):
            first = self.add_node("same-address")
            second = self.add_node("same-address", first.chord.id)
            self.assertNotEqual(first.chord.id, second.chord.id)
            self.check_ring([first, second], range(16))

    def test_invalid_requests_fail_instead_of_hanging(self):
        first = self.add_node("first")
        with self.assertRaisesRegex(ValueError, "bootstrap"):
            Node(self.env, self.network, "second", -1)
        with self.assertRaisesRegex(ValueError, "destination"):
            first.send_message(-1, "GET_SUCCESSOR")
        event = first.send_message(first.chord.id, "NOT_A_COMMAND")
        with self.assertRaisesRegex(ValueError, "Unknown request"):
            self.wait(event)
        self.assertFalse(first.pending)


if __name__ == "__main__":
    unittest.main()
