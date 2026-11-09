"""One-crash fusion for string-to-integer maps, with XOR parity per key.

All public operations are serialized by a reliable simulation driver. The
backup stores encoded values and per-primary key membership, never full copies.
"""

from functools import reduce
from operator import xor

import simpy

from src.network import Network
from src.node import Node, NodeUnavailable


class Primary(Node):
    def __init__(self, env, network, node_id):
        super().__init__(env, network, node_id)
        self.data = {}
        self.ready = True
        self.register_handler("SNAPSHOT", lambda _: dict(self.data))
        self.register_handler("INSTALL", self._install)

    def _install(self, data):
        self.data = dict(data)

    def crash(self):
        super().crash()
        self.data.clear()
        self.ready = False


class FusedBackup(Node):
    def __init__(self, env, network, node_id, primary_ids):
        super().__init__(env, network, node_id)
        self.parity = {}
        self.members = {primary_id: set() for primary_id in primary_ids}
        self.ready = True
        self.register_handler("APPLY", self._apply)
        self.register_handler("SNAPSHOT", self._snapshot)
        self.register_handler("REBUILD", self._rebuild)

    def _apply(self, update):
        primary_id, key, old, new, new_present = update
        self.parity[key] = self.parity.get(key, 0) ^ old ^ new
        present = self.members[primary_id]
        if new_present:
            present.add(key)
        else:
            present.discard(key)
        if not any(key in keys for keys in self.members.values()):
            del self.parity[key]

    def _snapshot(self, _):
        return dict(self.parity), {p: set(keys) for p, keys in self.members.items()}

    def _rebuild(self, snapshots):
        self.parity.clear()
        for primary_id, data in snapshots.items():
            self.members[primary_id] = set(data)
            for key, value in data.items():
                self.parity[key] = self.parity.get(key, 0) ^ value

    def crash(self):
        super().crash()
        self.parity.clear()
        for keys in self.members.values():
            keys.clear()
        self.ready = False


class FusionCluster:
    """Use env.process(cluster.put/delete/get/recover(...)) to run operations.

The client and its lock are a reliable driver, outside the protected processes.
Writes to live primaries can continue with one primary down. A down backup
blocks writes until recovered. Do not modify Primary.data directly.
"""

    def __init__(self, env, primary_ids=(0, 1, 2), backup_id="backup"):
        primary_ids = tuple(primary_ids)
        if not primary_ids or len(set(primary_ids)) != len(primary_ids):
            raise ValueError("Provide at least one primary, with unique IDs")
        if backup_id in primary_ids or "__client__" in (*primary_ids, backup_id):
            raise ValueError("Primary, backup, and client IDs must be distinct")
        self.env = env
        self.network = Network(env)
        self.primaries = {p: Primary(env, self.network, p) for p in primary_ids}
        self.backup = FusedBackup(env, self.network, backup_id, primary_ids)
        self.client = Node(env, self.network, "__client__")
        self.lock = simpy.Resource(env, capacity=1)

    def _available(self, node):
        if not node.alive or not node.ready:
            raise NodeUnavailable(f"Node {node.node_id!r} needs recovery")

    def _single_fault(self):
        nodes = [*self.primaries.values(), self.backup]
        if sum(not n.alive or not n.ready for n in nodes) > 1:
            raise NodeUnavailable("Fusion supports only one unavailable process")

    def put(self, primary_id, key, value):
        if not isinstance(key, str) or type(value) is not int:
            raise TypeError("Keys must be strings and values must be integers")
        yield from self._write(primary_id, key, value, True)

    def delete(self, primary_id, key):
        if not isinstance(key, str):
            raise TypeError("Keys must be strings")
        yield from self._write(primary_id, key, 0, False)

    def _write(self, primary_id, key, value, present):
        with self.lock.request() as request:
            yield request
            self._single_fault()
            primary = self.primaries[primary_id]
            self._available(primary)
            self._available(self.backup)
            data = yield self.client.send_message(primary_id, "SNAPSHOT")
            if not present and key not in data:
                raise KeyError(key)
            update = (primary_id, key, data.get(key, 0), value, present)
            # Parity commits first. If the primary dies afterwards, recovery
            # includes this write, even if its acknowledgement was lost.
            yield self.client.send_message(self.backup.node_id, "APPLY", update)
            if present:
                data[key] = value
            else:
                del data[key]
            yield self.client.send_message(primary_id, "INSTALL", data)

    def get(self, primary_id, key):
        with self.lock.request() as request:
            yield request
            self._available(self.primaries[primary_id])
            data = yield self.client.send_message(primary_id, "SNAPSHOT")
            return data[key]

    def crash(self, node_id):
        """Destroy a process's volatile state immediately, including in flight."""
        node = self.backup if node_id == self.backup.node_id else self.primaries[node_id]
        node.crash()

    def recover(self, node_id):
        with self.lock.request() as request:
            yield request
            self._single_fault()
            node = self.backup if node_id == self.backup.node_id else self.primaries[node_id]
            if node.alive and node.ready:
                raise ValueError("Node is already healthy")
            if not node.alive:
                node.restart()
            snapshots = {}
            for p, primary in self.primaries.items():
                if primary is not node:
                    self._available(primary)
                    snapshots[p] = yield self.client.send_message(p, "SNAPSHOT")
            if node is self.backup:
                yield self.client.send_message(node_id, "REBUILD", snapshots)
            else:
                self._available(self.backup)
                parity, members = yield self.client.send_message(
                    self.backup.node_id, "SNAPSHOT"
                )
                recovered = {
                    key: reduce(xor, (data.get(key, 0) for data in snapshots.values()),
                                parity[key])
                    for key in members[node_id]
                }
                yield self.client.send_message(node_id, "INSTALL", recovered)
            self._single_fault()
            if not node.alive:
                raise NodeUnavailable("Node crashed during recovery")
            node.ready = True
