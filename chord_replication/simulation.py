"""Static Chord successor replication using the fusion benchmark's network model.

Run python -m chord_replication.simulation --output /tmp/chord_comparison.csv.
"""

import argparse
from bisect import bisect_left
import csv
from dataclasses import dataclass, field
from pathlib import Path
import random
from statistics import mean
from time import process_time

import simpy

from fusion_p2p.simulation import Simulation as SharedSimulation, _deep_size, run


@dataclass
class Node:
    node_id: int
    alive: bool = True
    store: dict[str, str] = field(default_factory=dict)
    replicas: dict[int, dict[str, str]] = field(default_factory=dict)

    def erase(self):
        self.alive = False
        self.store.clear()
        self.replicas.clear()


class Simulation(SharedSimulation):
    """Reuse ownership hashing, successor routing, RPCs, reads, and crash events.

    Storage, writes, validation, and recovery use ordinary string dictionaries.
    Run generator operations with env.process(...) or run(env, operation).
    """

    def __init__(self, env, n=4, k=1, max_key_bytes=64, max_value_bytes=256,
                 network_delay=2, bandwidth=100_000):
        if type(n) is not int or type(k) is not int or not 1 <= k < n:
            raise ValueError("Replication requires 1 <= k < n")
        if (type(max_key_bytes) is not int or type(max_value_bytes) is not int
                or not 0 <= max_key_bytes < 2**32 or not 0 <= max_value_bytes < 2**32):
            raise ValueError("String limits must be nonnegative 32-bit integers")
        if network_delay < 0 or bandwidth <= 0:
            raise ValueError("Delay must be nonnegative and bandwidth positive")
        self.env, self.n, self.k = env, n, k
        self.scheme = "chord_replication"
        self.max_key_bytes, self.max_value_bytes = max_key_bytes, max_value_bytes
        self.network_delay, self.bandwidth = network_delay, bandwidth
        self.node_ids = sorted(self.hash_key(f"node-{i}") for i in range(n))
        if len(set(self.node_ids)) != n:
            raise ValueError("Node identifiers collided")
        self.nodes = [Node(node_id) for node_id in self.node_ids]
        for host, node in enumerate(self.nodes):
            node.replicas = {owner: {} for owner in range(n) if host in self.replica_hosts(owner)}
        self.failed = set()
        self.busy = False
        self.recovering = False
        self.last_coordinator = None
        self.messages = 0
        self.bytes_transferred = 0
        self.latencies = {operation: [] for operation in ("put", "get", "delete", "recover")}

    def owner(self, key):
        if not isinstance(key, str):
            raise TypeError("Keys must be strings")
        if len(key.encode("utf-8")) > self.max_key_bytes:
            raise ValueError("Key exceeds maximum UTF-8 byte length")
        return bisect_left(self.node_ids, self.hash_key(key)) % self.n

    def put(self, key, value):
        self._start()
        started = self.env.now
        try:
            owner = self.owner(key)
            if not isinstance(value, str):
                raise TypeError("Values must be strings")
            if len(value.encode("utf-8")) > self.max_value_bytes:
                raise ValueError("Value exceeds maximum UTF-8 byte length")
            request_bytes = 9 + len(key.encode("utf-8")) + len(value.encode("utf-8"))
            yield from self._route(owner, request_bytes)
            yield from self._parallel([
                self._rpc(owner, host, request_bytes) for host in self.replica_hosts(owner)
            ])
            # Commit all copies together, after all replication RPCs complete.
            self.nodes[owner].store[key] = value
            for host in self.replica_hosts(owner):
                self.nodes[host].replicas[owner][key] = value
            yield from self._transfer(owner, 0, 1)
            self.latencies["put"].append(self.env.now - started)
        finally:
            self.busy = False

    def delete(self, key):
        self._start()
        started = self.env.now
        try:
            owner = self.owner(key)
            request_bytes = 5 + len(key.encode("utf-8"))
            yield from self._route(owner, request_bytes)
            if key in self.nodes[owner].store:
                yield from self._parallel([
                    self._rpc(owner, host, request_bytes) for host in self.replica_hosts(owner)
                ])
                del self.nodes[owner].store[key]
                for host in self.replica_hosts(owner):
                    del self.nodes[host].replicas[owner][key]
            yield from self._transfer(owner, 0, 1)
            self.latencies["delete"].append(self.env.now - started)
        finally:
            self.busy = False

    @staticmethod
    def _snapshot_bytes(store):
        # Eight-byte record count, then four-byte UTF-8 lengths per key/value.
        return 8 + sum(8 + len(key.encode("utf-8")) + len(value.encode("utf-8"))
                       for key, value in store.items())

    def recover(self):
        self._start(recovery=True)
        self.recovering = True
        started = self.env.now
        try:
            if not self.failed:
                raise ValueError("No failed nodes to recover")
            missing = sorted(self.failed)
            survivors = set(range(self.n)) - self.failed
            coordinator = min(survivors)
            self.last_coordinator = coordinator
            restored = {}
            transfers = []
            for owner in missing:
                source = next(host for host in self.replica_hosts(owner) if host in survivors)
                store = dict(self.nodes[source].replicas[owner])
                restored[owner] = Node(self.node_ids[owner], store=store)
                transfers.append(self._rpc(coordinator, source, 9, self._snapshot_bytes(store)))
            yield from self._parallel(transfers)
            yield from self._parallel([
                self._rpc(coordinator, owner, self._snapshot_bytes(node.store))
                for owner, node in restored.items()
            ])
            candidate = [restored.get(i, node) for i, node in enumerate(self.nodes)]
            # Fetch the other owners whose replica copies were lost with hosts.
            owners = [owner for owner in range(self.n)
                      if any(host in self.failed for host in self.replica_hosts(owner))]
            yield from self._parallel([
                self._rpc(coordinator, owner, 9, self._snapshot_bytes(candidate[owner].store))
                for owner in owners if owner in survivors
            ])
            transfers = []
            for owner in owners:
                for host in self.replica_hosts(owner):
                    if host in self.failed:
                        candidate[host].replicas[owner] = dict(candidate[owner].store)
                        transfers.append(self._rpc(coordinator, host,
                                                   self._snapshot_bytes(candidate[owner].store)))
            yield from self._parallel(transfers)
            self._verify(candidate)
            for owner in missing:
                self.nodes[owner] = candidate[owner]
            self.failed.clear()
            self.latencies["recover"].append(self.env.now - started)
        finally:
            self.recovering = False
            self.busy = False

    def _verify(self, nodes):
        for owner, node in enumerate(nodes):
            if node.node_id != self.node_ids[owner] or not node.alive:
                raise ValueError("Node identity or availability is inconsistent")
            for key, value in node.store.items():
                if self.owner(key) != owner or not isinstance(value, str):
                    raise ValueError("Primary record has incorrect ownership or type")
                if len(value.encode("utf-8")) > self.max_value_bytes:
                    raise ValueError("Primary value exceeds UTF-8 limit")
            expected = {primary: nodes[primary].store for primary in range(self.n)
                        if owner in self.replica_hosts(primary)}
            if node.replicas != expected:
                raise ValueError("Replica data or successor placement is inconsistent")

    def storage_stats(self):
        primary = sum(_deep_size(node.store, set()) for node in self.nodes)
        backup = sum(_deep_size(node.replicas, set()) for node in self.nodes)
        payload = sum(len(key.encode("utf-8")) + len(value.encode("utf-8"))
                      for node in self.nodes for key, value in node.store.items())
        backup_payload = sum(self._snapshot_bytes(replica)
                             for node in self.nodes for replica in node.replicas.values())
        metadata = _deep_size((self.node_ids, self.n, self.k,
                               self.max_key_bytes, self.max_value_bytes), set())
        return {
            "primary_storage_bytes": primary, "backup_storage_bytes": backup,
            "global_metadata_bytes": metadata,
            "storage_overhead": backup / primary if primary else 0,
            "total_storage_bytes": primary + backup + metadata,
            "primary_payload_bytes": payload, "backup_payload_bytes": backup_payload,
        }


def benchmark(output, configurations=((4, 1), (4, 2), (5, 2), (8, 1)),
              records_per_node=(8, 24), value_sizes=(32, 128), seed=17,
              network_delay=2, bandwidth=100_000, compare_fusion=True):
    """Paired workloads match fusion_p2p's benchmark, with conventional replicas."""
    rows = []
    schemes = ("fusion", "chord_replication") if compare_fusion else ("chord_replication",)
    for n, k in configurations:
        for count in records_per_node:
            for value_size in value_sizes:
                rng = random.Random(seed)
                workload = [(f"key-{i:06d}", "".join(rng.choices("abcdefgh01234567", k=value_size)))
                            for i in range(n * count)]
                for scheme in schemes:
                    env = simpy.Environment()
                    factory = SharedSimulation if scheme == "fusion" else Simulation
                    system = factory(env, n=n, k=k, max_key_bytes=32,
                                     max_value_bytes=value_size, network_delay=network_delay,
                                     bandwidth=bandwidth)
                    started_cpu = process_time()
                    for key, value in workload:
                        run(env, system.put(key, value))
                    for key, value in workload[::5]:
                        run(env, system.put(key, value[::-1]))
                    for key, _ in workload:
                        run(env, system.get(key))
                    for key, _ in workload[::7]:
                        run(env, system.delete(key))
                    system.verify()
                    storage = system.storage_stats()
                    before_messages, before_bytes = system.messages, system.bytes_transferred
                    run(env, system.crash(range(k)))
                    recovery_cpu = process_time()
                    run(env, system.recover())
                    recovery_cpu = process_time() - recovery_cpu
                    system.verify()
                    rows.append({
                        "scheme": scheme, "n": n, "k": k,
                        "records_per_node_target": count, "value_bytes": value_size, "seed": seed,
                        "network_delay": network_delay, "bandwidth_bytes_per_unit": bandwidth,
                        "record_bytes": system.codec.size if scheme == "fusion" else 0,
                        "index_range": system.committed_range if scheme == "fusion" else 0,
                        "parity_record_bytes": system.code.parity_size if scheme == "fusion" else 0,
                        "field_bits": system.code.field.bits if scheme == "fusion" else 0,
                        **storage,
                        "write_latency": mean(system.latencies["put"]),
                        "read_latency": mean(system.latencies["get"]),
                        "delete_latency": mean(system.latencies["delete"]),
                        "recovery_time": system.latencies["recover"][-1],
                        "message_count": system.messages, "bytes_transferred": system.bytes_transferred,
                        "recovery_messages": system.messages - before_messages,
                        "recovery_bytes": system.bytes_transferred - before_bytes,
                        "actual_cpu_seconds": process_time() - started_cpu,
                        "recovery_cpu_seconds": recovery_cpu,
                    })
    if not rows:
        raise ValueError("Benchmark needs at least one workload configuration")
    with Path(output).open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="/tmp/chord_comparison.csv")
    parser.add_argument("--configs", nargs="+", default=["4:1", "4:2", "5:2", "8:1"])
    parser.add_argument("--records", nargs="+", type=int, default=[8, 24])
    parser.add_argument("--value-bytes", nargs="+", type=int, default=[32, 128])
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--delay", type=float, default=2)
    parser.add_argument("--bandwidth", type=float, default=100_000)
    parser.add_argument("--replication-only", action="store_true")
    args = parser.parse_args()
    if any(count <= 0 for count in args.records) or any(size < 0 for size in args.value_bytes):
        parser.error("Record counts must be positive and value sizes nonnegative")
    try:
        configurations = [tuple(map(int, item.split(":"))) for item in args.configs]
        if any(len(pair) != 2 for pair in configurations):
            raise ValueError("Configurations must use n:k")
        rows = benchmark(args.output, configurations, args.records, args.value_bytes,
                         args.seed, args.delay, args.bandwidth, not args.replication_only)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    for row in rows:
        print(f"{row['scheme']:18} n={row['n']} k={row['k']} "
              f"records={row['records_per_node_target']:3} value={row['value_bytes']:3} "
              f"backup={row['backup_storage_bytes']:7}B "
              f"write={row['write_latency']:.4f} recovery={row['recovery_time']:.4f}")
    print(f"Wrote {len(rows)} rows to {args.output}. Latencies use simulated time units.")


if __name__ == "__main__":
    main()
