"""Static Chord-like storage with distributed fusion or successor replication.

Run benchmarks with python -m fusion_p2p.simulation --output /tmp/fusion.csv.
Operations are SimPy generators. One driver coordinates atomic transactions.
"""

import argparse
from bisect import bisect_left
import csv
from dataclasses import dataclass, field
import hashlib
import heapq
from pathlib import Path
import random
from statistics import mean
import sys
from time import process_time

import simpy

from .coding import CauchyCode, Record, RecordCodec, xor_bytes


@dataclass
class Node:
    node_id: int
    alive: bool = True
    store: dict = field(default_factory=dict)
    key_to_index: dict = field(default_factory=dict)
    records: list = field(default_factory=list)
    free_indices: list = field(default_factory=list)
    parity: dict = field(default_factory=dict)
    replicas: dict = field(default_factory=dict)

    def erase(self):
        self.alive = False
        self.store.clear()
        self.key_to_index.clear()
        self.records.clear()
        self.free_indices.clear()
        self.parity.clear()
        self.replicas.clear()


def _deep_size(value, seen):
    """Count Python representation bytes, with shared references counted once."""
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(_deep_size(key, seen) + _deep_size(item, seen)
                    for key, item in value.items())
    elif isinstance(value, (tuple, list, set)):
        size += sum(_deep_size(item, seen) for item in value)
    elif isinstance(value, Record):
        size += _deep_size(value.__dict__, seen)
    return size


class Simulation:
    def __init__(self, env, n=4, k=1, scheme="fusion", max_key_bytes=64,
                 max_value_bytes=256, network_delay=2, bandwidth=100_000):
        self.codec = RecordCodec(max_key_bytes, max_value_bytes)
        self.code = CauchyCode(n, k, self.codec.size)
        if scheme not in ("fusion", "replication"):
            raise ValueError("Scheme must be fusion or replication")
        if network_delay < 0 or bandwidth <= 0:
            raise ValueError("Delay must be nonnegative and bandwidth positive")
        self.env, self.n, self.k, self.scheme = env, n, k, scheme
        self.network_delay, self.bandwidth = network_delay, bandwidth
        self.node_ids = sorted(self.hash_key(f"node-{i}") for i in range(n))
        if len(set(self.node_ids)) != n:
            raise ValueError("Node identifiers collided")
        self.nodes = [Node(node_id) for node_id in self.node_ids]
        self.placement = {
            p: tuple((p * n // k + offset) % n for offset in range(k + 1))
            for p in range(k)
        }
        self.committed_range = 0
        self.failed = set()
        self.busy = False
        self.recovering = False
        self.last_coordinator = None
        self.messages = 0
        self.bytes_transferred = 0
        self.latencies = {operation: [] for operation in ("put", "get", "delete", "recover")}
        for host, node in enumerate(self.nodes):
            if scheme == "fusion":
                node.parity = {p: {} for p, hosts in self.placement.items() if host in hosts}
            else:
                node.replicas = {owner: {} for owner in range(n)
                                 if host in self.replica_hosts(owner)}

    @staticmethod
    def hash_key(key):
        return int.from_bytes(hashlib.sha1(key.encode("utf-8")).digest(), "big")

    def owner(self, key):
        self.codec.validate_key(key)
        return bisect_left(self.node_ids, self.hash_key(key)) % self.n

    def replica_hosts(self, owner):
        return tuple((owner + offset) % self.n for offset in range(1, self.k + 1))

    def _start(self, recovery=False):
        if self.busy or self.recovering:
            raise RuntimeError("Another operation or recovery is active")
        if self.failed and not recovery:
            raise RuntimeError("Recover failed nodes before accepting requests")
        self.busy = True

    def _transfer(self, sender, receiver, payload_bytes):
        # Recovery may send state to a returning node before it becomes ready.
        if sender == receiver:
            yield self.env.timeout(0)
            return
        self.messages += 1
        self.bytes_transferred += payload_bytes
        yield self.env.timeout(self.network_delay + payload_bytes / self.bandwidth)

    def _rpc(self, sender, receiver, request_bytes, reply_bytes=1):
        yield from self._transfer(sender, receiver, request_bytes)
        yield from self._transfer(receiver, sender, reply_bytes)

    def _parallel(self, transfers):
        yield self.env.all_of([self.env.process(transfer) for transfer in transfers])

    def _route(self, owner, request_bytes):
        # Requests enter at ring position 0 and walk successor links.
        for sender in range(owner):
            yield from self._transfer(sender, sender + 1, request_bytes)

    def _encoded(self, node, index):
        record = node.records[index] if index < len(node.records) else None
        return self.codec.encode(record)

    def put(self, key, value):
        self._start()
        started = self.env.now
        try:
            record = Record(key, value)
            new = self.codec.encode(record)
            owner = self.owner(key)
            node = self.nodes[owner]
            if key in node.key_to_index:
                index = node.key_to_index[key]
            else:
                index = node.free_indices[0] if node.free_indices else len(node.records)
            yield from self._write(owner, index, record, new)
            self.latencies["put"].append(self.env.now - started)
        finally:
            self.busy = False

    def delete(self, key):
        self._start()
        started = self.env.now
        try:
            owner = self.owner(key)
            node = self.nodes[owner]
            if key in node.key_to_index:
                yield from self._write(owner, node.key_to_index[key], None, self.codec.empty)
            else:
                yield from self._route(owner, 5 + len(key.encode("utf-8")))
                yield from self._transfer(owner, 0, 1)
            self.latencies["delete"].append(self.env.now - started)
        finally:
            self.busy = False

    def _write(self, owner, index, record, new):
        node = self.nodes[owner]
        old_record = node.records[index] if index < len(node.records) else None
        old = self.codec.encode(old_record)
        key = record.key if record is not None else old_record.key
        request_bytes = 9 + len(key.encode("utf-8"))
        if record is not None:
            request_bytes += len(record.value.encode("utf-8"))
        yield from self._route(owner, request_bytes)
        changes = []
        transfers = []
        if self.scheme == "fusion":
            for stream, hosts in self.placement.items():
                contribution = self.code.update(self.code.empty, stream, owner, old, new)
                for host in hosts:
                    # The update message carries the weighted delta. Each host
                    # combines it with its own local parity, without remote reads.
                    current = self.nodes[host].parity[stream].get(index, self.code.empty)
                    updated = xor_bytes(current, contribution)
                    changes.append((host, stream, updated))
                    transfers.append(self._rpc(owner, host, 10 + self.code.parity_size))
        else:
            for host in self.replica_hosts(owner):
                transfers.append(self._rpc(owner, host, request_bytes + 8))
        yield from self._parallel(transfers)
        # All messages complete before this atomic, non-yielding commit.
        if record is None:
            del node.store[key]
            del node.key_to_index[key]
            node.records[index] = None
            heapq.heappush(node.free_indices, index)
        else:
            if index == len(node.records):
                node.records.append(record)
            else:
                if node.records[index] is None:
                    reused = heapq.heappop(node.free_indices)
                    assert reused == index
                node.records[index] = record
            node.store[key] = record.value
            node.key_to_index[key] = index
        self.committed_range = max(self.committed_range, len(node.records))
        if self.scheme == "fusion":
            for host, stream, updated in changes:
                self.nodes[host].parity[stream][index] = bytes(bytearray(updated))
        else:
            for host in self.replica_hosts(owner):
                replica = self.nodes[host].replicas[owner]
                if record is None:
                    del replica[key]
                else:
                    replica[key] = (index, record.value)
        yield from self._transfer(owner, 0, 1)

    def get(self, key):
        self._start()
        started = self.env.now
        try:
            owner = self.owner(key)
            yield from self._route(owner, 5 + len(key.encode("utf-8")))
            value = self.nodes[owner].store.get(key)
            yield from self._transfer(owner, 0, 5 + (len(value.encode("utf-8")) if value is not None else 0))
            self.latencies["get"].append(self.env.now - started)
            return value
        finally:
            self.busy = False

    def crash(self, failed_nodes):
        """SimPy event that erases primary data and every hosted backup copy."""
        if self.busy or self.recovering:
            raise RuntimeError("Crashes are allowed only between completed operations")
        failed_nodes = set(failed_nodes)
        if (not failed_nodes or any(type(i) is not int or not 0 <= i < self.n for i in failed_nodes)
                or failed_nodes & self.failed or len(self.failed | failed_nodes) > self.k):
            raise ValueError("Crash set must contain up to k distinct live ring positions")
        self.busy = True
        try:
            for i in failed_nodes:
                self.nodes[i].erase()
            # The reliable simulator immediately notifies every survivor.
            self.failed.update(failed_nodes)
            yield self.env.timeout(0)
        finally:
            self.busy = False

    def _restore_primary(self, index, encoded_records):
        node = Node(self.node_ids[index])
        for position, encoded in enumerate(encoded_records):
            record = self.codec.decode(encoded)
            node.records.append(record)
            if record is None:
                heapq.heappush(node.free_indices, position)
            else:
                if self.owner(record.key) != index or record.key in node.store:
                    raise ValueError("Decoded key has wrong ownership or is duplicated")
                node.store[record.key] = record.value
                node.key_to_index[record.key] = position
        return node

    def recover(self):
        self._start(recovery=True)
        started = self.env.now
        self.recovering = True
        try:
            if not self.failed:
                raise ValueError("No failed nodes to recover")
            missing = sorted(self.failed)
            survivors = [i for i in range(self.n) if i not in self.failed]
            coordinator = survivors[0]
            self.last_coordinator = coordinator
            if self.scheme == "fusion":
                restored = yield from self._recover_fusion(missing, survivors, coordinator)
            else:
                restored = yield from self._recover_replicas(missing, survivors, coordinator)
            candidate = [restored.get(i, node) for i, node in enumerate(self.nodes)]
            yield from self._restore_backups(candidate, missing, coordinator)
            # Validate the entire candidate state before exposing recovered nodes.
            self._verify(candidate)
            for i in missing:
                self.nodes[i] = candidate[i]
            self.failed.clear()
            self.latencies["recover"].append(self.env.now - started)
        finally:
            self.recovering = False
            self.busy = False

    def _recover_fusion(self, missing, survivors, coordinator):
        known = {i: [self._encoded(self.nodes[i], j) for j in range(self.committed_range)]
                 for i in survivors}
        parity = {}
        transfers = [self._rpc(coordinator, i, 1, 8 + self.committed_range * self.codec.size)
                     for i in survivors]
        for stream, hosts in self.placement.items():
            source = next(host for host in hosts if host not in self.failed)
            # Read only surviving copies. No pre-crash primary state is retained.
            parity[stream] = dict(self.nodes[source].parity[stream])
            transfers.append(self._rpc(coordinator, source, 3,
                                       8 + self.committed_range * self.code.parity_size))
        yield from self._parallel(transfers)
        streams = tuple(range(len(missing)))
        decoder = self.code.decoder(missing, streams)
        decoded = {i: [] for i in missing}
        for j in range(self.committed_range):
            recovered = self.code.recover(
                {i: known[i][j] for i in survivors},
                {p: parity[p][j] for p in parity}, missing, streams, decoder
            )
            for i in missing:
                decoded[i].append(recovered[i])
        restored = {i: self._restore_primary(i, decoded[i]) for i in missing}
        yield from self._parallel([
            self._rpc(coordinator, i, 8 + self.committed_range * self.codec.size)
            for i in missing
        ])
        return restored

    def _recover_replicas(self, missing, survivors, coordinator):
        restored = {}
        transfers = []
        for i in missing:
            source = next(host for host in self.replica_hosts(i) if host in survivors)
            replica = self.nodes[source].replicas[i]
            encoded = [self.codec.empty] * self.committed_range
            for key, (index, value) in replica.items():
                encoded[index] = self.codec.encode(Record(key, value))
            restored[i] = self._restore_primary(i, encoded)
            size = self._replica_payload(replica)
            transfers.append(self._rpc(coordinator, source, 9, size))
        yield from self._parallel(transfers)
        yield from self._parallel([
            self._rpc(coordinator, i, self._replica_payload({
                key: (node.key_to_index[key], value) for key, value in node.store.items()
            })) for i, node in restored.items()
        ])
        return restored

    def _restore_backups(self, candidate, missing, coordinator):
        transfers = []
        if self.scheme == "fusion":
            lost_streams = {p for p, hosts in self.placement.items()
                            if any(i in missing for i in hosts)}
            rebuilt = {p: {} for p in lost_streams}
            for j in range(self.committed_range if lost_streams else 0):
                records = [self._encoded(node, j) for node in candidate]
                parity = self.code.encode(records)
                for p in lost_streams:
                    rebuilt[p][j] = parity[p]
            for i in missing:
                for p, hosts in self.placement.items():
                    if i in hosts:
                        candidate[i].parity[p] = {
                            j: bytes(bytearray(value)) for j, value in rebuilt[p].items()
                        }
                        transfers.append(self._rpc(coordinator, i,
                                                   10 + self.committed_range * self.code.parity_size))
        else:
            # A failed host can contain replicas of both failed and live owners.
            sources = {owner for owner in range(self.n)
                       if any(i in missing for i in self.replica_hosts(owner))}
            copies = {}
            for owner in sorted(sources):
                node = candidate[owner]
                replica = {key: (node.key_to_index[key], value)
                           for key, value in node.store.items()}
                copies[owner] = replica
                size = self._replica_payload(replica)
                if owner not in missing:
                    transfers.append(self._rpc(coordinator, owner, 9, size))
            yield from self._parallel(transfers)
            transfers = []
            for owner, replica in copies.items():
                size = self._replica_payload(replica)
                for host in self.replica_hosts(owner):
                    if host in missing:
                        candidate[host].replicas[owner] = dict(replica)
                        transfers.append(self._rpc(coordinator, host, size))
        yield from self._parallel(transfers)

    def _verify(self, nodes):
        all_keys = set()
        for owner, node in enumerate(nodes):
            expected_records = {record.key: (j, record.value)
                                for j, record in enumerate(node.records) if record is not None}
            if len(expected_records) != sum(record is not None for record in node.records):
                raise ValueError("Duplicate key in primary records")
            if (node.store != {key: value for key, (_, value) in expected_records.items()}
                    or node.key_to_index != {key: j for key, (j, _) in expected_records.items()}
                    or sorted(node.free_indices) != [j for j, r in enumerate(node.records) if r is None]
                    or len(node.records) > self.committed_range):
                raise ValueError("Primary index metadata is inconsistent")
            for key in node.store:
                if key in all_keys or self.owner(key) != owner:
                    raise ValueError("Duplicate key or wrong owner")
                all_keys.add(key)
        if self.scheme == "fusion":
            expected = {p: {} for p in range(self.k)}
            for j in range(self.committed_range):
                parity = self.code.encode([self._encoded(node, j) for node in nodes])
                for p in range(self.k):
                    expected[p][j] = parity[p]
            for host, node in enumerate(nodes):
                required = {p: expected[p] for p, hosts in self.placement.items() if host in hosts}
                if node.parity != required or node.replicas:
                    raise ValueError("Parity placement or encoded data is inconsistent")
        else:
            for host, node in enumerate(nodes):
                required = {
                    owner: {key: (nodes[owner].key_to_index[key], value)
                            for key, value in nodes[owner].store.items()}
                    for owner in range(self.n) if host in self.replica_hosts(owner)
                }
                if node.replicas != required or node.parity:
                    raise ValueError("Replica placement or data is inconsistent")

    def verify(self):
        if self.failed or self.busy:
            raise RuntimeError("Verification requires a healthy, idle system")
        self._verify(self.nodes)

    @staticmethod
    def _replica_payload(replica):
        return 8 + sum(16 + len(key.encode("utf-8")) + len(value.encode("utf-8"))
                       for key, (_, value) in replica.items())

    def storage_stats(self):
        primary, backup, primary_payload, backup_payload = 0, 0, 0, 0
        for node in self.nodes:
            seen = set()
            primary += sum(_deep_size(value, seen) for value in
                           (node.store, node.key_to_index, node.records, node.free_indices))
            backup += _deep_size(node.parity if self.scheme == "fusion" else node.replicas, set())
            primary_payload += sum(len(key.encode("utf-8")) + len(value.encode("utf-8"))
                                   for key, value in node.store.items())
            if self.scheme == "fusion":
                backup_payload += sum(len(record) for stream in node.parity.values()
                                      for record in stream.values())
            else:
                backup_payload += sum(self._replica_payload(replica) for replica in node.replicas.values())
        metadata = _deep_size((self.node_ids, self.placement, self.code.matrix,
                               self.committed_range, vars(self.codec), vars(self.code.field)), set())
        return {
            "primary_storage_bytes": primary, "backup_storage_bytes": backup,
            "global_metadata_bytes": metadata,
            "storage_overhead": backup / primary if primary else 0,
            "total_storage_bytes": primary + backup + metadata,
            "primary_payload_bytes": primary_payload, "backup_payload_bytes": backup_payload,
        }


def run(env, operation):
    return env.run(until=env.process(operation))


def benchmark(output, configurations=((4, 1), (4, 2), (5, 2), (8, 1)),
              records_per_node=(8, 24), value_sizes=(32, 128), seed=17,
              network_delay=2, bandwidth=100_000):
    """Return CSV rows. Workload keys are uniform hashes, not balanced by force."""
    rows = []
    for n, k in configurations:
        for count in records_per_node:
            for value_size in value_sizes:
                rng = random.Random(seed)
                workload = [(f"key-{i:06d}", "".join(rng.choices("abcdefgh01234567", k=value_size)))
                            for i in range(n * count)]
                for scheme in ("fusion", "replication"):
                    env = simpy.Environment()
                    simulation = Simulation(env, n, k, scheme, max_key_bytes=32,
                                            max_value_bytes=value_size,
                                            network_delay=network_delay, bandwidth=bandwidth)
                    started_cpu = process_time()
                    for key, value in workload:
                        run(env, simulation.put(key, value))
                    for key, value in workload[::5]:
                        run(env, simulation.put(key, value[::-1]))
                    for key, _ in workload:
                        run(env, simulation.get(key))
                    for key, _ in workload[::7]:
                        run(env, simulation.delete(key))
                    simulation.verify()
                    storage = simulation.storage_stats()
                    before_messages, before_bytes = simulation.messages, simulation.bytes_transferred
                    run(env, simulation.crash(range(k)))
                    recovery_cpu = process_time()
                    run(env, simulation.recover())
                    recovery_cpu = process_time() - recovery_cpu
                    simulation.verify()
                    rows.append({
                        "scheme": scheme, "n": n, "k": k, "records_per_node_target": count,
                        "value_bytes": value_size, "seed": seed,
                        "network_delay": network_delay, "bandwidth_bytes_per_unit": bandwidth,
                        "record_bytes": simulation.codec.size, "index_range": simulation.committed_range,
                        "parity_record_bytes": simulation.code.parity_size,
                        "field_bits": simulation.code.field.bits,
                        **storage,
                        "write_latency": mean(simulation.latencies["put"]),
                        "read_latency": mean(simulation.latencies["get"]),
                        "delete_latency": mean(simulation.latencies["delete"]),
                        "recovery_time": simulation.latencies["recover"][-1],
                        "message_count": simulation.messages,
                        "bytes_transferred": simulation.bytes_transferred,
                        "recovery_messages": simulation.messages - before_messages,
                        "recovery_bytes": simulation.bytes_transferred - before_bytes,
                        "actual_cpu_seconds": process_time() - started_cpu,
                        "recovery_cpu_seconds": recovery_cpu,
                    })
    output = Path(output)
    with output.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="/tmp/fusion_p2p_benchmark.csv")
    parser.add_argument("--configs", nargs="+", default=["4:1", "4:2", "5:2", "8:1"])
    parser.add_argument("--records", nargs="+", type=int, default=[8, 24])
    parser.add_argument("--value-bytes", nargs="+", type=int, default=[32, 128])
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--delay", type=float, default=2)
    parser.add_argument("--bandwidth", type=float, default=100_000)
    args = parser.parse_args()
    if any(count <= 0 for count in args.records) or any(size < 0 for size in args.value_bytes):
        parser.error("Record counts must be positive and value sizes nonnegative")
    try:
        configurations = [tuple(map(int, item.split(":"))) for item in args.configs]
        if any(len(pair) != 2 for pair in configurations):
            raise ValueError("Each configuration must be n:k")
        rows = benchmark(args.output, configurations, args.records, args.value_bytes,
                         args.seed, args.delay, args.bandwidth)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    for row in rows:
        print(f"{row['scheme']:11} n={row['n']} k={row['k']} "
              f"records={row['records_per_node_target']:3} value={row['value_bytes']:3} "
              f"backup={row['backup_storage_bytes']:7}B "
              f"write={row['write_latency']:.4f} recovery={row['recovery_time']:.4f}")
    print(f"Wrote {len(rows)} rows to {args.output}. Latencies use simulated time units.")


if __name__ == "__main__":
    main()
