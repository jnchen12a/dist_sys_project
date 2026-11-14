"""Run with python -m pytest fusion_p2p/test_simulation.py -q."""

from bisect import bisect_left
import csv
import hashlib
from itertools import combinations
import random
import struct

import pytest
import simpy

from fusion_p2p.coding import (
    CauchyCode, Record, RecordCodec, inverse, invert_matrix, multiply,
)
from fusion_p2p.simulation import Simulation, benchmark, run


CONFIGURATIONS = [(n, k) for n in range(2, 6) for k in range(1, n)]
FAILURES = [(n, k, subset) for n, k in CONFIGURATIONS
            for count in range(1, k + 1) for subset in combinations(range(n), count)]


def keys_for_owner(simulation, owner, count):
    keys = []
    candidate = 0
    while len(keys) < count:
        key = f"node{owner}-key{candidate}"
        if simulation.owner(key) == owner:
            keys.append(key)
        candidate += 1
    return keys


def populate(simulation):
    expected, deleted = {}, set()
    for owner in range(simulation.n):
        keys = keys_for_owner(simulation, owner, owner + 3)
        for index, key in enumerate(keys):
            value = f"value-{owner}-{index}-é"
            run(simulation.env, simulation.put(key, value))
            expected[key] = value
        run(simulation.env, simulation.put(keys[0], "overwritten-世界"))
        expected[keys[0]] = "overwritten-世界"
        run(simulation.env, simulation.delete(keys[1]))
        del expected[keys[1]]
        deleted.add(keys[1])
    simulation.verify()
    return expected, deleted


@pytest.mark.parametrize("scheme", ["fusion", "replication"])
def test_key_value_operations_and_ownership(scheme):
    env = simpy.Environment()
    simulation = Simulation(env, 5, 2, scheme, max_key_bytes=16, max_value_bytes=16)
    for key, value in [("", ""), ("é🔑", "世界🌎"), ("ordinary", "value")]:
        digest = int.from_bytes(hashlib.sha1(key.encode("utf-8")).digest(), "big")
        owner = bisect_left(simulation.node_ids, digest) % simulation.n
        assert simulation.owner(key) == owner
        run(env, simulation.put(key, value))
        assert run(env, simulation.get(key)) == value
        index = simulation.nodes[owner].key_to_index[key]
        run(env, simulation.put(key, "replacement"))
        assert simulation.nodes[owner].key_to_index[key] == index
        assert sum(key in node.store for node in simulation.nodes) == 1
        assert run(env, simulation.get(key)) == "replacement"
        run(env, simulation.delete(key))
        assert run(env, simulation.get(key)) is None
        run(env, simulation.delete(key))
    simulation.verify()


def test_codec_and_malformed_records():
    codec = RecordCodec(4, 4)
    for record in [None, Record("", ""), Record("éé", "🌎"), Record("abcd", "1234")]:
        encoded = codec.encode(record)
        assert len(encoded) == codec.size
        assert codec.decode(encoded) == record
    for record in [Record("ééé", "x"), Record("x", "🌎x")]:
        with pytest.raises(ValueError):
            codec.encode(record)
    with pytest.raises(TypeError):
        codec.encode(Record("x", 1))
    malformed = [b"short", bytes([2]) + bytes(codec.size - 1),
                 struct.pack(">BII", 1, 5, 0) + bytes(8),
                 struct.pack(">BII", 1, 1, 0) + b"\xff" + bytes(7),
                 bytes(codec.size - 1) + b"\x01",
                 codec.encode(Record("x", "y"))[:-1] + b"\x01"]
    for encoded in malformed:
        with pytest.raises(ValueError):
            codec.decode(encoded)


@pytest.mark.parametrize("scheme", ["fusion", "replication"])
def test_limits_are_utf8_bytes_and_rejections_are_atomic(scheme):
    env = simpy.Environment()
    simulation = Simulation(env, 4, 1, scheme, max_key_bytes=4, max_value_bytes=4)
    run(env, simulation.put("éé", "🌎"))
    for key, value in [("ééx", "v"), ("k", "🌎x"), (3, "v"), ("k", 3)]:
        with pytest.raises((ValueError, TypeError)):
            run(env, simulation.put(key, value))
        assert not simulation.busy
    assert run(env, simulation.get("éé")) == "🌎"
    assert sum(len(node.store) for node in simulation.nodes) == 1
    simulation.verify()


def test_finite_field_against_polynomial_multiplication():
    def reference(a, b):
        result = 0
        while b:
            if b & 1:
                result ^= a
            a <<= 1
            if a & 256:
                a ^= 0x11D
            b >>= 1
        return result

    for a in range(256):
        for b in range(256):
            assert multiply(a, b) == reference(a, b)
        if a:
            assert multiply(a, inverse(a)) == 1


@pytest.mark.parametrize("n,k", CONFIGURATIONS + [(8, 3)])
def test_cauchy_mds_all_missing_subsets_and_equation_choices(n, k):
    rng = random.Random(101)
    code = CauchyCode(n, k, 13)
    records = [bytes(rng.randrange(256) for _ in range(13)) for _ in range(n)]
    parity = code.encode(records)
    for count in range(1, k + 1):
        for missing in combinations(range(n), count):
            surviving = {i: record for i, record in enumerate(records) if i not in missing}
            for streams in combinations(range(k), count):
                decoded = code.recover(surviving, {p: parity[p] for p in streams}, missing)
                assert decoded == {i: records[i] for i in missing}


@pytest.mark.parametrize("n,k", [(1, 1), (4, 0), (4, 4), (3, 1.0)])
def test_unsupported_codes_rejected(n, k):
    with pytest.raises(ValueError):
        CauchyCode(n, k, 16)


def test_supported_field_boundary_and_invalid_matrix():
    code = CauchyCode(255, 1, 1)
    assert len(code.matrix[0]) == 255
    with pytest.raises(ValueError):
        invert_matrix([[1, 1], [1, 1]])


def test_larger_field_and_recovery_with_one_remaining_primary():
    code = CauchyCode(129, 128, 3)
    assert code.field.bits == 9
    assert code.parity_size == 6
    for value in range(1, 1 << code.field.bits):
        assert code.field.multiply(value, code.field.inverse(value)) == 1
    records = [bytes((i, (i * 17) % 256, 255 - i)) for i in range(129)]
    parity = code.encode(records)
    missing = tuple(range(1, 129))
    recovered = code.recover({0: records[0]}, dict(enumerate(parity)), missing)
    assert recovered == {i: records[i] for i in missing}
    updated = code.update(parity[0], 0, 128, records[128], b"new")
    records[128] = b"new"
    assert updated == code.encode(records)[0]


def test_wide_parity_in_simulation_and_storage_accounting():
    env = simpy.Environment()
    simulation = Simulation(env, 256, 1, max_key_bytes=32, max_value_bytes=8)
    assert simulation.code.field.bits == 9
    key = keys_for_owner(simulation, 0, 1)[0]
    run(env, simulation.put(key, "wide"))
    # Owner 0 hosts one copy locally and sends one delta plus an ACK remotely.
    assert simulation.messages == 2
    assert simulation.bytes_transferred == 11 + simulation.code.parity_size
    assert simulation.storage_stats()["backup_payload_bytes"] == 2 * simulation.code.parity_size
    run(env, simulation.crash([0]))
    run(env, simulation.recover())
    assert run(env, simulation.get(key)) == "wide"
    run(env, simulation.put(key, "updated"))
    run(env, simulation.crash([1]))
    run(env, simulation.recover())
    simulation.verify()


def test_incremental_update_only_touches_changed_index(monkeypatch):
    env = simpy.Environment()
    simulation = Simulation(env, 5, 2)
    keys = keys_for_owner(simulation, 2, 3)
    for key in keys:
        run(env, simulation.put(key, "initial"))
    before = {p: dict(simulation.nodes[hosts[0]].parity[p])
              for p, hosts in simulation.placement.items()}
    changed_index = simulation.nodes[2].key_to_index[keys[1]]

    def forbidden(*args):
        raise AssertionError("A write must not rebuild complete parity")

    with monkeypatch.context() as patch:
        patch.setattr(simulation.code, "encode", forbidden)
        run(env, simulation.put(keys[1], "new value"))
    for p, hosts in simulation.placement.items():
        for index, encoded in before[p].items():
            after = simulation.nodes[hosts[0]].parity[p][index]
            if index != changed_index:
                assert after == encoded
            else:
                assert after != encoded
    simulation.verify()
    run(env, simulation.delete(keys[1]))
    replacement = keys_for_owner(simulation, 2, 4)[-1]
    run(env, simulation.put(replacement, "reused"))
    assert simulation.nodes[2].key_to_index[replacement] == changed_index
    assert len(simulation.nodes[2].records) == 3
    simulation.verify()


@pytest.mark.parametrize("scheme", ["fusion", "replication"])
@pytest.mark.parametrize("n,k,failed", FAILURES)
def test_exhaustive_failure_recovery(n, k, failed, scheme):
    env = simpy.Environment()
    simulation = Simulation(env, n, k, scheme)
    expected, deleted = populate(simulation)
    owners = {key: simulation.owner(key) for key in expected}
    indexes = {key: simulation.nodes[owner].key_to_index[key] for key, owner in owners.items()}
    committed_range = simulation.committed_range
    run(env, simulation.crash(failed))
    for i in failed:
        node = simulation.nodes[i]
        assert not node.alive
        assert not any((node.store, node.key_to_index, node.records, node.free_indices,
                        node.parity, node.replicas))
    with pytest.raises(RuntimeError):
        run(env, simulation.get(next(iter(expected))))
    run(env, simulation.recover())
    assert simulation.last_coordinator == min(set(range(n)) - set(failed))
    assert simulation.committed_range == committed_range
    simulation.verify()
    assert {key: value for node in simulation.nodes for key, value in node.store.items()} == expected
    for key, value in expected.items():
        assert run(env, simulation.get(key)) == value
        owner = simulation.owner(key)
        assert owner == owners[key]
        assert simulation.nodes[owner].key_to_index[key] == indexes[key]
    for key in deleted:
        assert run(env, simulation.get(key)) is None
    # Write to a recovered owner, then lose a different set of nodes.
    new_key = keys_for_owner(simulation, failed[0], n + 10)[-1]
    run(env, simulation.put(new_key, "after recovery"))
    expected[new_key] = "after recovery"
    second_failure = tuple((i + 1) % n for i in failed)
    run(env, simulation.crash(second_failure))
    run(env, simulation.recover())
    simulation.verify()
    assert {key: value for node in simulation.nodes for key, value in node.store.items()} == expected


@pytest.mark.parametrize("scheme", ["fusion", "replication"])
@pytest.mark.parametrize("n,k", CONFIGURATIONS)
def test_empty_and_uneven_maps(n, k, scheme):
    env = simpy.Environment()
    simulation = Simulation(env, n, k, scheme)
    for failed in combinations(range(n), k):
        run(env, simulation.crash(failed))
        run(env, simulation.recover())
        simulation.verify()
        assert all(not node.store for node in simulation.nodes)
    # Only the failed owner contains records. Its index range must survive.
    keys = keys_for_owner(simulation, n - 1, 5)
    for key in keys:
        run(env, simulation.put(key, "only on largest owner"))
    run(env, simulation.delete(keys[2]))
    run(env, simulation.crash([n - 1]))
    run(env, simulation.recover())
    assert simulation.nodes[n - 1].store == {key: "only on largest owner" for key in keys if key != keys[2]}
    assert 2 in simulation.nodes[n - 1].free_indices
    simulation.verify()


def test_placement_and_failures_during_operations_are_rejected():
    env = simpy.Environment()
    simulation = Simulation(env, 5, 2)
    for stream, hosts in simulation.placement.items():
        assert len(hosts) == len(set(hosts)) == simulation.k + 1
        for failed in combinations(range(simulation.n), simulation.k):
            assert set(hosts) - set(failed)
    key = keys_for_owner(simulation, 3, 1)[0]
    write = env.process(simulation.put(key, "value"))

    def reject_during_write():
        yield env.timeout(0.1)
        with pytest.raises(RuntimeError):
            yield env.process(simulation.crash([0]))

    reject = env.process(reject_during_write())
    env.run(until=env.all_of([write, reject]))
    run(env, simulation.crash([0, 1]))
    recovery = env.process(simulation.recover())

    def reject_during_recovery():
        yield env.timeout(0.1)
        with pytest.raises(RuntimeError):
            yield env.process(simulation.put("blocked", "value"))
        with pytest.raises(RuntimeError):
            yield env.process(simulation.crash([2]))

    reject = env.process(reject_during_recovery())
    env.run(until=env.all_of([recovery, reject]))
    simulation.verify()


def test_benchmark_csv_and_reproducible_simulated_metrics(tmp_path):
    rows = benchmark(tmp_path / "first.csv", [(4, 1), (5, 2)], [2], [8], seed=3)
    repeated = benchmark(tmp_path / "second.csv", [(4, 1), (5, 2)], [2], [8], seed=3)
    assert len(rows) == 4
    for left, right in zip(rows, repeated):
        for field in ("write_latency", "read_latency", "delete_latency", "recovery_time",
                      "message_count", "bytes_transferred", "backup_payload_bytes"):
            assert left[field] == right[field]
        assert left["primary_storage_bytes"] > 0
        assert left["backup_storage_bytes"] > 0
        assert left["recovery_messages"] > 0
    with (tmp_path / "first.csv").open(newline="") as file:
        assert len(list(csv.DictReader(file))) == 4
