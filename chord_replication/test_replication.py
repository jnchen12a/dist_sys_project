import csv
from itertools import combinations

import pytest
import simpy

from chord_replication.simulation import Simulation, benchmark, run
from fusion_p2p.simulation import Simulation as FusionSimulation


FAILURES = [(n, k, subset) for n in range(2, 6) for k in range(1, n)
            for size in range(1, k + 1) for subset in combinations(range(n), size)]


def owner_keys(system, owner, count):
    result = []
    candidate = 0
    while len(result) < count:
        key = f"owner-{owner}-key-{candidate}"
        if system.owner(key) == owner:
            result.append(key)
        candidate += 1
    return result


def test_operations_utf8_limits_and_replica_placement():
    env = simpy.Environment()
    system = Simulation(env, n=4, k=2, max_key_bytes=8, max_value_bytes=8)
    for key, value in [("", ""), ("é🔑", "世界")]:
        run(env, system.put(key, value))
        owner = system.owner(key)
        assert run(env, system.get(key)) == value
        for host, node in enumerate(system.nodes):
            assert (key in node.store) == (host == owner)
            if host in system.replica_hosts(owner):
                assert node.replicas[owner][key] == value
            else:
                assert owner not in node.replicas
        run(env, system.put(key, "new"))
        assert run(env, system.get(key)) == "new"
        system.verify()
        run(env, system.delete(key))
        assert run(env, system.get(key)) is None
        run(env, system.delete(key))
        system.verify()
    run(env, system.put("éééé", "🌎🌎"))
    for key, value in [("ééééx", "v"), ("key", "🌎🌎x"), (1, "v"), ("key", 1)]:
        with pytest.raises((TypeError, ValueError)):
            run(env, system.put(key, value))
        assert not system.busy
    assert run(env, system.get("éééé")) == "🌎🌎"
    system.verify()


@pytest.mark.parametrize("n,k,failed", FAILURES)
def test_all_failure_subsets_and_second_episode(n, k, failed):
    env = simpy.Environment()
    system = Simulation(env, n, k)
    expected = {}
    deleted = set()
    for owner in range(n):
        keys = owner_keys(system, owner, owner + 3)
        for key in keys:
            run(env, system.put(key, "value-世界"))
            expected[key] = "value-世界"
        run(env, system.put(keys[0], "updated"))
        expected[keys[0]] = "updated"
        run(env, system.delete(keys[1]))
        del expected[keys[1]]
        deleted.add(keys[1])
    owners = {key: system.owner(key) for key in expected}
    system.verify()
    run(env, system.crash(failed))
    for i in failed:
        assert not system.nodes[i].alive
        assert not system.nodes[i].store
        assert not system.nodes[i].replicas
    with pytest.raises(RuntimeError):
        run(env, system.get(next(iter(expected))))
    run(env, system.recover())
    system.verify()
    assert system.last_coordinator == min(set(range(n)) - set(failed))
    assert {key: value for node in system.nodes for key, value in node.store.items()} == expected
    for key, value in expected.items():
        assert system.owner(key) == owners[key]
        assert run(env, system.get(key)) == value
    for key in deleted:
        assert run(env, system.get(key)) is None
    key = owner_keys(system, failed[0], n + 8)[-1]
    run(env, system.put(key, "after recovery"))
    expected[key] = "after recovery"
    run(env, system.crash([(i + 1) % n for i in failed]))
    run(env, system.recover())
    system.verify()
    assert {key: value for node in system.nodes for key, value in node.store.items()} == expected


def test_empty_maps_and_no_fusion_capacity_or_metadata():
    env = simpy.Environment()
    system = Simulation(env, 4, 2)
    for failed in combinations(range(4), 2):
        run(env, system.crash(failed))
        run(env, system.recover())
        system.verify()
    assert not hasattr(system, "code")
    assert not hasattr(system, "codec")
    assert not hasattr(system, "committed_range")
    assert set(vars(system.nodes[0])) == {"node_id", "alive", "store", "replicas"}
    # Replication supports large rings without constructing a coding field.
    Simulation(simpy.Environment(), 256, 1)


def test_same_ownership_and_read_network_cost_as_fusion():
    replication = Simulation(simpy.Environment(), 5, 2)
    fusion = FusionSimulation(simpy.Environment(), 5, 2)
    assert replication.node_ids == fusion.node_ids
    for key in ["", "é", "key-a", "key-b"]:
        assert replication.owner(key) == fusion.owner(key)
        run(replication.env, replication.put(key, "value"))
        run(fusion.env, fusion.put(key, "value"))
        before = [(system.messages, system.bytes_transferred) for system in (replication, fusion)]
        for system in (replication, fusion):
            assert run(system.env, system.get(key)) == "value"
        assert replication.latencies["get"][-1] == pytest.approx(fusion.latencies["get"][-1])
        assert (replication.messages - before[0][0], replication.bytes_transferred - before[0][1]) == (
            fusion.messages - before[1][0], fusion.bytes_transferred - before[1][1])


def test_recovery_blocks_writes_and_additional_crashes():
    env = simpy.Environment()
    system = Simulation(env, 4, 2)
    run(env, system.put("key", "value"))
    run(env, system.crash([0, 1]))
    recovery = env.process(system.recover())

    def blocked_requests():
        yield env.timeout(0.1)
        with pytest.raises(RuntimeError):
            yield env.process(system.put("blocked", "value"))
        with pytest.raises(RuntimeError):
            yield env.process(system.crash([2]))

    env.run(until=env.all_of([recovery, env.process(blocked_requests())]))
    system.verify()


def test_comparison_benchmark(tmp_path):
    output = tmp_path / "comparison.csv"
    rows = benchmark(output, [(4, 1), (5, 2)], [2], [8])
    assert len(rows) == 4
    for fusion, replication in zip(rows[::2], rows[1::2]):
        assert fusion["scheme"] == "fusion"
        assert replication["scheme"] == "chord_replication"
        assert fusion["primary_payload_bytes"] == replication["primary_payload_bytes"]
        assert fusion["read_latency"] == pytest.approx(replication["read_latency"])
        assert replication["record_bytes"] == replication["index_range"] == 0
        assert replication["recovery_messages"] > 0
    with output.open(newline="") as file:
        assert len(list(csv.DictReader(file))) == 4
    repeated = benchmark(tmp_path / "again.csv", [(4, 1), (5, 2)], [2], [8])
    for first, second in zip(rows, repeated):
        for metric in ("write_latency", "read_latency", "delete_latency", "recovery_time",
                       "message_count", "bytes_transferred"):
            assert first[metric] == second[metric]
