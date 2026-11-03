"""From the project root: python -B -m chord.src.demo_chord --nodes 10 20 50.

Checks routing and ring membership, not key/value storage or fusion backups.
The sorted list is used only as an independent test oracle; Chord still routes
through its own fingers and the simulated network.
"""

import argparse
from bisect import bisect_left
import random

import simpy

from chord.src.network import Network
from chord.src.node import Node


def wait(env, event):
    """Bound the wait so missing replies produce a clear failure."""
    env.run(until=event | env.timeout(1_000_000))
    if not event.triggered:
        raise RuntimeError("Operation timed out in simulated time")
    return event.value


def expected_owner(ids, key_id):
    return ids[bisect_left(ids, key_id) % len(ids)]


def check_ring(nodes):
    """Check every pointer and finger, including after intermediate joins."""
    ids = sorted(node.chord.id for node in nodes)
    checks = 0
    for node in nodes:
        chord = node.chord
        index = ids.index(chord.id)
        expected_predecessor = ids[index - 1]
        expected_successor = ids[(index + 1) % len(ids)]
        if chord.predecessor != expected_predecessor:
            raise RuntimeError(f"Wrong predecessor for node {chord.id}")
        if chord.successor != expected_successor:
            raise RuntimeError(f"Wrong successor for node {chord.id}")
        checks += 2
        for i, actual in enumerate(chord.fingerNode):
            start = (chord.id + 2**i) % (2**chord.m)
            expected = expected_owner(ids, start)
            if chord.fingerStart[i] != start or actual != expected:
                raise RuntimeError(
                    f"Node {chord.id}, finger {i}: got {actual}, expected {expected}"
                )
            checks += 1
        if node.pending:
            raise RuntimeError(f"Node {chord.id} still has unanswered requests")
    return checks


def run_demo(count, lookup_count, seed, full_ids):
    env = simpy.Environment()
    network = Network(env)
    rng = random.Random(seed)
    nodes = []
    names = {}
    width = 40 if full_ids else 10

    def hex_id(node_id):
        digits = f"{node_id:040x}"
        return digits if full_ids else digits[:10]

    def name(node_id):
        return names[node_id]

    print(f"\n{'=' * 76}\nCHORD DEMO: {count} nodes, 160-bit IDs, seed={seed}")
    print("IDs are hexadecimal" + ("." if full_ids else " (first 10 digits shown)."))
    print("\nJoining one node at a time; validating the entire ring after each join:")
    intermediate_checks = 0
    for i in range(count):
        bootstrap = rng.choice(nodes) if nodes else None
        started = env.now
        node = Node(
            env, network, f"10.0.0.{i + 1}",
            bootstrap.chord.id if bootstrap else None,
        )
        wait(env, node.ready)
        nodes.append(node)
        names[node.chord.id] = f"N{i:02d}"
        intermediate_checks += check_ring(nodes)
        via = name(bootstrap.chord.id) if bootstrap else "new ring"
        print(
            f"  {name(node.chord.id)} id={hex_id(node.chord.id)} "
            f"bootstrap={via:8} join_time={env.now - started:6.0f}  PASS"
        )

    ids = sorted(names)
    print("\nFinal ring in clockwise order:")
    print(f"  {'Node':5} {'ID':{width}} {'Predecessor':12} {'Successor':10} Check")
    for node in sorted(nodes, key=lambda item: item.chord.id):
        chord = node.chord
        print(
            f"  {name(chord.id):5} {hex_id(chord.id):{width}} "
            f"{name(chord.predecessor):12} {name(chord.successor):10} PASS"
        )
    print("  The last row links back to the first. Each node owns (predecessor, ID].")

    # Group only consecutive fingers with the same destination for readability.
    chord = nodes[0].chord
    print(f"\nFinger table for {name(chord.id)} (consecutive equal destinations grouped):")
    print(f"  {'Indexes':10} {'First start':{width}} {'Last start':{width}} Destination")
    begin = 0
    for end in range(chord.m):
        if end + 1 < chord.m and chord.fingerNode[end + 1] == chord.fingerNode[begin]:
            continue
        indexes = str(begin) if begin == end else f"{begin}-{end}"
        print(
            f"  {indexes:10} {hex_id(chord.fingerStart[begin]):{width}} "
            f"{hex_id(chord.fingerStart[end]):{width}} {name(chord.fingerNode[begin])}"
        )
        begin = end + 1
    print(f"  All {count * chord.m} fingers across all nodes were checked individually.")

    # Look up each key from EVERY starting node, so correctness cannot depend
    # on the initiator. Also check exact node IDs and both ring boundaries.
    samples = [
        (f"key-{i:03d}", chord._hash_string(f"key-{i:03d}"))
        for i in range(lookup_count)
    ]
    samples += [("zero", 0), ("max ID", 2**chord.m - 1)]
    queries = samples + [(f"exact {name(node_id)}", node_id) for node_id in ids]
    print("\nLookups (one displayed initiator per key; every initiator is tested):")
    print(f"  {'Key':10} {'Hash/ID':{width}} {'From':5} {'Expected':9} {'Actual':7} Time  Check")
    lookup_checks = 0
    for query_index, (key, key_id) in enumerate(queries):
        expected = expected_owner(ids, key_id)
        displayed_source = query_index % count
        for source_index, source in enumerate(nodes):
            started = env.now
            process = env.process(source.chord._find_successor(source.chord.id, key_id))
            actual = wait(env, process)
            if actual != expected:
                raise RuntimeError(
                    f"{key} from {name(source.chord.id)}: "
                    f"got {actual}, expected {expected}"
                )
            lookup_checks += 1
            if query_index < len(samples) and source_index == displayed_source:
                print(
                    f"  {key:10} {hex_id(key_id):{width}} {name(source.chord.id):5} "
                    f"{name(expected):9} {name(actual):7} {env.now - started:4.0f}  PASS"
                )

    check_ring(nodes)
    print(f"\nPASS: {intermediate_checks:,} pointer/finger checks during joins;")
    print(f"      {lookup_checks:,} lookups including every exact node ID;")
    print(f"      zero unanswered requests. Final simulation time: {env.now:,.0f}.")
    print("Times are simulated units, not wall-clock seconds.")


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", nargs="+", type=positive_int, default=[10, 20, 50])
    parser.add_argument("--lookups", type=positive_int, default=10,
                        help="number of sample string keys (default: 10)")
    parser.add_argument("--seed", type=int, default=42,
                        help="seed for reproducible bootstrap selection")
    parser.add_argument("--full-ids", action="store_true",
                        help="print all 40 hexadecimal digits of each ID")
    args = parser.parse_args()
    try:
        for count in args.nodes:
            run_demo(count, args.lookups, args.seed, args.full_ids)
    except (RuntimeError, ValueError) as error:
        parser.exit(1, f"\nFAIL: {error}\n")


if __name__ == "__main__":
    main()
