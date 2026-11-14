# Chord successor replication benchmark

This static Chord implementation stores each primary key on its SHA-1 successor
and keeps `k` additional complete copies on the next `k` ring successors. Each
node stores a primary `dict[str, str]` and replica dictionaries keyed by owner.

The implementation reuses hashing, successor-hop routing, SimPy communication,
read handling, and crash events from `fusion_p2p.simulation`. It implements string
validation, writes, replica restoration, and storage measurements in this folder.
Replication stores ordinary key/value maps. It has no record indices, padded
records, parity streams, or finite-field configuration limits.

## Run

From the repository root, with SimPy and pytest installed:

```sh
pip install -r requirements.txt pytest
python -m pytest fusion_p2p/test_simulation.py chord_replication/test_replication.py -q
python -m chord_replication.simulation --output /tmp/chord_comparison.csv
```

The default benchmark writes paired fusion/Chord replication rows for `(4,1)`,
`(4,2)`, `(5,2)`, and `(8,1)`, with 8 or 24 target records per node and 32-byte or
128-byte values. The workload inserts, overwrites, reads, deletes, crashes the
first `k` ring positions, and restores all lost primaries and replicas.

```sh
python -m chord_replication.simulation --configs 4:1 5:2 8:1 \
  --records 8 24 --value-bytes 32 128 --seed 17 \
  --delay 2 --bandwidth 100000 --output /tmp/chord_comparison.csv
python -m chord_replication.simulation --replication-only \
  --output /tmp/chord_replication.csv
```

## API

```python
import simpy
from chord_replication.simulation import Simulation, run

env = simpy.Environment()
system = Simulation(env, n=5, k=2)
run(env, system.put("hello", "世界"))
assert run(env, system.get("hello")) == "世界"
owner = system.owner("hello")
assert system.replica_hosts(owner) == ((owner + 1) % 5, (owner + 2) % 5)
run(env, system.crash([owner, (owner + 1) % 5]))
run(env, system.recover())
assert run(env, system.get("hello")) == "世界"
run(env, system.delete("hello"))
assert run(env, system.get("hello")) is None
system.verify()
```

Operations are SimPy generators. Inside another process, yield
`env.process(system.put(...))`, and likewise for reads, deletes, crashes, and
recovery. `owner`, `replica_hosts`, `verify`, and `storage_stats` return immediately.
Node arguments are clockwise ring positions `0..n-1`.

Defaults are `n=4`, `k=1`, `max_key_bytes=64`, `max_value_bytes=256`,
`network_delay=2`, and `bandwidth=100000`. Both schemes support every `1 <= k < n`.
Fusion automatically widens its coding field when `n+k > 256`. Limits apply to
UTF-8 bytes. Empty strings work. `put` overwrites existing keys, `get` returns
`None` for missing keys, and deleting a missing key is a no-op.

## Failures and recovery

The ring has permanent membership and ownership. Crashes occur between completed
operations and erase both primary and replica maps on affected nodes. At most
`k` nodes may be down. The simulator immediately records the failures and blocks
requests until recovery completes. Overlapping operations or crashes during an
operation/recovery raise `RuntimeError`.

The lowest surviving ring position coordinates recovery. Every primary has
`k+1` copies including its original, so at least one copy survives any `k` node
failures. Recovery fetches a surviving replica for each missing owner, transfers
the recovered map to its returning node, and restores every lost hosted replica.
The coordinator fetches surviving owners' maps when needed for rebuilding copies.
It validates ownership and replica consistency before exposing the restored state.

Writes replicate to all `k` successors and commit all copies atomically before
acknowledging completion. Deletes remove the key from all copies. State recovered
from surviving replicas is the only source used to restore lost primaries.

## Comparison and measurements

The new command runs fusion and standard replication with identical key/value
workloads, SHA-1 ring IDs, ingress position 0, failure sets, network delay, and
per-message bandwidth. Requests walk successor links. Replies and replication
RPCs use direct transfers. Replication updates the `k` copies in parallel.

CSV output uses the fusion benchmark's columns. It includes primary/backup bytes,
storage overhead, mean operation latencies, recovery duration, total/recovery
messages and bytes, and separately measured process CPU seconds. Replication's
`record_bytes` and `index_range` columns are zero because those fields do not
apply to its map representation. Its `parity_record_bytes` and `field_bits`
columns are also zero. Snapshots contain an eight-byte entry count
and four-byte lengths for each UTF-8 key/value.

Storage counts use `sys.getsizeof`, include container metadata, and count each
node/role independently. Raw payload counters are also included. These memory
measurements depend on Python and container allocation. Simulated latency excludes
CPU coding time and shared-link contention. Routing uses successor hops in both
schemes, preserving comparable lookup costs in this static experiment.

All 119 replication tests pass, including every valid `k` and every failure
subset of sizes 1 through `k` on rings of 2 through 5 nodes, recovery followed by another write and
failure episode, UTF-8 validation, empty maps, and matching fusion read costs.

The 32-row paired benchmark was executed with seed 17. Selected results use 24
target records per node, 128-byte values, delay 2, and bandwidth 100000.

| n | k | Scheme | Backup bytes | Mean write | Recovery |
| --- | --- | --- | --- | --- | --- |
| 4 | 2 | Fusion | 82,004 | 6.7445 | 12.2589 |
| 4 | 2 | Chord replication | 42,592 | 6.7442 | 12.1507 |
| 8 | 1 | Fusion | 21,568 | 13.4620 | 12.2031 |
| 8 | 1 | Chord replication | 42,896 | 13.4617 | 12.1055 |

Use this folder's paired benchmark for comparisons against conventional map
replication. The earlier `fusion_p2p` replication mode also stores per-record
indices, which increases its replica storage and communication costs.

For example, benchmark all supported replication factors on an eight-node ring:

```sh
python -m chord_replication.simulation --configs 8:1 8:2 8:3 8:4 8:5 8:6 8:7 \
  --records 2 --value-bytes 8 --output /tmp/all_k_comparison.csv
```

At `k=n-1`, every node holds a complete copy of every other node's primary map,
and recovery works with one surviving node. Replication storage and update
messages grow linearly with `k` for a fixed primary workload.
