# Static P2P fusion simulation

`simulation.py` implements a fixed SHA-1 ring, indexed primary maps, distributed
fused backups, successor replication, failure/recovery events, and benchmarks.
`coding.py` implements UTF-8 records and adaptive binary-field coding. `test_simulation.py`
contains the pytest suite.

## Run

From the repository root, use Python 3.10 or newer and install SimPy plus pytest.

```sh
python3 -m venv /tmp/fusion-p2p-venv
source /tmp/fusion-p2p-venv/bin/activate
pip install -r requirements.txt pytest
python -m pytest fusion_p2p/test_simulation.py -q
python -m fusion_p2p.simulation --output /tmp/fusion_p2p_benchmark.csv
```

The default benchmark runs both schemes for `(n, k)` equal to `(4, 1)`, `(4, 2)`,
`(5, 2)`, and `(8, 1)`, with 8 or 24 target records per node and values of 32 or
128 bytes. It writes 32 CSV rows. Configure smaller or larger runs with flags.

```sh
python -m fusion_p2p.simulation --configs 4:1 4:2 5:2 8:1 \
  --records 8 24 --value-bytes 32 128 --seed 17 \
  --delay 2 --bandwidth 100000 --output /tmp/fusion_p2p_benchmark.csv
```

## API

```python
import simpy
from fusion_p2p.simulation import Simulation, run

env = simpy.Environment()
system = Simulation(env, n=5, k=2, scheme="fusion")
run(env, system.put("hello", "世界"))
assert run(env, system.get("hello")) == "世界"
owner = system.owner("hello")
run(env, system.crash([owner, (owner + 1) % system.n]))
run(env, system.recover())
assert run(env, system.get("hello")) == "世界"
run(env, system.delete("hello"))
assert run(env, system.get("hello")) is None
system.verify()
print(system.storage_stats())
```

`run(env, operation)` runs a generator to completion. Inside a SimPy process,
use `yield env.process(system.put(...))` and similarly for other operations.

| Method | Behavior |
| --- | --- |
| `put(key, value)` | Insert or overwrite a string value. |
| `get(key)` | Return the string value or `None`. |
| `delete(key)` | Delete an entry. Missing keys are a no-op. |
| `owner(key)` | Immediately return the permanent owner's ring position. |
| `crash(positions)` | SimPy event erasing primary and hosted backup state. |
| `recover()` | SimPy event restoring all failed nodes and backup redundancy. |
| `verify()` | Immediately validate primary metadata, ownership, and all backups. |
| `storage_stats()` | Immediately measure the current storage representation. |

Nodes are addressed by positions `0..n-1` in clockwise order. Their permanent
160-bit IDs are sorted SHA-1 hashes of `node-0` through `node-(n-1)`. The owner
is the clockwise successor of the key hash, with wraparound. Keys are globally
unique because every operation uses that owner. Overwrites preserve the index.

Constructor options include `max_key_bytes=64`, `max_value_bytes=256`,
`network_delay=2`, and `bandwidth=100000`. Limits apply to UTF-8 bytes. Empty
strings are supported. Invalid record types raise `TypeError`; oversized records,
unsupported coding configurations, or invalid crash sets raise `ValueError`.
Overlapping operations and requests during an outage/recovery raise `RuntimeError`.

## Encoding and placement

A record has a one-byte occupancy flag, two four-byte big-endian lengths, UTF-8
key and value bytes, and zero padding to `9 + max_key_bytes + max_value_bytes`.
The defaults produce 329-byte records. All-zero bytes encode an empty slot.
Decoding validates occupancy, lengths, UTF-8, and padding.

Each primary has `store`, `key_to_index`, `records`, and a heap of `free_indices`.
New keys reuse the smallest free slot or append. Deletions leave holes. A
committed simulator-level `committed_range` preserves the largest allocated index
range even if the largest primary fails. Absent slots contribute zero records.

For parity stream `p` and primary position `i`, the coefficient is
`inverse(p XOR (k + i))` in GF(2^w). The field width is chosen automatically as
`max(8, ceil(log2(n+k)))`, keeping the two field-element sets disjoint for every
`1 <= k < n`. This is a Cauchy matrix, whose square submatrices are nonsingular.
GF(256) uses polynomial `0x11D`. Wider fields use a deterministically selected
irreducible binary polynomial, validated by polynomial GCD and repeated squaring.

Each original byte is one field symbol. Parity symbols occupy `ceil(w/8)` bytes,
so parity records widen when `n+k > 256`. Primary encoding stays the same.
Incremental updates, recovery, byte accounting, and simulated transfer delays
use the actual parity record width. CSV columns `field_bits` and
`parity_record_bytes` expose the chosen representation.

Stream `p` is stored on positions `(floor(p*n/k) + offset) % n` for offsets
`0..k`. Each stream therefore has `k+1` distinct copies, leaving at least one
copy after any `k` failures. Some nodes host no stream when the number of copies
is smaller than `n`; every node supports hosting streams. All copies reside on
the fixed ring nodes.

Each write sends the weighted old/new record delta for the changed index to
each parity host. Hosts update their local parity at that index. The driver
commits primary, all parity copies, and index-range metadata atomically after
communication completes, then acknowledges the write. Full parity recomputation
occurs during recovery and verification.

## Recovery and replication

Crashes occur between completed operations and immediately erase the failed
nodes' stores, index metadata, parity streams, and replicas. The simulator records
the failed positions and blocks requests. It immediately informs survivors through
the shared failure set. No additional crash is accepted during recovery.

The lowest surviving ring position coordinates recovery. Fusion reads surviving
primary records and one surviving copy of every parity stream. For each committed
index, it subtracts surviving contributions and solves an `r` by `r` field system
using the first `r` streams, where `r` is the number of failed nodes. It decodes
keys and values, rebuilds lookup/index dictionaries and free slots, recomputes lost
parity copies, and verifies the candidate state before resuming requests.

Recovered primary arrays extend through the committed range, making absent and
deleted slots reusable while preserving all occupied indices. Recovery uses
surviving state. Test oracles are kept only in tests.

With `scheme="replication"`, each primary has complete replicas on its next `k`
distinct ring successors. Replica entries retain `(index, value)` under each key.
Recovery reads surviving replicas, restores primaries, then fetches any other
surviving primary data needed to replace lost replicas. Both schemes restore all
redundancy before returning.

## Measurements and results

The network uses SimPy timeouts like the existing repository. Requests enter at
position 0 and walk successor links. Replies and backup/recovery RPCs use direct
point-to-point transfers. Each remote message costs `delay + bytes / bandwidth`.
Independent backup RPCs run in parallel with no shared-link contention. Local
transfers cost zero and do not increment network counters. Payload accounting
includes record/index fields and one-byte acknowledgments. The failure
notification is an immediate simulator event.

Simulated latency excludes coding CPU time. CSV fields include mean write, read,
and delete latency, recovery duration, total/recovery message counts, and
total/recovery bytes. Actual process CPU seconds are reported separately for the
whole workload and recovery, including consistency validation. These CPU timings
vary between runs; simulated metrics and workload generation are deterministic.

Storage bytes are measured recursively with `sys.getsizeof` for the actual
Python data structures. References shared within one primary or backup role are
counted once, and each simulated node/role is measured independently. Primary
storage includes maps, record objects, indices, and free slots. Backup storage
includes dictionaries, replica metadata, and all padded parity copies. Global
configuration metadata is reported separately. `storage_overhead` is backup bytes
divided by primary bytes; total bytes also include global metadata. These are
Python representation measurements and depend on interpreter/container allocation.
Raw UTF-8 primary payload and encoded backup payload sizes are also exported.

For equal padded arrays of `L` bytes per primary, ideal backup payload is about
`n*k*L` for replication and `k*(k+1)*L` for this fusion placement. The actual
baseline stores variable-length strings, and fusion uses padded records through
the longest primary array. Padding, imbalance, deletions, and metadata affect the
measured comparison.

The default benchmark was executed with seed 17. Representative results below use
24 target records per node, 128-byte values, delay 2, and bandwidth 100000.
Times are simulated units and backup sizes include Python metadata.

| n | k | Scheme | Backup bytes | Mean write | Mean read | Mean delete | Recovery | Messages | Bytes transferred |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4 | 2 | Fusion | 82,004 | 6.7445 | 2.8133 | 7.4306 | 12.2589 | 1,480 | 178,574 |
| 4 | 2 | Replication | 56,448 | 6.7443 | 2.8133 | 7.4291 | 12.1590 | 852 | 84,371 |
| 8 | 1 | Fusion | 21,568 | 13.4620 | 9.4601 | 14.2169 | 12.2031 | 3,121 | 307,383 |
| 8 | 1 | Replication | 57,788 | 13.4618 | 9.4601 | 14.2154 | 12.1112 | 2,667 | 214,592 |

The `(8,1)` case used 62.7% fewer backup bytes with fusion. The `(4,2)` case used
45.3% more. Fusion had higher recovery CPU cost and transferred more bytes in
these runs. Simulated latency is dominated by parallel message delays and excludes
that CPU cost.

The fusion pytest suite passes 275 tests. It covers every valid `k` and every
failure subset of sizes 1 through `k` for rings of 2 through 5 nodes, for both
schemes, with writes and a second failure episode after recovery. A larger-field
coding test recovers 128 missing primaries out of 129. A 256-node simulation
checks wider parity records, recovery, and storage accounting. Other tests check malformed records,
GF arithmetic against polynomial multiplication, Cauchy submatrices, incremental
updates, slot reuse, empty/uneven maps, request blocking, and reproducible CSV metrics.

To benchmark all valid `k` on an eight-node ring, run:

```sh
python -m chord_replication.simulation --configs 8:1 8:2 8:3 8:4 8:5 8:6 8:7 \
  --records 2 --value-bytes 8 --output /tmp/all_k_comparison.csv
```

As `k` grows, this placement stores `k*(k+1)` parity-stream copies and sends
updates to those copies. Recovery factors an `r` by `r` matrix once, with cubic
cost in the number `r` of failed primaries, then reuses that inverse across indices.
Large rings and failure sets therefore require more CPU and memory.
