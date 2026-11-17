# Chord-Based Fusion Fault Tolerance

Implementation and measurements verified on **November 16, 2026**.

## 1. System overview

The system simulates a static Chord-like key-value store protected by distributed
fused backups. Each primary stores string keys and string values. Several
primaries contribute records to shared coded parity streams, which can recover
lost keys and values after up to `k` node failures.

Exactly `n` nodes form the ring. Every primary and every backup copy resides on
these nodes. Membership and key ownership are permanent. Both fusion and the
standard successor-replication baseline support every integer `1 <= k < n`.

The implementation uses Python and SimPy. One reliable simulator coordinates
atomic operations and recovery, while SimPy events model logical communication.

| Source | Responsibility |
| --- | --- |
| [fusion_p2p/simulation.py](fusion_p2p/simulation.py) | Ring ownership, indexed primary storage, parity placement, operations, recovery, metrics |
| [fusion_p2p/coding.py](fusion_p2p/coding.py) | Record encoding, finite-field arithmetic, Cauchy coefficients, decoding |
| [fusion_p2p/test_simulation.py](fusion_p2p/test_simulation.py) | Coding, recovery, operation, memory, and benchmark tests |
| [chord_replication/simulation.py](chord_replication/simulation.py) | Conventional successor replication and paired benchmarks |
| [chord_replication/test_replication.py](chord_replication/test_replication.py) | Replication correctness, failure subsets, comparison tests |
| [chord_replication/visualize.py](chord_replication/visualize.py) | Matplotlib plots of CSV metrics against `k` |

The paired benchmark in `chord_replication` compares fusion against ordinary
replica maps. The older `scheme="replication"` mode within `fusion_p2p` also
retains record indices, giving it different storage costs.

## 2. Ownership and routing

Node IDs are sorted SHA-1 hashes of `node-0` through `node-(n-1)`. A key belongs to
the first node clockwise whose ID is greater than or equal to its SHA-1 hash.
Ownership wraps to the first node when the hash exceeds the largest node ID.

```text
owner(key) = clockwise_successor(SHA1(UTF8(key)))
```

The application supplies the key and value. The simulator chooses the owner.
Putting an existing key overwrites its value on that same owner. The system has
one primary owner for each key.

Ownership calculation uses binary search over the fixed node IDs. The simulated
request enters at ring position 0 and follows successor links to the owner.
The owner replies directly to the ingress node. Backup and recovery RPCs also
use direct point-to-point transfers.

The routing implemented for this experiment has up to `n-1` request hops. The
`O(log n)` network-hop bound associated with Chord finger-table routing does not
describe this experiment's successor-walk implementation.

## 3. Primary storage and record indices

Each fusion node maintains the following primary structures.

```python
store: dict[str, str]          # Key-value lookups
key_to_index: dict[str, int]   # Key's local fusion slot
records: list[Record | None]  # Slotted record objects and vacant slots
free_indices: list[int]       # Min-heap of vacant positions
```

New keys take the smallest free index or append a record. An overwrite retains
its index. Deletion removes the key from the dictionaries, clears the record,
and adds its index to the free heap. Other records retain their positions.

Different nodes can have different array lengths. The simulator retains a
committed maximum range `J`, representing the largest allocated array length.
Every index from `0` through `J-1` participates in coding. Absent and deleted
records contribute zero bytes. This metadata survives simulated node failures.

Fusion combines records by **local index**. For example, node A's slot 0 and
node B's slot 0 can contain completely different keys. Shared key names are not
required for backup savings.

## 4. Encoding and finite-field coding

### Fixed-size records

Every occupied record contains an occupancy flag, lengths, key, value, and padding.

| Field | Size |
| --- | --- |
| Occupancy flag | 1 byte |
| Key length | 4 bytes, big-endian |
| Value length | 4 bytes, big-endian |
| UTF-8 key and value | Actual encoded lengths |
| Zero padding | Remaining configured capacity |

The encoded primary record size is

```text
b = 9 + max_key_bytes + max_value_bytes
```

Defaults of 64 key bytes and 256 value bytes produce `b=329`. Maximum lengths
apply to UTF-8 bytes. An entirely zero record is empty. An occupied record can
contain an empty key and value. Decoding validates occupancy, lengths, UTF-8,
and zero padding.

### Independent parity equations

For index `j`, let `R[i,j]` be primary `i`'s encoded record. The `k` parity records
are computed independently for each byte position using finite-field arithmetic.

```text
P[p,j] = sum(A[p,i] * R[i,j] for i in 0..n-1)
A[p,i] = inverse(p XOR (k+i))
```

The coefficients form a Cauchy matrix. Its row labels `0..k-1` and column labels
`k..k+n-1` are disjoint field elements. Every square coefficient submatrix is
invertible, allowing any `r <= k` missing primary records to be recovered from
`r` independent parity equations and surviving primary records.

The field size grows automatically with the configuration.

```text
w = max(8, ceil(log2(n+k)))
s = ceil(w / 8)
B = b * s
```

`B` is the encoded parity record size. Each original byte is represented by one
field symbol. GF(256) uses polynomial `0x11D`. Wider fields use a deterministically
selected irreducible binary polynomial. At `n+k > 256`, parity symbols occupy
multiple bytes. The implementation accounts for the resulting storage and
transfer costs. This supports larger `k` without the former GF(256) size bound.

## 5. Parity placement and packing

There are `k` independent parity streams. Each stream contains one parity record
per committed index and is copied to `k+1` distinct ring nodes.

```text
hosts(p) = [(floor(p*n/k) + offset) mod n for offset in 0..k]
total parity-stream copies = k * (k+1)
```

All node types can host primary and backup data. A node can host several streams.
Some nodes host no parity stream when there are fewer copies than nodes.

Each hosted stream is one independent `bytearray`. Record `j` occupies bytes
`j*B` through `(j+1)*B-1`.

```text
stream buffer = [parity record 0][parity record 1]...[parity record J-1]
```

Packing removes a dictionary entry and a separate `bytes` object for every parity
record. Primary `Record` objects use dataclass slots. GF(256) coefficient rows use
packed `bytes`. These changes reduce Python representation overhead while retaining
the same `k(k+1)` copy placement and coding equations.

## 6. Normal operations

### Insert or overwrite

1. Validate and encode the new key-value record.
2. Find its permanent owner and allocate or retain its local index.
3. Route the request to the owner.
4. Compute `delta = old_record XOR new_record`.
5. For each parity equation, multiply the delta by that primary's coefficient.
6. Send that weighted delta to every host of the stream. Each host XORs it into
   the affected parity record.
7. After all required RPCs finish, atomically commit the primary maps, record
   slot, parity slices, and committed range. Return the acknowledgment.

Only the affected index is updated. A write does not serialize whole dictionaries
or recompute every parity index. A buffer append can require allocation and copying
as its capacity grows.

### Read and delete

Reads route to the owner and use its native dictionary. They return the value
or `None`. Deletion applies the same parity-delta mechanism with an empty record,
then frees the local slot. Deleting a missing key is a no-op with routing cost.

The simulator admits one operation at a time. It rejects overlapping operations
and blocks requests throughout an outage and recovery.

## 7. Failure model and recovery guarantee

A crash occurs between completed operations and clears the node's primary maps,
record array, index metadata, and all hosted backups. The simulator immediately
records the failures. It accepts up to `k` failed nodes and prevents additional
crashes during recovery.

The lowest surviving ring position coordinates recovery.

1. Read encoded records from surviving primaries.
2. Fetch one surviving copy of each of the `k` parity streams.
3. Build and invert the coefficient matrix for the `r` failed primaries using
   the first `r` streams. Reuse that inverse across all indices.
4. At each index, subtract surviving contributions and solve for missing records.
5. Decode keys and values. Rebuild each returning node's dictionaries, index map,
   record array, and free heap under its original node ID.
6. Recompute lost parity copies and transfer them to returning nodes.
7. Verify the candidate primary and backup state, install it, and resume requests.

Recovered arrays extend through `J`, preserving occupied indices and making
empty positions reusable. Recovery reconstructs failed data from surviving
records and parity. Expected pre-crash dictionaries are held only by tests.

For a failure set `F` of size `r`, the unknown records satisfy

```text
A_F * missing = parity - known_surviving_contributions
```

At least one copy of every stream survives because its `k+1` hosts are distinct.
The Cauchy submatrix `A_F` is invertible. These two properties provide recovery
for any permitted failure set. At `k=n-1`, a single survivor holds enough coded
state to restore the other nodes.

## 8. Standard replication baseline

The baseline uses identical ownership, routing, network parameters, workloads,
and failure schedules. Each primary has `k` additional complete copies on its
next `k` distinct successors. A node stores its own primary map and a dictionary
of replica maps by owner. Replicas contain ordinary string keys and values.

An update is committed to all copies before acknowledgment. Recovery fetches a
surviving replica for each failed owner and restores every replica lost with a
failed host. All `k+1` copies of one owner's data cannot be erased by `k` failures.

## 9. Algorithmic complexity

Use the following symbols when interpreting the bounds.

| Symbol | Meaning |
| --- | --- |
| `n`, `k`, `r` | Nodes, tolerated failures, and actual failures (`r <= k`) |
| `J` | Maximum committed record index range, including holes |
| `b`, `B` | Encoded primary and parity record sizes |
| `M`, `D` | Live primary record count and total logical UTF-8 key/value bytes |
| `h` | Request-routing hops, from 0 to `n-1` |
| `I` | Parity-stream copies lost with failed hosts, at most `r*k` |

The coding bounds count field operations. GF(256) multiplication uses lookup
tables. Wider-field multiplication uses polynomial arithmetic whose cost grows
with `w`. String hashing and encoding also scale with their byte lengths.

### Operation costs

| Operation | Fusion | Conventional replication |
| --- | --- | --- |
| Owner calculation | `O(key_bytes + log n)` CPU | Same |
| Local dictionary lookup | Expected `O(1)` after hashing | Same |
| Routed read | `O(h)` messages, worst-case `O(n)` | Same |
| Write coding | `O(k*b)` field operations | No field coding |
| Backup update/copy work | `O(k(k+1)*B)` byte processing | `O(k)` map updates plus string/message handling |
| Write network traffic | `O(h + k²)` messages, `O(h*request_bytes + k²*B)` bytes | `O(h+k)` messages, `O((h+k)*request_bytes)` bytes |
| Free-slot insertion/reuse | `O(log J)` heap operation | No fusion-slot heap |

The byte-buffer update cost is amortized for append growth. These bounds exclude
SimPy's event-queue scheduling overhead. Explicit verification runs separately
from normal writes and during recovery.

### Recovery costs

Core fusion decoding costs `O(r³)` field operations to invert the missing-node
matrix, followed by `O(J*r*n*b)` field operations to remove known contributions
and solve all indices. The inverse is computed once per episode.

The full implementation also recomputes parity to rebuild lost copies and again
to validate the candidate state. Each full parity pass costs `O(J*k*n*b)` field
operations. Copying and comparing all required packed copies adds up to
`O(J*k(k+1)*B)` byte work. Key validation, dictionary rebuilding, free-index heaps,
and placement checks add Python bookkeeping costs. Recovery timings include these
checks, so they exceed the cost of the linear solve alone.

Fusion recovery transfers

```text
O(n*J*b + (k+I)*J*B) bytes
O(n + k + r + I) logical RPCs
```

Local coordinator transfers are excluded from network counters. The byte bound
accounts for survivor reads, all parity-stream fetches, restored primary records,
and replaced parity copies. Since `I <= r*k`, recovery message growth can be
quadratic when both `r` and `k` increase together.

Replication recovery copies maps and rebuilds lost replicas, with no matrix
inversion. Its bytes depend on the failed owners' data and on all replica maps
lost with failed hosts. Validation still scans primary and replica content and
checks successor placement. The current Python verification loops add placement
enumeration overhead, so actual CPU is measured rather than inferred solely from
payload sizes.

### Retained and temporary storage

The exact retained encoded fusion backup payload is

```text
k * (k+1) * J * B bytes
```

Primary storage has `O(M)` records plus dictionaries and allocated slots through
each local array length, with up to `O(n*J)` slots after recovery. Coding and
placement metadata include `O(n*k)` coefficients and `O(k²)` host entries.

For equal arrays of `L=J*b` bytes and byte-sized parity symbols, ideal backup
payloads are `n*k*L` for full replication and `k*(k+1)*L` for fusion. Their ratio
is `(k+1)/n`. For wider parity symbols the ratio becomes `(k+1)*B/(n*b)`.
The actual conventional baseline stores variable-length strings, so fixed-record
padding, holes, unequal owner sizes, and Python metadata affect the comparison.

Recovery temporarily holds survivor snapshots, fetched streams, decoded records,
rebuilt buffers, and a candidate state alongside surviving storage. This includes
`O(n*J*b + (k+I)*J*B)` temporary encoded/copy data and an `O(r²)` decoder matrix,
plus Python objects. Benchmark storage columns measure retained state before the
crash. They do not measure peak recovery memory.

## 10. Network and timing model

Each remote transfer incurs a deterministic SimPy timeout.

```text
transfer_time = network_delay + payload_bytes / bandwidth
```

Local transfers consume zero simulated time and do not increment counters. An
RPC consists of a request and a reply, commonly a one-byte acknowledgment.
Independent backup RPCs run in parallel with independent bandwidth budgets.

For a fusion write, let `t` be the number of parity streams hosted by the owner.
There are `q = k(k+1)-t` remote backup updates. The message count is exactly
`h + 2*q + indicator(h>0)`. The baseline has `h + 2*k + indicator(h>0)` messages.

Fusion backup-update requests carry a two-byte stream field, an eight-byte index,
and `B` parity bytes. Replication put requests carry a command/length header and
the actual key/value bytes. Reads route their key and return the actual value.
Replica snapshots use an eight-byte entry count and four-byte key/value lengths.

The network model has no shared-link contention or aggregate node bandwidth cap.
Increasing the number of simultaneous RPCs therefore increases messages and bytes
without proportionally increasing simulated latency. Simulated time excludes
coding CPU time, disk I/O, real scheduling, failure detection, and restart delays.

CPU measurements use `time.process_time()` separately. Recovery CPU includes
decoding, rebuilding, and verification. Whole-workload CPU includes the additional
explicit benchmark verification calls. CPU results are single-run measurements
and vary between executions.

## 11. What the memory and CSV metrics mean

| Metric | Interpretation |
| --- | --- |
| `primary_payload_bytes` | Actual UTF-8 key and value bytes, summed once per primary record |
| `primary_storage_bytes` | Recursive Python size of selected primary maps, arrays, records, and free-index structures |
| `backup_payload_bytes` | All encoded parity buffer lengths, or replica snapshot byte counts including framing |
| `backup_storage_bytes` | Recursive Python size of hosted backup structures, including container and buffer overhead |
| `global_metadata_bytes` | Measured ring/configuration/coding metadata |
| `total_storage_bytes` | Primary + backup + global metadata |
| `primary_metadata_bytes`, `backup_metadata_bytes` | Representation totals minus their respective payload counters |
| `storage_overhead` | Backup memory divided by that scheme's primary memory |
| `write_latency`, `read_latency`, `delete_latency` | Mean successful operation durations in simulated time units |
| `recovery_time` | Duration of one full recovery episode in simulated time units |
| `message_count`, `bytes_transferred` | Whole-workload network counters, including recovery |
| `recovery_messages`, `recovery_bytes` | Network counters restricted to recovery |
| `actual_cpu_seconds`, `recovery_cpu_seconds` | Measured process CPU seconds |
| `record_bytes`, `parity_record_bytes`, `field_bits`, `index_range` | Coding configuration, with zero placeholders in the conventional baseline |

Memory accounting uses `sys.getsizeof`. Shared references within one node/role
are counted once. Different simulated nodes and roles are measured independently,
even though they execute in one Python process. The figures describe selected
storage structures. They exclude interpreter/module memory, node wrapper objects,
latency histories, SimPy queues, and temporary recovery allocations.

The fields named `metadata_bytes` are accounting differences rather than a precise
count of structural headers. UTF-8 payload counts, Python string representations,
and replica wire framing have different layouts. Measurements depend on Python
version, container allocation, workload size, and encoding.

### Avoiding a misleading percentage comparison

Fusion has extra primary indices and records. Dividing each backup by its own
primary representation can make fusion's percentage look better solely because
its denominator is larger. The CSV retains that explicitly defined ratio.

The visualization instead plots

```text
100 * backup_storage_bytes / primary_payload_bytes
```

The logical data denominator is identical for paired workloads, making the
percentage and absolute-backup rankings agree. The plot also includes total
storage. Percentages are undefined for zero logical payload. Very large
percentages are expected when tiny datasets are dominated by Python overhead.

## 12. Actual testing and benchmark results

### Environment and coverage

The verified run used Python 3.14.7, SimPy 4.1.2, and pytest 9.2.0 on macOS 26.6
ARM64. The combined suites passed **395 tests in 8.09 seconds**.

Tests cover every valid `k` for rings of two through five nodes and every failure
subset of sizes one through `k`, for fusion and both replication implementations.
They check restored keys/values, deleted keys, unchanged ownership and occupied
indices, all replacement backups, another write, and a second recovery episode.

Other tests cover UTF-8/length validation, malformed records, finite-field
arithmetic, Cauchy decoding with different equation choices, incremental updates,
index reuse, empty/uneven maps, rejected requests during recovery, and independent
packed copies. A coding test reconstructs 128 failed primaries out of 129. A
256-node simulation checks wider-field parity and network/storage accounting.

### Workloads

Two benchmark runs produced **46 CSV rows across 23 paired configurations**.

- The all-`k` run used `n=8`, every `k=1..7`, two target records per node, and
  eight-byte ASCII values.
- The broader sweep used `(4,1)`, `(4,2)`, `(5,2)`, and `(8,1)`, with eight or
  24 target records per node and 32-byte or 128-byte values.

Both used seed 17, maximum key length 32 bytes, maximum value length equal to
the workload's value size, delay 2, and bandwidth 100000 bytes per simulated unit.
Keys are deterministic `key-NNNNNN` strings. SHA-1 distributes them without forcing
equal per-owner counts. Each run inserts all keys, overwrites every fifth key,
reads every key, deletes every seventh key, then crashes positions `0..k-1`
and recovers. Storage is measured after deletes and before the crash.

### All-`k` storage results

This workload has 16 initial records and 13 live records after deletion, with
234 logical UTF-8 bytes. The committed range is four, and parity records are
49 bytes each. All figures below are bytes.

| k | Fusion backup | Replication backup | Fusion total | Replication total |
| --- | --- | --- | --- | --- |
| 1 | 1,394 | 4,668 | 9,983 | 7,996 |
| 2 | 3,158 | 7,544 | 11,936 | 10,872 |
| 3 | 5,164 | 10,420 | 14,119 | 13,748 |
| 4 | 7,412 | 13,296 | 16,504 | 16,624 |
| 5 | 10,222 | 16,172 | 19,499 | 19,500 |
| 6 | 13,850 | 20,072 | 23,424 | 23,400 |
| 7 | 18,552 | 22,948 | 28,311 | 26,276 |

Fusion uses less backup memory at every tested `k` in this workload. Its primary
structures use 6,292 bytes versus 2,652 for replication. This extra primary and
configuration state can outweigh the backup savings. Total-memory differences
of only a few bytes, such as the `k=5` result, are sensitive to Python allocation
and should not be treated as robust advantages.

### All-`k` network and recovery results

The table selects three failure budgets. Total counters include normal operations
and recovery. Recovery duration is simulated time. Recovery CPU is actual milliseconds.

| k | Scheme | Total messages | Total bytes | Recovery messages | Recovery bytes | Recovery duration | Recovery CPU ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Fusion | 284 | 7,582 | 16 | 1,642 | 12.00617 | 0.703 |
| 1 | Replication | 236 | 4,085 | 6 | 113 | 12.00113 | 0.099 |
| 4 | Fusion | 1,030 | 31,432 | 36 | 3,712 | 12.00619 | 2.353 |
| 4 | Replication | 414 | 6,859 | 46 | 1,063 | 12.00295 | 0.254 |
| 7 | Fusion | 2,550 | 82,618 | 112 | 11,578 | 8.00412 | 4.142 |
| 7 | Replication | 618 | 10,490 | 112 | 2,870 | 8.00226 | 0.435 |

For all seven budgets, mean read latency was 9.250676 simulated units for both
schemes. Mean write latency was 13.401635 for fusion and 13.401315 for replication.
Mean delete latency was 14.668160 and 14.667533 respectively. Their near-constant
values reflect parallel RPCs and identical routing, while messages/bytes grow.

Recovery at `k=7` has a shorter simulated duration because the single surviving
coordinator already holds the necessary source copies. Some collection phases
become local and contribute zero network delay. Coding CPU still increases.
The smaller simulated duration does not establish cheaper overall recovery.

### Larger workload examples

These cases use 24 target records per node and 128-byte values. All storage
columns are bytes. Timings are simulated units.

| n | k | Scheme | Backup | Total storage | Mean write | Recovery |
| --- | --- | --- | --- | --- | --- | --- |
| 4 | 2 | Fusion | 58,472 | 92,358 | 6.744527 | 12.25886 |
| 4 | 2 | Replication | 42,592 | 63,840 | 6.744207 | 12.15073 |
| 5 | 2 | Fusion | 46,144 | 89,116 | 9.157148 | 12.21830 |
| 5 | 2 | Replication | 54,248 | 81,232 | 9.156828 | 12.10839 |
| 8 | 1 | Fusion | 15,840 | 80,141 | 13.461993 | 12.20309 |
| 8 | 1 | Replication | 42,896 | 84,480 | 13.461673 | 12.10547 |

The `(8,1)` case saves both backup and total storage. The `(5,2)` case saves
backup storage while using more total storage. The `(4,2)` case uses more of
both. The current scheme's benefit depends on data size, imbalance, padding,
failure budget, and representation overhead.

### Effect of the memory optimizations

Before packing, each parity record had its own dictionary entry and `bytes`
object. On the same eight-node `k=7` small workload, the encoded parity payload
was 10,976 bytes but backup representation occupied 35,296 bytes.

After packing, the same encoded payload occupies 18,552 bytes of backup memory,
a 47.4% reduction. Slots and packed coefficients reduced other retained storage.
Total memory fell from 47,774 to 28,311 bytes. Before/after checks across the
46 benchmark rows confirmed identical encoded payload sizes, network counters,
and simulated timings. Packing reduced representation overhead.

## 13. Running and reproducing the results

Run from the repository root with Python 3.10 or newer.

```sh
python3 -m venv /tmp/fusion-study-venv
source /tmp/fusion-study-venv/bin/activate
pip install -r requirements.txt pytest matplotlib

python -m pytest fusion_p2p/test_simulation.py chord_replication/test_replication.py -q

python -m chord_replication.simulation \
  --configs 8:1 8:2 8:3 8:4 8:5 8:6 8:7 \
  --records 2 --value-bytes 8 --seed 17 \
  --delay 2 --bandwidth 100000 --output /tmp/fusion_report_all_k.csv

python -m chord_replication.simulation --output /tmp/fusion_report_sweep.csv

python -m chord_replication.visualize /tmp/fusion_report_all_k.csv \
  --output /tmp/fusion_report_all_k.png
```

The measured CSVs for this document were written to `/private/tmp/fusion_report_all_k.csv`
and `/private/tmp/fusion_report_sweep.csv`. These are temporary outputs that can
be regenerated using the commands above. Tables in this document retain the
reported results independently of those files. CPU and Python memory figures
can vary with the runtime and machine.

An application scenario uses the same generator interface.

```python
import simpy
from fusion_p2p.simulation import Simulation, run

env = simpy.Environment()
system = Simulation(env, n=8, k=2)
run(env, system.put("account-1", "100"))
owner = system.owner("account-1")
run(env, system.crash([owner, (owner + 1) % system.n]))
run(env, system.recover())
assert run(env, system.get("account-1")) == "100"
run(env, system.put("account-1", "110"))
system.verify()
```

## 14. Interpretation and limits

The tested implementation reconstructs lost data and restores redundancy for
the supported failure model. Its storage savings come from sharing coded records
across primaries and reducing Python representation overhead. They depend on
the workload and need to be evaluated using total storage as well as backup size.

The `k(k+1)` placement adds quadratic backup-copy and update costs. Fixed-size
records and the largest committed index range retain padding and holes. Primary
index maps and coding metadata add memory. Coding, collection, and consistency
validation increase recovery CPU and traffic relative to full replicas.

The experiment assumes atomic writes, immediate failure notification, permanent
membership, a reliable coordinator, and recovery without concurrent requests or
additional failures. It does not model incomplete writes, partitions, Byzantine
behavior, shared-link contention, or crash/restart wall-clock delays. The fresh
benchmarks use one seed and one recovery episode per row. Correctness tests add
exhaustive small-ring failure sets and repeated episodes, while performance results
remain workload samples rather than general throughput or production guarantees.
