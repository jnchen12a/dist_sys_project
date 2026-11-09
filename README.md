# Fusion fault tolerance simulation

Multiple primary processes hold string-to-integer maps, protected by one fused
XOR backup. One primary or the backup may crash, lose all its data, restart,
reconstruct its state, and continue serving operations.

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.demo_fusion
python -m unittest discover -s test -v
```

## Source structure

- `src/network.py`: delayed message delivery. Incarnation numbers drop messages
  involving crashed/restarted endpoints; payloads are copied.
- `src/node.py`: generic endpoint with `send_message(target, command, payload)`,
  synchronous registered handlers, request/reply correlation, timeouts, crash,
  and restart. Chord dependencies have been removed.
- `src/fusion.py`: `Primary`, `FusedBackup`, and the `FusionCluster` driver.
- `src/demo_fusion.py`: write, crash, recover, continue, and rebuild the backup.
- `test/test_fusion.py`: recovery, in-flight crashes, concurrency, and invariants.

## Interface

```python
import simpy
from src.fusion import FusionCluster

env = simpy.Environment()
cluster = FusionCluster(env, primary_ids=(0, 1, 2))

def scenario():
    yield env.process(cluster.put(0, "counter", 42))
    cluster.crash(0)  # Immediate simulated failure; clears the map.
    yield env.process(cluster.recover(0))  # Restart and reconstruct the map.
    assert (yield env.process(cluster.get(0, "counter"))) == 42
    yield env.process(cluster.put(0, "counter", 43))
    yield env.process(cluster.delete(0, "counter"))

env.run(until=env.process(scenario()))
```

## Encoding and recovery

For each key, the backup stores the XOR of values from all primaries containing
that key. Missing entries contribute zero. Updates XOR out the old value and XOR
in the new value. Per-primary key sets distinguish missing entries from stored
zeros and preserve membership after deletion.

To recover a primary, start with the backup parity and XOR out every surviving
primary's value, for each key belonging to the failed primary. No copy of the
failed primary's values is retained. If the backup fails, rebuild parity and
membership from all primaries. Signed and arbitrarily large integers work.

## Failure and consistency model

- These are SimPy processes, not operating-system processes. Failure is injected
  with `crash`; recovery is explicitly requested with `recover`.
- At most one protected process may be unavailable at a time. Recover it fully
  before another failure. The simulation client/driver is assumed reliable.
- Public operations are serialized through one driver lock, including recovery.
  State travels through simulated request/reply messages. Do not mutate process
  dictionaries or invoke internal write RPCs yourself.
- An acknowledged write survives recovery. The backup commits before primary
  installation; a write interrupted by a crash may be present or absent even
  though the caller received a timeout. Automatic retries are not provided.
- Surviving primaries can write while one primary is down. Operations targeting
  the failed primary fail until recovery. A down backup blocks writes until
  rebuilt; reads from healthy primaries remain available.
- Recovery returns a node to service after installation is acknowledged.
  Request timeouts prevent lost messages from hanging the simulation.
- Transport is reliable with fixed delay while endpoints remain alive.
  Partitions, duplicated messages, and Byzantine behavior are outside this model.

This is a simple map-based fusion prototype. It encodes by **key**, so storage
savings depend on overlap between primary key sets. Disjoint keys do not reduce
the number of stored values. Key membership metadata is replicated. It does not
implement the compact node packing used in research fused data structures, and
currently transfers whole map snapshots for updates and recovery. This establishes
crash/recovery correctness before optimizing storage or messaging.
