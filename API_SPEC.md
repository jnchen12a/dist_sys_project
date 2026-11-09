# Fusion API

SimPy simulation of integer maps protected by one XOR backup. Implemented in
`src/fusion.py`; messaging is in `src/node.py` and `src/network.py`.

## Quick start

Install dependencies with `pip install -r requirements.txt`. Run from the repo root:

```python
import simpy
from src.fusion import FusionCluster

env = simpy.Environment()
cluster = FusionCluster(env, primary_ids=(0, 1, 2), backup_id="backup")

def scenario():
    yield env.process(cluster.put(0, "count", 42))
    cluster.crash(0)  # Immediate; clears this primary's data.
    yield env.process(cluster.recover(0))
    assert (yield env.process(cluster.get(0, "count"))) == 42
    yield env.process(cluster.put(0, "count", 43))
    yield env.process(cluster.delete(0, "count"))

env.run(until=env.process(scenario()))
```

## Public cluster interface

`FusionCluster(env, primary_ids=(0, 1, 2), backup_id="backup")`

- `env`: `simpy.Environment`.
- IDs: unique hashable addresses, conventionally integers or strings. Supply at
  least one primary; the backup ID must be distinct. `"__client__"` is reserved.
- Keys are strings. Values are Python integers, including zero and negative
  values; booleans are rejected by `put`.

All methods below except `crash` return **generators**: run them with
`env.process(...)`, then yield or run until the resulting process completes.

| Method | Result | Behavior |
| --- | --- | --- |
| `put(primary_id, key, value)` | `None` | Insert or replace an entry; update the backup before installing the primary map. |
| `get(primary_id, key)` | `int` | Read an entry from a healthy primary. |
| `delete(primary_id, key)` | `None` | Remove an existing entry and its backup contribution. |
| `crash(node_id)` | `None` (immediate) | Stop a primary or backup and erase its data. |
| `recover(node_id)` | `None` | Restart and reconstruct a primary, or rebuild the backup; mark ready after installation is acknowledged. |

Errors propagate when the operation is run:

| Error | Conditions |
| --- | --- |
| `NodeUnavailable` (`src.node`) | Required node is down/unready, an RPC times out, a down node is crashed again, or a write/recovery detects multiple unavailable processes. |
| `KeyError` | Unknown cluster node ID, or missing key on `get`/`delete`. |
| `TypeError` | Invalid key/value on `put`, or invalid key on `delete`. |
| `ValueError` | Invalid constructor IDs, or recovery requested for an already healthy node. |

## Messaging interface

`Node(env, network, node_id, timeout=20)` registers an endpoint with
`Network(env, delay=2)`. Times are simulated units; timeout must exceed twice the
network delay. Duplicate endpoint IDs are rejected.

- `node.register_handler(command, handler)`: register a synchronous function
  `handler(payload) -> result`. `REPLY` and `ERROR` are reserved transport kinds.
- `node.send_message(target, msg, value=None) -> simpy.Event`: send a command and
  yield the returned event **directly** for its reply. Handler exceptions fail
  the event; missing replies fail with `NodeUnavailable`. Unknown destinations
  raise `ValueError` immediately.
- `node.crash()` / `node.restart()`: control transport availability. Restarting
  alone does not reconstruct fusion state; use `cluster.recover(...)` for that.

Transport envelope: `(sender_id, kind, request_id, value)`. For requests, `kind`
is the command; for replies it is `REPLY` or `ERROR`. Payloads are copied, and
messages are dropped if either endpoint crashes or changes generation before
delivery. Late replies to expired requests are ignored.

### Internal fusion commands

These are used by `FusionCluster`; application code should use its public API.
Sending mutation commands directly bypasses serialization and can corrupt parity.

| Receiver | Command | Payload | Reply |
| --- | --- | --- | --- |
| Primary | `SNAPSHOT` | `None` | Copy of `dict[str, int]` |
| Primary | `INSTALL` | Replacement `dict[str, int]` | `None` |
| Backup | `APPLY` | `(primary_id, key, old, new, new_present)` | `None` |
| Backup | `SNAPSHOT` | `None` | `(parity, members)` |
| Backup | `REBUILD` | `{primary_id: primary_map}` for all primaries | `None` |

`parity` is `dict[str, int]`; `members` maps each primary ID to its `set[str]`
of keys. For `APPLY`, absent values contribute zero; `new_present` determines
whether the key remains in that primary's membership set.

## Guarantees and limits

- One protected process may be unavailable at a time. Fully recover it before
  another fails. Failure injection and recovery are explicit, not automatic.
- Public reads, writes, and recovery are serialized by a reliable driver lock.
  The driver/client itself is outside the protected failure model.
- Acknowledged writes survive one process failure. An interrupted write may be
  present or absent after recovery despite a timeout. No automatic retries or
  deduplication are provided; do not replay internal `APPLY` messages.
- With a primary down, healthy primaries can continue operating. With the backup
  down, writes fail until recovery; healthy primary reads remain available.
- Treat `cluster.primaries`, `cluster.backup`, and their dictionaries as inspection
  state; mutate data through the public cluster API.
- This models SimPy processes, not OS processes. Encoding is per key; storage
  savings depend on key overlap. Updates currently transfer whole primary maps.
