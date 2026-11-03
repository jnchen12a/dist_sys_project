from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, Generator
from simpy.events import Event

if TYPE_CHECKING:
    from chord.src.node import Node


class Chord:
    m = 160

    def __init__(self, ip: str, nPrime: int | None, self_node: Node) -> None:
        self.self_node = self_node
        self.id = self._hash_string(ip)
        # Reserve a unique address before starting any processes. The simulation
        # transport registry detects collisions, including not-yet-ready nodes.
        if len(self_node.network.inboxes) >= 2**self.m:
            raise ValueError("The identifier space is full")
        nonce = 0
        while self.id in self_node.network.inboxes:
            self.id = self._hash_string(f"{ip}:{nonce}")
            nonce += 1
        self.fingerStart = [
            (self.id + 2**i) % (2**self.m) for i in range(self.m)
        ]
        self.fingerInterval = [
            (self.fingerStart[i], (self.id + 2**(i + 1)) % (2**self.m))
            for i in range(self.m)
        ]
        self.fingerNode = [self.id] * self.m
        self.predecessor = self.id

    @property
    def successor(self) -> int:
        # The successor is the first finger, with one source of truth.
        return self.fingerNode[0]

    @successor.setter
    def successor(self, node_id: int) -> None:
        self.fingerNode[0] = node_id

    def get_predecessor(self) -> int:
        return self.predecessor

    def set_predecessor(self, p: int) -> None:
        self.predecessor = p

    def get_successor(self) -> int:
        return self.successor

    def get_id(self) -> int:
        return self.id

    @staticmethod
    def _in_interval(
        value: int, start: int, end: int,
        include_start: bool = False, include_end: bool = False,
    ) -> bool:
        """Test a clockwise interval, including full-circle intervals."""
        if start == end:
            return value != start or include_start or include_end
        if value == start:
            return include_start
        if value == end:
            return include_end
        if start < end:
            return start < value < end
        return value > start or value < end

    def join(self, ip: str, nPrime: int | None) -> Generator[Event, Any, None]:
        """Node schedules this after registering its inbox.

        Wait for Node.ready before starting another join. The initial state is
        already a singleton ring when there is no bootstrap node.
        """
        if nPrime is not None:
            yield from self._init_finger_table(nPrime)
            yield from self._update_others()

    def _init_finger_table(self, nPrime: int) -> Generator[Event, Any, None]:
        self.successor = yield from self._find_successor(nPrime, self.fingerStart[0])
        self.predecessor = yield from self._get_other_predecessor(self.successor)
        yield from self._set_other_predecessor(self.successor, self.id)
        for i in range(self.m - 1):
            # The old ring does not yet route to us. Starts in our newly
            # acquired interval already belong to us, including after wraparound.
            if self._in_interval(
                self.fingerStart[i + 1], self.predecessor, self.id,
                include_end=True,
            ):
                self.fingerNode[i + 1] = self.id
            elif self._in_interval(
                self.fingerStart[i + 1], self.id, self.fingerNode[i],
                include_start=True,
            ):
                self.fingerNode[i + 1] = self.fingerNode[i]
            else:
                self.fingerNode[i + 1] = yield from self._find_successor(
                    nPrime, self.fingerStart[i + 1]
                )

    def _update_others(self) -> Generator[Event, Any, None]:
        for i in range(self.m):
            target = (self.id - 2**i) % (2**self.m)
            # Include a node exactly at target: its i-th finger starts at us.
            p = yield from self._execute_find_predecessor(
                (target + 1) % (2**self.m)
            )
            yield from self._other_update_finger_table(p, self.id, i)

    def _update_finger_table(self, s: int, i: int) -> Generator[Event, Any, None]:
        if s != self.id and self._in_interval(
            s, self.id, self.fingerNode[i], include_start=True
        ):
            self.fingerNode[i] = s
            yield from self._other_update_finger_table(self.predecessor, s, i)

    def _find_successor(self, n: int, id: int) -> Generator[Event, Any, int]:
        predecessor = yield from self._find_predecessor(n, id)
        return (yield from self._get_other_successor(predecessor))

    def _find_predecessor(self, n: int, id: int) -> Generator[Event, Any, int]:
        if n == self.id:
            return (yield from self._execute_find_predecessor(id))
        predecessor = yield self.self_node.send_message(n, f"FIND_PREDECESSOR {id}")
        return predecessor

    def _execute_find_predecessor(self, id: int) -> Generator[Event, Any, int]:
        id %= 2**self.m
        nPrime = self.id
        nPrimeSuccessor = yield from self._get_other_successor(nPrime)
        while not self._in_interval(id, nPrime, nPrimeSuccessor, include_end=True):
            closest = yield from self._get_closest_preceeding_finger(nPrime, id)
            # Successors allow progress even if other fingers are stale.
            nPrime = nPrimeSuccessor if closest == nPrime else closest
            nPrimeSuccessor = yield from self._get_other_successor(nPrime)
        return nPrime

    def _execute_closest_preceeding_finger(self, k: int) -> int:
        for i in range(self.m - 1, -1, -1):
            if self._in_interval(self.fingerNode[i], self.id, k):
                return self.fingerNode[i]
        return self.id

    def _get_closest_preceeding_finger(
        self, nPrime: int, id: int
    ) -> Generator[Event, Any, int]:
        if nPrime == self.id:
            return self._execute_closest_preceeding_finger(id)
        return (yield self.self_node.send_message(
            nPrime, f"CLOSEST_PRECEEDING_FINGER {id}"
        ))

    def _hash_string(self, s: str) -> int:
        return int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16) % (2**self.m)

    def _get_other_predecessor(self, nPrime: int) -> Generator[Event, Any, int]:
        if nPrime == self.id:
            return self.predecessor
        return (yield self.self_node.send_message(nPrime, "GET_PREDECESSOR"))

    def _set_other_predecessor(
        self, nPrime: int, p: int
    ) -> Generator[Event, Any, None]:
        if nPrime == self.id:
            self.set_predecessor(p)
        else:
            yield self.self_node.send_message(nPrime, f"SET_PREDECESSOR {p}")

    def _get_other_successor(self, nPrime: int) -> Generator[Event, Any, int]:
        if nPrime == self.id:
            return self.successor
        return (yield self.self_node.send_message(nPrime, "GET_SUCCESSOR"))

    def _other_update_finger_table(
        self, nPrime: int, s: int, i: int
    ) -> Generator[Event, Any, None]:
        if nPrime == self.id:
            yield from self._update_finger_table(s, i)
        else:
            yield self.self_node.send_message(nPrime, f"UPDATE_FINGER_TABLE {s} {i}")
