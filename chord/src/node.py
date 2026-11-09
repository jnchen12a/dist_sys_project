import itertools

import simpy

from chord.src.network import Network
from chord.src.chord import Chord


class Node:
    def __init__(
        self, env: simpy.Environment, network: Network, ip: str,
        nPrime: int | None = None,
    ):
        self.env = env
        self.network = network
        if nPrime is not None and nPrime not in network.inboxes:
            raise ValueError(f"Unknown bootstrap node: {nPrime}")
        self.inbox = simpy.Store(env)
        self.request_ids = itertools.count()
        self.pending = {}
        self.chord = Chord(ip, nPrime, self)
        network.inboxes[self.chord.id] = self.inbox
        env.process(self.receive_loop())
        self.ready = env.process(self.chord.join(ip, nPrime))

    def send_message(self, target: int, msg: str):
        if target not in self.network.inboxes:
            raise ValueError(f"Unknown destination node: {target}")
        request_id = next(self.request_ids)
        reply_event = self.env.event()
        self.pending[request_id] = reply_event
        self.network.send(self.chord.id, target, msg, request_id)
        return reply_event

    def receive_loop(self):
        while True:
            sender, kind, request_id, value = yield self.inbox.get()
            if kind in ("REPLY", "ERROR"):
                reply_event = self.pending.pop(request_id)
                if kind == "ERROR":
                    reply_event.fail(value)
                else:
                    reply_event.succeed(value)
            else:
                # Handlers can wait for remote calls without blocking replies.
                self.env.process(self._handle_request(sender, kind, request_id))

    def _handle_request(self, sender: int, message: str, request_id: int):
        try:
            command, *args = message.split()
            if command == "GET_SUCCESSOR":
                result = self.chord.successor
            elif command == "GET_PREDECESSOR":
                result = self.chord.predecessor
            elif command == "SET_PREDECESSOR":
                self.chord.set_predecessor(int(args[0]))
                result = None
            elif command == "FIND_PREDECESSOR":
                result = yield from self.chord._execute_find_predecessor(int(args[0]))
            elif command == "CLOSEST_PRECEEDING_FINGER":
                result = self.chord._execute_closest_preceeding_finger(int(args[0]))
            elif command == "UPDATE_FINGER_TABLE":
                yield from self.chord._update_finger_table(int(args[0]), int(args[1]))
                result = None
            else:
                raise ValueError(f"Unknown request: {message}")
        except Exception as error:
            self.network.send(self.chord.id, sender, "ERROR", request_id, error)
        else:
            self.network.send(self.chord.id, sender, "REPLY", request_id, result)
