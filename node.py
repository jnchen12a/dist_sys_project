import simpy, itertools
from network import Network
from chord import Chord

class Node:
    def __init__(self, env: simpy.Environment, network: Network, ip: str, nPrime: int):
        self.env = env
        self.network = network
        self.chord = Chord(ip, nPrime, self)

        self.inbox = simpy.Store(env)
        network.inboxes[self.chord.id] = self.inbox

        self.request_ids = itertools.count()
        self.pending = {}
        env.process(self.receive_loop())

    def send_message(self, target: int, msg: str):
        request_id = next(self.request_ids)
        reply_event = self.env.event()
        self.pending[request_id] = reply_event

        self.network.send(self.chord.id, target, msg, request_id)

        return reply_event

    def receive_loop(self):
        while True:
            sender, kind, request_id, value = yield self.inbox.get()

            print(f"t={self.env.now}: Node {self.chord.id} received {kind}")

            if kind == "GET_SUCCESSOR":
                self.network.send(
                    self.chord.id, sender, "REPLY",
                    request_id, self.chord.successor
                )

            elif kind == "REPLY":
                reply_event = self.pending.pop(request_id)
                reply_event.succeed(value)