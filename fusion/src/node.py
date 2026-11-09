"""Reusable request/reply endpoint for the SimPy transport."""

import itertools

import simpy

from src.network import Network


class NodeUnavailable(RuntimeError):
    pass


class Node:
    def __init__(self, env: simpy.Environment, network: Network, node_id,
                 timeout=20):
        if timeout <= 2 * network.delay:
            raise ValueError("Timeout must exceed the network round-trip delay")
        self.env, self.network, self.node_id = env, network, node_id
        self.timeout = timeout
        self.request_ids = itertools.count()
        self.pending = {}
        self.handlers = {}
        self.alive = True
        self.generation = 0
        self.inbox = simpy.Store(env)
        network.register(self)
        self.receiver = env.process(self.receive_loop())

    def register_handler(self, command, handler):
        """Handlers are synchronous functions accepting one payload argument."""
        self.handlers[command] = handler

    def send_message(self, target, msg, value=None):
        if not self.alive:
            raise NodeUnavailable(f"Node {self.node_id!r} is down")
        if target not in self.network.nodes:
            raise ValueError(f"Unknown destination node: {target!r}")
        request_id = next(self.request_ids)
        reply = self.env.event()
        self.pending[request_id] = reply
        self.network.send(self.node_id, target, msg, request_id, value)
        self.env.process(self._expire(request_id))
        return reply

    def _expire(self, request_id):
        yield self.env.timeout(self.timeout)
        reply = self.pending.pop(request_id, None)
        if reply is not None:
            reply.fail(NodeUnavailable("Request timed out; outcome may be unknown"))

    def receive_loop(self):
        try:
            while True:
                sender, kind, request_id, value = yield self.inbox.get()
                if kind in ("REPLY", "ERROR"):
                    reply = self.pending.pop(request_id, None)
                    if reply is not None:
                        if kind == "ERROR":
                            reply.fail(value)
                        else:
                            reply.succeed(value)
                    continue
                try:
                    if kind not in self.handlers:
                        raise ValueError(f"Unknown request: {kind}")
                    result = self.handlers[kind](value)
                except Exception as error:
                    self.network.send(self.node_id, sender, "ERROR", request_id, error)
                else:
                    self.network.send(self.node_id, sender, "REPLY", request_id, result)
        except simpy.Interrupt:
            return

    def crash(self):
        if not self.alive:
            raise NodeUnavailable(f"Node {self.node_id!r} is already down")
        self.alive = False
        self.generation += 1
        self.receiver.interrupt()
        for reply in self.pending.values():
            reply.fail(NodeUnavailable(f"Node {self.node_id!r} crashed"))
        self.pending.clear()

    def restart(self):
        if self.alive:
            raise ValueError("Node is already alive")
        self.generation += 1
        self.inbox = simpy.Store(self.env)
        self.network.inboxes[self.node_id] = self.inbox
        self.alive = True
        self.receiver = self.env.process(self.receive_loop())
