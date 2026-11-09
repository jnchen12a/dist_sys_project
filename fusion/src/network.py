"""Reliable delayed delivery while endpoints remain in the same incarnation."""

from copy import deepcopy


class Network:
    def __init__(self, env, delay=2):
        if delay < 0:
            raise ValueError("Network delay cannot be negative")
        self.env, self.delay = env, delay
        self.nodes = {}
        self.inboxes = {}

    def register(self, node):
        if node.node_id in self.nodes:
            raise ValueError(f"Duplicate node ID: {node.node_id!r}")
        self.nodes[node.node_id] = node
        self.inboxes[node.node_id] = node.inbox

    def send(self, sender, receiver, kind, request_id, value=None):
        source, target = self.nodes[sender], self.nodes[receiver]
        if source.alive and target.alive:
            message = deepcopy((sender, kind, request_id, value))
            self.env.process(self.deliver(
                source, target, source.generation, target.generation, message
            ))

    def deliver(self, source, target, source_generation, target_generation, message):
        yield self.env.timeout(self.delay)
        if (source.alive and target.alive
                and source.generation == source_generation
                and target.generation == target_generation):
            yield target.inbox.put(message)
