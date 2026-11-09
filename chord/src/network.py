import simpy

class Network:
    def __init__(self, env: simpy.Environment):
        self.env = env
        self.inboxes = {}

    def send(self, sender, receiver, kind, request_id, value=None):
        self.env.process(
            self.deliver(sender, receiver, kind, request_id, value)
        )

    def deliver(self, sender, receiver, kind, request_id, value):
        yield self.env.timeout(2)  # Network delay
        message = (sender, kind, request_id, value)
        yield self.inboxes[receiver].put(message)