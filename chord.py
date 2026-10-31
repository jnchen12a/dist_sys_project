import hashlib
from node import Node
# figure 4 is mostly complete, just need to figure out how messages and stuff work
# TODO: finish figure 7? is that even needed to be implemented? why is join implemented twice?


class Chord():
    m = 160

    def __init__(self, ip: str, nPrime: int, self_node: Node) -> None:
        # nPrime is a node already in the chord that we are given
        self.fingerNode = []
        self.fingerStart = []
        self.fingerInterval = []
        self.predecessor = 0
        self.successor = 0
        self.id = 0
        self.self_node = self_node
        self.join(ip, nPrime)

    def get_predecessor(self) -> int:
        return self.predecessor

    def set_predecessor(self, p: int) -> None:
        self.predecessor = p

    def get_successor(self) -> int:
        return self.successor

    def get_id(self) -> int:
        return self.id

    # done for now
    def join(self, ip: str, nPrime: int) -> None:
        '''
        Have node n join the chord
        '''
        # check if n is in the network
        if nPrime:
            # first, try to set our own ID
            nonce = 0
            while True:
                candidateID = self._hash_string(ip)
                owner = self._find_successor(nPrime, candidateID)
                if owner != candidateID:
                    self.id = candidateID
                    self.successor = owner
                    break
                ip = f'{ip}:{nonce}'
                nonce += 1
            self._init_finger_table(nPrime)

        else:
            # n = none, we are the only node in the network
            for i in range(self.m):
                self.fingerNode[i] = nPrime
                self.fingerStart[i] = None
                self.fingerInterval[i] = []

            self.predecessor = nPrime

    def _init_finger_table(self, nPrime: int) -> None:
        '''
        Initializes node's own finger table
        '''
        self.fingerNode[0] = self._find_successor(nPrime, self.fingerStart[0])
        predecessor = self._get_other_predecessor(nPrime)
        self._set_other_predecessor(self.successor, self.id)
        for i in range(self.m - 2):
            if self.fingerStart[i + 1] >= self.id and self.fingerStart[i + 1] < self.fingerNode[i]:
                self.fingerNode[i + 1] = self.fingerNode[i]
            else:
                self.fingerNode[i + 1] = self._find_successor(nPrime, self.fingerStart[i + 1])


    def _update_others(self) -> None:
        for i in range(self.m):
            p = self._execute_find_predecessor(self.id - (2 ** i))
            self._other_update_finger_table(p, self.id, i + 1)

    def _update_finger_table(self, s: int, i: int) -> None:
        if s >= self.id and s <= self.fingerNode[i]:
            self.fingerNode[i] = s
            p = self.predecessor
            self._other_update_finger_table(p, s, i)

    # need to figure out how messages are sent
    # done for now
    def _find_successor(self, n: int, id) -> int:
        '''
        Asks node n to find the sucessor of id
        '''
        if n == self.id:
            # execute this ourselves
            nPrime = self._execute_find_predecessor(id)
            return nPrime.successor
        else:
            # send message to node n to find predecessor
            nPrime = self._find_predecessor(n, id)
            return nPrime.successor

    def _find_predecessor(self, n: int, id) -> int:
        '''
        Asks node n to find the predecessor of k
        '''
        if n == self.id:
            return self._execute_find_predecessor(id)
        else:
            # send a message to node n to find predecessor
            _ = yield self.self_node.send_message(n, f"FIND_PREDECESSOR {id}")

    def _execute_find_predecessor(self, id) -> int:
        '''
        Actually run find_predecessor code
        '''
        nPrime = self.id
        nPrimeSuccessor = '' # ACTUALLY GET THIS
        while id <= nPrime or id > nPrimeSuccessor:
            nPrime = self._get_closest_preceeding_finger(id)
            nPrimeSuccessor = '' # ACTUALLY GET THIS
        return nPrime

    def _execute_closest_preceeding_finger(self, k) -> int:
        '''
        Actually run closest_preceeding_finger code
        '''
        for i in range(0, self.m, -1):
            if self.fingerNode[i] > self.id and self.fingerNode[i] < k:
                return self.fingerNode[i]
        return self.id

    def _get_closest_preceeding_finger(self, id) -> int:
        '''
        Message node n to execute closest_preceeding_finger on itself and message back the result
        '''
        pass

    def _hash_string(self, s) -> int:
        return int(hashlib.sha1(s.encode('utf-8')).hexdigest(), 16)

    def _get_other_predecessor(self, nPrime: int) -> int:
        '''
        Send message to node nPrime asking for its predecessor
        '''
        if self.id == nPrime:
            return self.predecessor
        pass

    def _set_other_predecessor(self, nPrime: int, p) -> None:
        '''
        Send message to other node nPrime asking to set its predecessor to p
        '''
        if self.id == nPrime:
            self.set_predecessor(p)
        pass

    def _other_update_finger_table(self, nPrime: int, s: int, i: int) -> None:
        '''
        Send message to node nPrime to update its finger table
        '''
        if self.id == nPrime:
            self._update_finger_table(s, i)