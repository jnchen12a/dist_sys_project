class Chord():
    def __init__(self) -> None:
        self.finger = []
        self.predecessor = None

    def join(self, n) -> None:
        '''
        Have node n join the chord
        '''
        # check if n is in the network
        if n:
            self._init_finger_table(n)

        else:
            # n = none, we are the only node in the network
            for i in range(m):
                self.finger[i] = n

            self.predecessor = n

    def _init_finger_table(self, n) -> None:
        '''
        Initializes node's own finger table
        '''
        self.finger[0]

    def _update_others(self) -> None:
        pass

    def _update_finger_table(self, s, i) -> None:
        pass