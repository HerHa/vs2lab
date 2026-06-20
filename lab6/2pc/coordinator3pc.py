import random
import logging

import stablelog

# coordinator messages
from const2PC import VOTE_REQUEST, GLOBAL_COMMIT, GLOBAL_ABORT
# participant messages
from const2PC import VOTE_COMMIT, VOTE_ABORT
# misc constants
from const2PC import TIMEOUT

# 3PC-specific messages
PREPARE_COMMIT = 'PREPARE_COMMIT'
READY_COMMIT = 'READY_COMMIT'


class Coordinator:
    """
    Implements a three phase commit coordinator.
    - state written to stable log (but recovery is not considered)
    - simulates possible crash failure after vote request
    - adds PRECOMMIT phase to avoid blocking on coordinator crash
    """

    def __init__(self, chan):
        self.channel = chan
        self.coordinator = self.channel.join('coordinator')
        self.participants = []
        self.stable_log = stablelog.create_log("coordinator-" + self.coordinator)
        self.logger = logging.getLogger("vs2lab.lab6.3pc.Coordinator")
        self.state = None

    def _enter_state(self, state):
        self.stable_log.info(state)
        self.logger.info("Coordinator {} entered state {}.".format(self.coordinator, state))
        self.state = state

    def init(self):
        self.channel.bind(self.coordinator)
        self._enter_state('INIT')
        self.participants = self.channel.subgroup('participant')

    def run(self):
        # Simulate crash before doing anything
        if random.random() > 3/4:
            return "Coordinator crashed in state INIT."

        # Phase 1a: Send VOTE_REQUEST to all participants
        self._enter_state('WAIT')
        self.channel.send_to(self.participants, VOTE_REQUEST)

        # Simulate crash after sending VOTE_REQUEST (before collecting votes)
        if random.random() > 2/3:
            return "Coordinator crashed in state WAIT (before collecting votes)."

        # Phase 2a: Collect votes from all participants
        yet_to_receive = list(self.participants)
        while len(yet_to_receive) > 0:
            msg = self.channel.receive_from(self.participants, TIMEOUT)

            if (not msg) or (msg[1] == VOTE_ABORT):
                # At least one abort or timeout -> global abort
                reason = "timeout" if not msg else "local_abort from " + msg[0]
                self._enter_state('ABORT')
                self.channel.send_to(self.participants, GLOBAL_ABORT)
                return "Coordinator {} terminated in state ABORT. Reason: {}.".format(
                    self.coordinator, reason)
            else:
                assert msg[1] == VOTE_COMMIT
                yet_to_receive.remove(msg[0])

        # All participants voted COMMIT -> enter PRECOMMIT, send PREPARE_COMMIT
        self._enter_state('PRECOMMIT')
        self.channel.send_to(self.participants, PREPARE_COMMIT)

        # Simulate crash after sending PREPARE_COMMIT (before collecting READY_COMMIT)
        if random.random() > 2/3:
            return "Coordinator crashed in state PRECOMMIT."

        # Phase 3a: Collect READY_COMMIT acknowledgements
        yet_to_receive = list(self.participants)
        while len(yet_to_receive) > 0:
            msg = self.channel.receive_from(self.participants, TIMEOUT)

            if not msg:
                # Participant timed out in PRECOMMIT state -> safe to commit anyway (all participants that reached PRECOMMIT can only commit)
                self.logger.warning(
                    "Coordinator: timeout waiting for READY_COMMIT, proceeding to COMMIT.")
                break
            else:
                assert msg[1] == READY_COMMIT
                yet_to_receive.remove(msg[0])

        # All (reachable) participants are in PRECOMMIT -> safe to commit
        self._enter_state('COMMIT')
        self.channel.send_to(self.participants, GLOBAL_COMMIT)
        return "Coordinator {} terminated in state COMMIT.".format(self.coordinator)