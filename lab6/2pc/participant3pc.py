import random
import logging

# coordinator messages
from const2PC import VOTE_REQUEST, GLOBAL_COMMIT, GLOBAL_ABORT
# participant decisions
from const2PC import LOCAL_SUCCESS, LOCAL_ABORT
# participant messages
from const2PC import VOTE_COMMIT, VOTE_ABORT
# misc constants
from const2PC import TIMEOUT

# 3PC-specific messages
PREPARE_COMMIT = 'PREPARE_COMMIT'
READY_COMMIT = 'READY_COMMIT'
# termination protocol messages (sent between participants)
NEED_DECISION = 'NEED_DECISION'
STATE_REPORT = 'STATE_REPORT'

import stablelog


class Participant:
    """
    Implements a three phase commit participant.
    - state written to stable log (but recovery is not considered)
    - non-blocking: coordinator crash is handled via election of a new coordinator
      among surviving participants, using the deterministic rule: lowest ID wins
    - the extra PRECOMMIT state ensures no participant is left uncertain:
      if any participant has already committed, the new coordinator will too
    """

    def __init__(self, chan):
        self.channel = chan
        self.participant = self.channel.join('participant')
        self.stable_log = stablelog.create_log("participant-" + self.participant)
        self.logger = logging.getLogger("vs2lab.lab6.3pc.Participant")
        self.coordinator = {}
        self.all_participants = {}
        self.state = 'NEW'

    @staticmethod
    def _do_work():
        return LOCAL_ABORT if random.random() > 2/3 else LOCAL_SUCCESS

    def _enter_state(self, state):
        self.stable_log.info(state)
        self.logger.info("Participant {} entered state {}.".format(self.participant, state))
        self.state = state

    def init(self):
        self.channel.bind(self.participant)
        self.coordinator = self.channel.subgroup('coordinator')
        self.all_participants = self.channel.subgroup('participant')
        self._enter_state('INIT')

    def _termination_protocol(self):
        """
        Elect a new coordinator (lowest ID) and bring all participants
        to a consistent final state without blocking.

        State ordering used for the election:
          COMMIT > PRECOMMIT > READY > ABORT/INIT
        A participant with a 'later' state overrides earlier ones.
        """
        self.logger.info(
            "Participant {} starting termination protocol in state {}.".format(
                self.participant, self.state))

        # Broadcast our own state to all other participants so they can incorporate it, then collect theirs.
        self.channel.send_to(self.all_participants, (STATE_REPORT, self.state))

        # Gather STATE_REPORT from every other participant (best-effort with timeout)
        others = list(self.all_participants)
        others.remove(self.participant)
        peer_states = {self.participant: self.state}

        for _ in range(len(others)):
            msg = self.channel.receive_from_any(TIMEOUT * 2)
            if msg and msg[1] and isinstance(msg[1], tuple) and msg[1][0] == STATE_REPORT:
                peer_states[msg[0]] = msg[1][1]

        self.logger.info("Participant {} collected peer states: {}".format(
            self.participant, peer_states))

        # Determine the new coordinator (lowest ID)
        new_coordinator = min(peer_states.keys())

        # Determine the global decision based on the highest observed state
        #
        # Rules:
        # Any COMMIT seen  -> everyone must COMMIT
        # Any PRECOMMIT seen (and no COMMIT) -> everyone must COMMIT (all survivors are in READY, PRECOMMIT or COMMIT, so it is safe)
        # Otherwise (INIT / READY / ABORT mix) -> ABORT
        states_seen = set(peer_states.values())

        if 'COMMIT' in states_seen:
            global_decision = GLOBAL_COMMIT
        elif 'PRECOMMIT' in states_seen:
            # At least one participant reached PRECOMMIT, which means the coordinator sent PREPARE_COMMIT to everyone. None can have aborted yet, so we can safely commit.
            global_decision = GLOBAL_COMMIT
        else:
            # Participants are in INIT / READY / ABORT -> safe to abort
            global_decision = GLOBAL_ABORT

        self.logger.info(
            "Participant {} (new coordinator: {}) decided: {}.".format(
                self.participant, new_coordinator, global_decision))

        # The elected new coordinator broadcasts the decision, others just apply it.
        if self.participant == new_coordinator:
            self.channel.send_to(self.all_participants, global_decision)

        return global_decision
    
    def run(self):
        # Phase 1b: wait for VOTE_REQUEST 
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:
            # Coordinator crashed before sending VOTE_REQUEST -> safe to abort
            self._enter_state('ABORT')
            return "Participant {} terminated in state ABORT (coordinator silent).".format(
                self.participant)

        assert msg[1] == VOTE_REQUEST

        # Perform local work
        decision = self._do_work()

        if decision == LOCAL_ABORT:
            # Vote abort and quit, no need to wait further
            self._enter_state('ABORT')
            self.channel.send_to(self.coordinator, VOTE_ABORT)
            return "Participant {} terminated in state ABORT (local abort).".format(
                self.participant)

        # Local work succeeded -> enter READY and cast a commit vote
        assert decision == LOCAL_SUCCESS
        self._enter_state('READY')
        self.channel.send_to(self.coordinator, VOTE_COMMIT)

        # Phase 2b: wait for PREPARE_COMMIT or GLOBAL_ABORT
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:
            # Coordinator crashed while we were in READY -> run termination
            final_decision = self._termination_protocol()

        elif msg[1] == GLOBAL_ABORT:
            self._enter_state('ABORT')
            return "Participant {} terminated in state ABORT (global abort after READY).".format(
                self.participant)

        else:
            assert msg[1] == PREPARE_COMMIT
            # Phase 2b: enter PRECOMMIT, acknowledge
            self._enter_state('PRECOMMIT')
            self.channel.send_to(self.coordinator, READY_COMMIT)

            # Phase 3b: wait for GLOBAL_COMMIT
            msg = self.channel.receive_from(self.coordinator, TIMEOUT)

            if not msg:
                # Coordinator crashed while we were in PRECOMMIT -> run termination
                final_decision = self._termination_protocol()
            else:
                assert msg[1] == GLOBAL_COMMIT
                final_decision = GLOBAL_COMMIT

        # Apply the final decision
        if final_decision == GLOBAL_COMMIT:
            self._enter_state('COMMIT')
        else:
            self._enter_state('ABORT')

        return "Participant {} terminated in state {}.".format(
            self.participant, self.state)