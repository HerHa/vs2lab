import logging
import random
import time

from constMutex import ENTER, RELEASE, ALLOW, ACTIVE


class Process:
    """
    Implements access management to a critical section (CS) via fully
    distributed mutual exclusion (MUTEX).

    Processes broadcast messages (ENTER, ALLOW, RELEASE) timestamped with
    logical (lamport) clocks. All messages are stored in local queues sorted by
    logical clock time.

    Processes follow different behavioral patterns. An ACTIVE process competes 
    with others for accessing the critical section. A PASSIVE process will never 
    request to enter the critical section itself but will allow others to do so.

    A process broadcasts an ENTER request if it wants to enter the CS. A process
    that doesn't want to ENTER replies with an ALLOW broadcast. A process that
    wants to ENTER and receives another ENTER request replies with an ALLOW
    broadcast (which is then later in time than its own ENTER request).

    A process enters the CS if a) its ENTER message is first in the queue (it is
    the oldest pending message) AND b) all other processes have sent messages
    that are younger (either ENTER or ALLOW). RELEASE requests purge
    corresponding ENTER requests from the top of the local queues.

    Message Format:

    <Message>: (Timestamp, Process_ID, <Request_Type>)

    <Request Type>: ENTER | ALLOW  | RELEASE

    """
    
    def __init__(self, chan):
        self.channel = chan  # Create ref to actual channel
        self.process_id = self.channel.join('proc')  # Find out who you are
        self.all_processes: list = []  # All procs in the proc group
        self.other_processes: list = []  # Needed to multicast to others
        self.queue = []  # The request queue list
        self.clock = 0  # The current logical clock
        self.peer_name = 'unassigned'  # The original peer name
        self.peer_type = 'unassigned'  # A flag indicating behavior pattern
        self.logger = logging.getLogger("vs2lab.lab5.mutex.process.Process")
        
        # failure handling
        self.suspected = set()
        self.last_seen = {}
        self.miss_count = {}
        self.receive_timeout = 4
        self.miss_threshold = 4

        # current local ENTER attempt
        self.requesting = False
        self.request_clock = None
            


    def __mapid(self, id='-1'):
        # format channel member address
        if id == '-1':
            id = self.process_id
        return 'Proc-'+str(id)

    def __cleanup_queue(self):
        if len(self.queue) > 0:
            # self.queue.sort(key = lambda tup: tup[0])
            self.queue.sort()
            # There should never be old ALLOW messages at the head of the queue
            while self.queue and self.queue[0][2] == ALLOW:
                del (self.queue[0])
                if len(self.queue) == 0:
                    break
    
    def __remove_process_from_queue(self, pid):
        self.queue = [msg for msg in self.queue if msg[1] != pid]
        self.__cleanup_queue()


    def __request_to_enter(self):
        self.clock = self.clock + 1  # Increment clock value
        request_msg = (self.clock, self.process_id, ENTER)
        
        self.requesting = True # this process is now requesting to enter the CS
        self.request_clock = self.clock

        for pid in self.other_processes: # Reset miss count for all other processes when making a new request
            if pid not in self.suspected:
                self.miss_count[pid] = 0

        self.queue.append(request_msg)  # Append request to queue
        self.__cleanup_queue()  # Sort the queue
        self.channel.send_to(self.active_view(), request_msg)  # Send request to all unsuspected processes

    def __allow_to_enter(self, requester):
        self.clock = self.clock + 1  # Increment clock value
        msg = (self.clock, self.process_id, ALLOW)
        self.channel.send_to([requester], msg)  # Permit other

    def __release(self):
        # need to be first in queue to issue a release
        assert self.queue and self.queue[0][1] == self.process_id, 'State error: inconsistent local RELEASE'

        # construct new queue from later ENTER requests (removing all ALLOWS)
        tmp = [r for r in self.queue[1:] if r[2] == ENTER]
        self.queue = tmp  # and copy to new queue
        self.clock = self.clock + 1  # Increment clock value
        msg = (self.clock, self.process_id, RELEASE)
        # Multicast release notification
        self.channel.send_to(self.active_view(), msg)

        self.requesting = False
        self.request_clock = None
    
    def __waiting_for(self):
        if not self.requesting or self.request_clock is None:
            return set()

        waiting = set()

        for pid in self.other_processes:
            if pid in self.suspected: # ignore sus processes
                continue

            seen_newer_msg = any( # check if there is a newer message from this process
                msg[1] == pid and
                msg[0] > self.request_clock and
                msg[2] in (ENTER, ALLOW)
                for msg in self.queue
            )

            if not seen_newer_msg:
                waiting.add(pid) # wait for this process if no newer message has been seen

        return waiting
    
    def __allowed_to_enter(self):
        if not self.queue:
            return False

        # own enter request must be first in queue
        if self.queue[0][1] != self.process_id or self.queue[0][2] != ENTER:
            return False

        if self.__waiting_for():
            return False

        return True

    def __receive(self):
        _receive = self.channel.receive_from(self.other_processes, self.receive_timeout)

        if _receive:
            msg = _receive[1]
            sender = msg[1]

            self.last_seen[sender] = time.time() # update last seen
            self.miss_count[sender] = 0

            if sender in self.suspected:
                self.logger.warning(f"{self.__mapid(sender)} is responding again; removing suspicion.")
                self.suspected.discard(sender)

            self.clock = max(self.clock, msg[0])
            self.clock = self.clock + 1

            self.logger.debug("{} received {} from {}.".format(
                self.__mapid(),
                "ENTER" if msg[2] == ENTER
                else "ALLOW" if msg[2] == ALLOW
                else "RELEASE",
                self.__mapid(sender)))

            if msg[2] == ENTER:
                self.queue.append(msg)
                self.__allow_to_enter(sender)

            elif msg[2] == ALLOW:
                self.queue.append(msg)

            elif msg[2] == RELEASE:
                # robuster als assert: entferne passendes ENTER dieses Prozesses
                removed = False
                for i, entry in enumerate(self.queue):
                    if entry[1] == sender and entry[2] == ENTER:
                        del self.queue[i]
                        removed = True
                        break

                if not removed:
                    self.logger.warning(f"RELEASE from {self.__mapid(sender)} without matching ENTER in queue.")

            self.__cleanup_queue()
            return sender

        else:
            self.logger.info("{} timed out on RECEIVE. Local queue: {}".
                             format(self.__mapid(),
                                    list(map(lambda msg: (
                                        'Clock '+str(msg[0]),
                                        self.__mapid(msg[1]),
                                        msg[2]), self.queue))))
            return None 
    
    def __blocking_processes(self):
        """
        Returns processes whose ENTER is currently before my own ENTER
        and therefore blocks my entry into the CS.
        """
        blockers = set()

        if not self.requesting:
            return blockers

        for msg in self.queue:
            if msg[1] == self.process_id and msg[2] == ENTER: # end when own ENTER is reached
                break
            if msg[2] == ENTER: # only consider ENTER messages as blockers
                blockers.add(msg[1])

        return blockers

    def __check_failures(self):
        if not self.requesting:
            return

        relevant = set(self.__waiting_for()) # waiting processes that are relevant for failure suspicion
        relevant.update(self.__blocking_processes()) # add blocking processes whose ENTER is before my own ENTER in the queue

        for pid in relevant:
            if pid in self.suspected:
                continue

            self.miss_count[pid] = self.miss_count.get(pid, 0) + 1

            if self.miss_count[pid] >= self.miss_threshold:
                if pid not in self.suspected:
                    self.logger.warning("Suspecting {}".format(pid))
                self.suspected.add(pid)
                self.__remove_process_from_queue(pid)
                
    def active_view(self):
        return [p for p in self.other_processes if p not in self.suspected]


    def init(self, peer_name, peer_type):
        self.channel.bind(self.process_id)

        self.all_processes = list(self.channel.subgroup('proc'))
        # sort string elements by numerical order
        self.all_processes.sort(key=lambda x: int(x))

        self.other_processes = list(self.channel.subgroup('proc'))
        self.other_processes.remove(self.process_id)

        self.peer_name = peer_name  # assign peer name
        self.peer_type = peer_type  # assign peer behavior

        self.logger.info("{} joined channel as {}.".format(
            peer_name, self.__mapid()))
        
        for pid in self.other_processes:
            self.last_seen[pid] = None
            self.miss_count[pid] = 0


    def run(self):
        while True:
            # Enter the critical section if
            # 1) there are more than one process left and
            # 2) this peer has active behavior and
            # 3) random is true
            if len(self.all_processes) > 1 and \
                    self.peer_type == ACTIVE and \
                    random.choice([True, False]):
                self.logger.debug("{} wants to ENTER CS at CLOCK {}."
                                  .format(self.__mapid(), self.clock))

                self.__request_to_enter()

                while not self.__allowed_to_enter():
                    sender = self.__receive()

                    if sender is None:
                        self.__check_failures() # check for failures if receive timed out
                   


                # Stay in CS for some time ...
                sleep_time = random.randint(0, 2000)
                self.logger.debug("{} enters CS for {} milliseconds."
                                  .format(self.__mapid(), sleep_time))
                print(" CS <- {}".format(self.__mapid()))
                time.sleep(sleep_time/1000)

                # ... then leave CS
                print(" CS -> {}".format(self.__mapid()))
                self.__release()
                continue

            # Occasionally serve requests to enter (
            if random.choice([True, False]):
                self.__receive()
