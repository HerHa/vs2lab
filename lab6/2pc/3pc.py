"""
Application performing a distributed 3-phase commit.
Drop-in replacement for 2pc.py – swap coordinator/participant imports only.
"""

import multiprocessing as mp
import logging

import coordinator3pc as coordinator
import participant3pc as participant
from context import lab_channel, lab_logging

lab_logging.setup(stream_level=logging.INFO, file_level=logging.DEBUG)

logger = logging.getLogger("vs2lab.lab6.3pc.3pc")


def create_and_run(num_bits, proc_class, enter_bar, run_bar):
    chan = lab_channel.Channel(n_bits=num_bits)
    proc = proc_class(chan)
    enter_bar.wait()
    proc.init()
    run_bar.wait()
    logger.info(proc.run())


if __name__ == "__main__":
    m = 8   # address bits
    n = 3   # number of participants

    chan = lab_channel.Channel()
    chan.channel.flushall()

    mp.set_start_method('spawn')

    bar1 = mp.Barrier(n + 1)
    bar2 = mp.Barrier(n + 1)

    participants = []
    for i in range(n):
        p = mp.Process(
            target=create_and_run,
            name="Participant-" + str(i),
            args=(m, participant.Participant, bar1, bar2))
        participants.append(p)
        p.start()

    coord = mp.Process(
        target=create_and_run,
        name="Coordinator",
        args=(m, coordinator.Coordinator, bar1, bar2))
    coord.start()

    coord.join()
    for p in participants:
        p.join()