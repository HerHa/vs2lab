import zmq
import sys
import hashlib

def hash_word_to_reducer(word, num_reducers=2):
    """Berechnet, welcher Reducer für dieses Wort zuständig ist."""
    return int.from_bytes(hashlib.sha256(word.encode()).digest(), byteorder='big') % num_reducers


if __name__ == "__main__":
    # mapper id
    if len(sys.argv) < 2:
        print("Verwendung: python mapper.py <mapper_id>")
        sys.exit(1)
    
    mapper_id = sys.argv[1]
    context = zmq.Context()

    # pull-sockets for receiving from splitter
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://localhost:5555")

    # push-sockets for sending to 2 reducers
    sender_reducers = []
    for reducer_id in range(2):
        sender = context.socket(zmq.PUSH)
        sender.connect(f"tcp://localhost:{5556 + reducer_id}")
        sender_reducers.append(sender)

    print(f"[Mapper {mapper_id}] Startet...")
    print(f"[Mapper {mapper_id}] Wartet auf Splitter...")
    print(f"[Mapper {mapper_id}] Versucht Reducer zu erreichen...")

    sentence_count = 0
    word_count = 0

    try:
        while True:
            # receive from splitter
            sentence = receiver.recv_string()

            if not sentence:
                print(f"[Mapper {mapper_id}] Shutdown-Signal empfangen")
                for sender in sender_reducers:
                    sender.send(b'')
                print(f"[Mapper {mapper_id}] Shutdown-Signale an Reducer gesendet")
                break

            sentence_count += 1
            print(f"[Mapper {mapper_id}] Empfangen Satz {sentence_count}: {sentence}")

            
            words = sentence.lower().split()

            # send words
            for word in words:
                clean_word = ''.join(c for c in word if c.isalnum())
                if not clean_word:
                    continue

                reducer_id = hash_word_to_reducer(clean_word, num_reducers=2)
                
                sender_reducers[reducer_id].send_string(clean_word)
                word_count += 1

    except KeyboardInterrupt:
        print(f"[Mapper {mapper_id}] Unterbrochen")
    finally:
        receiver.close()
        for sender in sender_reducers:
            sender.close()
        context.term()
        print(f"[Mapper {mapper_id}] Beendet. Sätze: {sentence_count}, Wörter: {word_count}")
