import zmq
import sys
from collections import defaultdict


if __name__ == "__main__":
    # get reducer id from command line
    if len(sys.argv) < 2:
        print("Verwendung: python reducer.py <reducer_id>")
        sys.exit(1)
    
    reducer_id = int(sys.argv[1])
    if reducer_id == 1:
        port = 5556
    elif reducer_id == 2:
        port = 5557
    context = zmq.Context()

    # create pull-sockets for the mappers
    receiver = context.socket(zmq.PULL)
    receiver.bind(f"tcp://*:{port}")

    print(f"[Reducer {reducer_id}] Startet auf Port {port}...")
    print(f"[Reducer {reducer_id}] Warte auf Wörter von Mappern...")


    word_counts = defaultdict(int)
    total_words = 0

    try:
        while True:
            word = receiver.recv_string()

            
            if not word:
                print(f"[Reducer {reducer_id}] Shutdown-Signal empfangen")
                break

            word = word.lower()
            
            word_counts[word] += 1
            total_words += 1

            # print wordcount update
            print(f"[Reducer {reducer_id}] {word}: {word_counts[word]}")

    except KeyboardInterrupt:
        print(f"\n[Reducer {reducer_id}] Unterbrochen")
    finally:
        print(f"\n[Reducer {reducer_id}] ========== FINALE ERGEBNISSE ==========")
        print(f"[Reducer {reducer_id}] Insgesamt {total_words} Wörter verarbeitet")
        print(f"[Reducer {reducer_id}] Eindeutige Wörter: {len(word_counts)}")
        print(f"\n[Reducer {reducer_id}] Wort-Häufigkeiten:")
        
        # Sortiert nach Häufigkeit (absteigend)
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        for word, count in sorted_words:
            print(f"[Reducer {reducer_id}]   {word}: {count}")

        # Cleanup
        receiver.close()
        context.term()
        print(f"[Reducer {reducer_id}] Beendet.")
