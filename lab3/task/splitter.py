import zmq
import time
import sys


def read_sentences(filename):
    """Liest Sätze aus Datei, eine pro Zeile."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            sentences = [line.strip() for line in f if line.strip()]
        return sentences
    except FileNotFoundError:
        print(f"Datei {filename} nicht gefunden.")
        return None


if __name__ == "__main__":
    context = zmq.Context()

    # push-socket for mappers
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:5555")

    print("Splitter startet...")
    print("Warte auf Mapper-Verbindungen...")
    time.sleep(1)

    
    sentences = read_sentences("mydata.txt")
    if sentences is None:
        sentences = [
            "Hello world",
            "This is a test",
            "ZeroMQ is great for messaging",
            "MapReduce is a programming model",
            "Python is a versatile language",
            "Distributed systems can be complex",
            "Let's count some words",
            "This is the last sentence"
        ]
        print("Standard-Text geladen:")

    print(f"Sende {len(sentences)} Sätze an Mapper...")

    # distribute sentences
    for i, sentence in enumerate(sentences):
        sender.send_string(sentence) #zmq does round-robin automatically
        print(f"[Splitter] Satz {i+1}: {sentence}")
        time.sleep(0.1)  # small delay


    print("[Splitter] Sende Shutdown-Signale...")
    for i in range(3):  # 3 Mapper
        sender.send(b'')
        print("[Splitter] Shutdown-Signal "+ str(i+1) +" gesendet")
        time.sleep(0.1)

    sender.close()
    context.term()
    print("[Splitter] Beendet.")