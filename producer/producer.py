# producer/producer.py
import json
import time
from datetime import datetime
from kafka import KafkaProducer

def make_message(i):
    return {
        "id": i,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "demo-producer",
        "value": 20 + (i % 10)  # simple changing value
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )
    i = 0
    try:
        print("Producer started. Sending messages to topic 'test-topic' every 2s. Ctrl+C to stop.")
        while True:
            msg = make_message(i)
            producer.send("test-topic", value=msg)
            producer.flush()
            print("Sent:", msg)
            i += 1
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
