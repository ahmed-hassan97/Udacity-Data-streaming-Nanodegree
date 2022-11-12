from confluent_kafka import Producer , Consumer
from dataclasses import dataclass, field
import json , random

BROKER_URL = "localhost:9092"
TOPIC_NAME = "info"


def consumer(topic_name):
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    while True:
        #
        # TODO: Write a loop that uses consume to grab 5 messages at a time and has a timeout.
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.consume
        #
        messages = c.consume(5, timeout=1.0  )
        for message in messages:
            if message is None:
                print("no message in topic")
            else:    
                print(f"consume message {message.key()}: {message.value()}")

if __name__ == "__main__":
    consumer(TOPIC_NAME)
