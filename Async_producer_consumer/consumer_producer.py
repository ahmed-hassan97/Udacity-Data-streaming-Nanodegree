import asyncio
from dataclasses import dataclass, field
import json
import random
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

faker = Faker()
TOPIC_NAME = "hassans"
BROKER_URL = "localhost:9092"


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL, 
            "group.id": "0",
            "auto.offset.reset": "earliest",
        }

            )
    c.subscribe([topic_name])

    while True:
        #
        # TODO: Write a loop that uses consume to grab 5 messages at a time and has a timeout.
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.consume
        #
        messages = c.consume(5, timeout=1.0)
        # TODO: Print something to indicate how many messages you've consumed. Print the key and value of
        #       any message(s) you consumed
        print(f"consumed {len(messages)} messages")
        for message in messages:
            print(f"consume message {message.key()}: {message.value()}")

        # Do not delete this!
        await asyncio.sleep(0.01)

def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
    [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1) ] 
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            print("topic is found")
            pass       

    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")

async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, Purchase().serialize())
        ## waiting for Ack
        p.flush()


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2

@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )


if __name__ == "__main__":
    main()
