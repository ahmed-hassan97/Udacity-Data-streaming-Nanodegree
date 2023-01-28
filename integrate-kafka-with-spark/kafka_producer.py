from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer , Consumer
from dataclasses import dataclass, field
from faker import Faker
import json , random
import time 

BROKER_URL = "localhost:9092"
TOPIC_NAME = "kafka-spark-integration"


faker = Faker()
a = AdminClient({'bootstrap.servers': BROKER_URL})

id =0
@dataclass
class info:
    username : str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    address: str = field(default_factory=faker.address)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))
    id +=1

    def seralize(self):
        """Serializes the object in JSON string format"""
        return json.dumps(
            {
                "id": self.id,
                "username": self.username,
                "currency":self.currency,
                "address":self.address,
                "amount":self.amount
            }
                )

def produce(topic_name):
    """Produces data synchronously into the Kafka Topic""" 
    ## TODO: 
    # Configure the Producer to: 
    # # 1. Have a Client ID 
    # # 2. Have a batch size of 100 
    # # 3. A Linger Milliseconds of 1 second 
    # # 4. LZ4 Compression 
    p = Producer(
            {
                "bootstrap.servers": BROKER_URL,
                "client.id": "ex4",
                "linger.ms": 1000,
                "compression.type": "lz4",
                "batch.num.messages": 100,
            }
        )
    while True:
        p.produce(topic_name , info().seralize())
        time.sleep(10)
       

def create_topic(TOPIC_NAME):
    """Creates the topic with the given topic name"""
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

def main():
    """Checks for topic and creates the topic if it does not exist"""
    create_topic(TOPIC_NAME)
    try:
        produce(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shsutting down")

if __name__ == "__main__":
    main()
