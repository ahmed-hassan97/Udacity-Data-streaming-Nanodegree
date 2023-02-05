from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer , Consumer
from dataclasses import dataclass, field
from faker import Faker
import json , random
import time 

BROKER_URL = "localhost:9092"
TOPIC_NAME = "info"

faker = Faker()
a = AdminClient({'bootstrap.servers': BROKER_URL})

id =0
@dataclass
class info:
    device: str = random.choice(["device_0" , "device_1" , "device_2" , "device_3"])
    purpose: str = random.choice(["temperature" , "humadity" , "pressure"])
    value: str = random.uniform(10.5, 75.5)
    #address: str = field(default_factory=faker.address)
    longitude: str = random.uniform(27.5, 75.5)
    latitude: int = random.uniform(-27.5, 75.5)
 

    def seralize(self):
        """Serializes the object in JSON string format"""
        return json.dumps(
            {
                "device": random.choice(["device_0" , "device_1" , "device_2" , "device_3"]),
                "purpose":random.choice(["temperature" , "humadity" , "pressure"]),
               # "value":random.uniform(10.5, 75.5),
                #"address":self.address,
               # "longitude": random.uniform(27.5, 75.5),
               # "latitude":random.uniform(-27.5, 75.5)
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
        time.sleep(4)
       

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
        print("shutting down")

if __name__ == "__main__":
    main()
