from ensurepip import bootstrap
from kafka import KafkaProducer
import json

TOPIC_NAME = 'test'
KAFKA_SERVER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER)
for i in range(50000,80000):
    username = "gsdgb{}kdhsubt{}".format(i , i+10)
    passw = "shder6{}iu483nmsv{}cjcmxmfh{}".format(i , i+5 , i+10)
    data = json.dumps({"id" : i , "username": username , "pass":passw}).encode('utf-8')
    producer.send(TOPIC_NAME , data)
    producer.flush()

