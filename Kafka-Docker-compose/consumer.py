from kafka import KafkaConsumer
import json
TOPIC_NAME = "test"

consumer = KafkaConsumer(TOPIC_NAME)

for message in consumer:
    data = message.value
    decoded_hand = json.loads(data).values()
    decoded_hand = tuple(decoded_hand)
    print(decoded_hand)


# kafka-console-producer.sh  --broker-list localhost:9092 --topic "test"
# kafka-console-consumer.sh --topic test --bootstrap-server localhost:9092 
# kafka-console-consumer.sh --topic test --bootstrap-server localhost:9092 --from-beginning
