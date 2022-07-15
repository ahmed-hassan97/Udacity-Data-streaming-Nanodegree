#! /bin/bash

sudo docker exec -it kafka bash
cd opt/kafka/bin
kafka-console-consumer.sh --topic test --bootstrap-server localhost:9092 --from-beginning

