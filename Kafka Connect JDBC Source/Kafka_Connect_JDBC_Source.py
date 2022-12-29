import asyncio
import json
from confluent_kafka import Producer , Consumer
from dataclasses import dataclass, field
import json , random

BROKER_URL = "localhost:9092"

import requests

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "user-info"


def configure_connector():
    """Calls Kafka Connect to create the Connector"""
    print("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    #
    # TODO: Complete the Kafka Connect Config below.
    #       See: https://docs.confluent.io/current/connect/references/restapi.html
    #       See: https://docs.confluent.io/current/connect/filestream_connector.html#filesource-connector
    #
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": "clicks-jdbc",  # TODO
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",  # TODO
                    "topic.prefix": CONNECTOR_NAME,  # TODO
                    "mode": "incrementing",  # TODO
                    "incrementing.column.name": "id",  # TODO
                    "table.whitelist": "user_info",  # TODO
                    "tasks.max": 1,
                    "connection.url": "jdbc:postgresql://localhost:5432/docker",
                    "connection.user": "postgres",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            
                },
            
        ),
    )

    # Ensure a healthy response was given
     # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    print("connector created successfully.")
    print("Use kafka-console-consumer and kafka-topics to see data!")


async def consume():
    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL, 
            "group.id": "0",
            "auto.offset.reset": "earliest",
        }

            )
    c.subscribe([CONNECTOR_NAME])
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


async def log_task():
    """Runs the log task"""
    consumer = asyncio.create_task(consume())
    configure_connector()
    await consumer


def run():
    """Runs the simulation"""
    try:
        asyncio.run(log_task())
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    run()
