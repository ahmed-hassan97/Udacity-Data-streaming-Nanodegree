from dataclasses import dataclass
from dataclasses import asdict, dataclass, field
from faker import Faker
import random
import asyncio
from io import BytesIO
from fastavro import parse_schema, writer

faker = Faker()
@dataclass
class ClickAttribute:
    element: str = field(default_factory=lambda: random.choice(["div", "a", "button"]))
    content: str = field(default_factory=faker.bs)

    @classmethod
    def attributes(self):
        return {faker.uri_page(): ClickAttribute() for _ in range(random.randint(1, 5))}

@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))
    attributes: dict = field(default_factory=ClickAttribute.attributes)

    schema = parse_schema(
        {
            "type": "record",
            "name": "click_event",
            "namespace": "com.udacity.lesson3.exercise2",
            "fields": [
                {"name": "email", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "uri", "type": "string"},
                {"name": "number", "type": "int"},
                {"name": "attributes", "type": {
                        "type": "map",
                        "values": {
                            "type": "record",
                            "name": "attribute",
                            "fields": [
                                {"name": "element", "type": "string"},
                                {"name": "content", "type": "string"},
                            ],
                        },
                    },
                },
            ],
        }
    )

    def serialize(self):
        """Serializes the ClickEvent for sending to Kafka"""
        out = BytesIO()
        writer(out, ClickEvent.schema, [asdict(self)])
        return out.getvalue()
    
async def generate_data():
    while True:
        ClickEvent().serialize()
        await asyncio.sleep(1.0)

if __name__ == '__main__':
    generate_data()
