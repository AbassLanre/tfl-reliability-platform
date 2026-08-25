import json
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092"})

def on_delivery(err, msg):
    if err:
        print(f"FAILED: {err}")
    else:
        print(f"delivered to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

event = {"schema_version": 1, "event_type": "smoke_test", "line_id": "victoria"}

p.produce(
    "tfl.arrivals",
    key="victoria",
    value=json.dumps(event),
    callback=on_delivery,
)
p.flush()