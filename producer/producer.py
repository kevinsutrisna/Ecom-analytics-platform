import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import sys
print("Producer Started", flush=True)
sys.stdout.flush()

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

users = [f"user_{i}" for i in range(1, 6)]
products = [f"product_{i}" for i in range(1, 6)]

def send_click():
    user_id = random.choice(users)

    event = {
        "event_type": "click",
        "user_id": user_id,
        "product_id": random.choice(products),
        "page": random.choice(["home", "search", "product"]),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send(
        topic="clicks",
        key=user_id,
        value=event
    )

def send_order():
    user_id = random.choice(users)

    event = {
        "event_type": "order",
        "order_id": f"order_{random.randint(1000,9999)}",
        "user_id": user_id,
        "total_amount": random.randint(10000, 500000),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send(
        topic="orders",
        key=user_id,
        value=event
    )

if __name__ == "__main__":
    while True:
        send_click()
        if random.random() > 0.7:
            send_order()
        time.sleep(1)