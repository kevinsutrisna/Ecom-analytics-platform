import json
import os
import io
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio
import sys
print("Consumer Started", flush=True)
sys.stdout.flush()

KAFKA_BROKER = "kafka:9092"
MINIO_ENDPOINT = "minio:9000"
MINIO_BUCKET = "raw-events"

consumer = KafkaConsumer(
    "clicks",
    "orders",
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=False,
)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

for message in consumer:
    event = message.value
    topic = message.topic
    event_date = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"{topic}/date={event_date}/{message.offset}.json"

    data = json.dumps(event).encode("utf-8")

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=filename,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )

    print(f"Saved {topic} event to MinIO: {filename}")
