import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-console',
    value_deserializer=ride_deserializer,
    consumer_timeout_ms=5000 
)

print(f"Listening to {topic_name}...")

count = 0
distance_more_than_5 = 0
for message in consumer:
    ride = message.value
    if ride.trip_distance > 5:
        distance_more_than_5 += 1
    count += 1
    if count % 100 == 0:
        print(f"Received {count} messages so far...")

print(f"Total messages received: {count}")
print(f"Messages with distance > 5: {distance_more_than_5}")
    

consumer.close()