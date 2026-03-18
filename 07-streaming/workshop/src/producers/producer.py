import dataclasses
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models import Ride, ride_from_row

# Download NYC yellow taxi trip data (first 1000 rows)
#url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
file = "src/producers/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
server = 'localhost:9092'
topic_name = 'green-trips'

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

#Read the parquet file into a DataFrame
df = pd.read_parquet(file, columns=columns)

#Drop rows with missing pickup/dropoff datetimes or trip distance
df = df.dropna(subset=['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'trip_distance'])

#trip distance must be greater than 0, otherwise it's not a valid trip
df = df[df['trip_distance'] > 0]

#Fill missing values and convert to correct types
df['passenger_count'] = df['passenger_count'].fillna(0).astype(int)
df['PULocationID'] = df['PULocationID'].fillna(0).astype(int)
df['DOLocationID'] = df['DOLocationID'].fillna(0).astype(int)
df['tip_amount'] = df['tip_amount'].fillna(0.0)
df['total_amount'] = df['total_amount'].fillna(0.0)

#Measure the time it takes to send the entire dataset and flush
t0 = time.time()

#send all rows to Kafka
count = 0
for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    count += 1
    if count % 100 == 0:
        print(f"Sent {count} messages so far...")
    time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')

producer.close()