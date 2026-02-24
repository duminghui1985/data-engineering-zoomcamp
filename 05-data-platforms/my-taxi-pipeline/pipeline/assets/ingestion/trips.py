"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append
@bruin"""

import os
import json
import pandas as pd

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    # Generate list of months between start and end dates
    # Fetch parquet files from:
    # https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet
    
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)

    month_range = pd.date_range(
        start=start_dt.replace(day=1), 
        end=end_dt, 
        freq='MS'
    )

    all_dfs = []

    for taxi_type in taxi_types:
        for month_dt in month_range:
            year = month_dt.year
            month = f"{month_dt.month:02d}"
            
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
            
            try:
                print(f"Fetching data for {taxi_type} - {year}-{month}...")
                df = pd.read_parquet(url)
                
                rename_map = {
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                    'PULocationID': 'pickup_location_id',
                    'DOLocationID': 'dropoff_location_id',
                    'payment_type': 'payment_type'
                }
                df = df.rename(columns=rename_map)
                
                df['taxi_type'] = taxi_type
                
                target_columns = [
                    'pickup_datetime', 'dropoff_datetime', 
                    'pickup_location_id', 'dropoff_location_id', 
                    'fare_amount', 'payment_type', 'taxi_type'
                ]

                existing_cols = [c for c in target_columns if c in df.columns]
                df = df[existing_cols]

                mask = (df['pickup_datetime'] >= start_dt) & (df['pickup_datetime'] < end_dt)
                df = df[mask]

                if not df.empty:
                    all_dfs.append(df)
                    
            except Exception as e:
                print(f"Could not fetch {url}: {e}")

    if not all_dfs:
        return pd.DataFrame(columns=['pickup_datetime', 'dropoff_datetime', 'taxi_type'])

    final_dataframe = pd.concat(all_dfs, ignore_index=True)
    

    return final_dataframe