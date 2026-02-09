--Create an external table using the Yellow Taxi Trip Records.
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp-486906.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp-486906-raw-data/yellow_tripdata_2024-*.parquet']
);

--Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).
CREATE OR REPLACE TABLE `dezoomcamp-486906.ny_taxi.yellow_tripdata_non_partitioned` AS
SELECT * FROM `dezoomcamp-486906.ny_taxi.external_yellow_tripdata`;

--Check the count of records for the 2024 Yellow Taxi Data
SELECT COUNT(*) FROM `dezoomcamp-486906.ny_taxi.yellow_tripdata_non_partitioned`;

--Count the distinct number of PULocationIDs on external table
SELECT COUNT(DISTINCT PULocationID) 
FROM `dezoomcamp-486906.ny_taxi.external_yellow_tripdata`;

--Count the distinct number of PULocationIDs on native table
SELECT COUNT(DISTINCT PULocationID) 
FROM `dezoomcamp-486906.ny_taxi.yellow_tripdata_non_partitioned`;

--Retrieve the PULocationID from the native table
SELECT PULocationID 
FROM `dezoomcamp-486906.ny_taxi.yellow_tripdata_non_partitioned`;

--Retrieve the PULocationID and DOLocationID from the native table
SELECT PULocationID, DOLocationID 
FROM `dezoomcamp-486906.ny_taxi.yellow_tripdata_non_partitioned`;

--Check the count of records have a fare_amount of 0
SELECT COUNT(*)
FROM `dezoomcamp-486906.ny_taxi.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0;

--Partition by tpep_dropoff_datetime and Cluster on VendorID
CREATE OR REPLACE TABLE `dezoomcamp-486906.ny_taxi.yellow_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `dezoomcamp-486906.ny_taxi.external_yellow_tripdata`;

--Retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 from non partitioned table
SELECT DISTINCT VendorID
FROM `dezoomcamp-486906.ny_taxi.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

--Retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 from partitioned table
SELECT DISTINCT VendorID
FROM `dezoomcamp-486906.ny_taxi.yellow_tripdata_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

--Check number of rows of native table
SELECT count(*) 
FROM `dezoomcamp-486906.ny_taxi.yellow_tripdata_non_partitioned`;