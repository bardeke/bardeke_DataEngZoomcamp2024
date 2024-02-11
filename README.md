# bardeke_DataEngZoomcamp2024

hw3 sql queries and notes
-- create external table from parquet file in bucket
CREATE OR REPLACE EXTERNAL TABLE `chromatic-fx-411315.ny_taxi.external_green_tripdata_fromparquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-barbara-dekeyser/greentaxi_22_hw3_parquet.parquet']
);
-- this does not get the times as timestamp even when using the FAQ solution using pyarrow - still came through as integer + casting them afterwords as dates resulted in errors as well


CREATE OR REPLACE EXTERNAL TABLE `chromatic-fx-411315.ny_taxi.external_green_tripdata_fromcsv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://mage-zoomcamp-barbara-dekeyser/greentaxi_22_hw3_csv.csv']

);
-- this imports store_and_fwd_flag as boolean even though there is a 'None' value in the data > results in errors


-- define schema explicitly
CREATE OR REPLACE EXTERNAL TABLE `chromatic-fx-411315.ny_taxi.external_green_tripdata_fromcsv_wschema` (
    int64_field_0 INTEGER,
    VendorID INTEGER,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    RatecodeID INTEGER,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count INTEGER,
    trip_distance FLOAT64,  -- Corrected data type
    fare_amount FLOAT64,  -- Corrected data type
    extra FLOAT64,  -- Corrected data type
    mta_tax FLOAT64,  -- Corrected data type
    tip_amount FLOAT64,  -- Corrected data type
    tolls_amount FLOAT64,  -- Corrected data type
    ehail_fee STRING,
    improvement_surcharge FLOAT64,  -- Corrected data type
    total_amount FLOAT64,  -- Corrected data type
    payment_type INTEGER,
    trip_type FLOAT64,  -- Corrected data type
    congestion_surcharge FLOAT64  -- Corrected data type
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://mage-zoomcamp-barbara-dekeyser/greentaxi_22_hw3_csv.csv'],
  skip_leading_rows = 1  -- If the first row contains headers
);


-- investigate problem with store_and_fwd_flag
SELECT distinct(store_and_fwd_flag)
FROM `chromatic-fx-411315.ny_taxi.external_green_tripdata_fromcsv`;
-- get error if I don't specify the schema explicitly

SELECT distinct(store_and_fwd_flag)
FROM `chromatic-fx-411315.ny_taxi.external_green_tripdata_fromparquet`;
-- get N, Y, None

-- investigate error with store_and_fwd_flag - only when I use csv
select distinct(store_and_fwd_flag) from `ny_taxi.external_green_tripdata_fromparquet`;
select distinct(store_and_fwd_flag) from `ny_taxi.external_green_tripdata_fromcsv`;

-- investigate why dates cannot be cast - only when I use parquet - solution in FAQ did not work - still came through as integer despite using pyarrow
select distinct(lpep_pickup_datetime) from `ny_taxi.external_green_tripdata_fromparquet`;

-- investigate errors when trying to partitioned table
-- parquet - no dates recognized even though did what FAQ said use pyarrow and coerce 'us'
CREATE OR REPLACE TABLE chromatic-fx-411315.ny_taxi.green_tripdata_hw3_non_partitioned_fromparquet AS
SELECT CAST(lpep_pickup_datetime AS TIMESTAMP) AS lpep_pickup_datetime, t.* FROM chromatic-fx-411315.ny_taxi.external_green_tripdata_fromparquet t;
-- did not work  even though csv dates are fine and I was able to parse in loading stage - User
-- Invalid cast from INT64 to DATE at [2:13]

-- csv - store_and_fwd_flag - was cast as boolean importing into bigquery even though it was specified as str in data loading stage in Mage

-- q1
select count(*) from `ny_taxi.external_green_tripdata_fromparquet`;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE chromatic-fx-411315.ny_taxi.green_tripdata_hw3_fromcsv_non_partitioned AS
SELECT * FROM chromatic-fx-411315.ny_taxi.external_green_tripdata_fromcsv_wschema;


-- q3
select count(*) from ny_taxi.green_tripdata_hw3_fromcsv_non_partitioned where fare_amount = 0;

-- q4
select count(distinct(PUlocationID)) from ny_taxi.green_tripdata_hw3_fromcsv_non_partitioned;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE chromatic-fx-411315.ny_taxi.green_tripdata_hw3_fromcsv_partitioned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM chromatic-fx-411315.ny_taxi.external_green_tripdata_fromcsv_wschema;


-- q5

SELECT DISTINCT PULocationID
FROM ny_taxi.green_tripdata_hw3_fromcsv_non_partitioned
WHERE lpep_pickup_datetime >= TIMESTAMP('2022-06-01') 
  AND lpep_pickup_datetime < TIMESTAMP('2022-07-01');

SELECT DISTINCT PULocationID
FROM ny_taxi.green_tripdata_hw3_fromcsv_partitioned
WHERE lpep_pickup_datetime >= TIMESTAMP('2022-06-01') 
  AND lpep_pickup_datetime < TIMESTAMP('2022-07-01');