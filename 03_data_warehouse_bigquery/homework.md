## Week 3 Homework

This homework uses the 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data found here:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.
Stop with loading the files into a bucket.

NOTE: You will need to use the PARQUET option files when creating an External Table

SETUP:
Create an external table using the Green Taxi Trip Records Data for 2022.
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).

First The External SQL table is created with specified SCHEMA to avoid datatype discrepancy

```SQL
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-2024-411721.ny_taxi.external_green_tripdata`
(vendorid INTEGER,
lpep_pickup_datetime DATETIME,
lpep_dropoff_datetime	DATETIME,
passenger_count	FLOAT64,
trip_distance	FLOAT64,
ratecodeid FLOAT64,
store_and_fwd_flag STRING,
pulocationid INTEGER,
dolocationid INTEGER,
payment_type	FLOAT64,
fare_amount	FLOAT64,
extra	FLOAT64,
mta_tax	FLOAT64,
tip_amount	FLOAT64,
tolls_amount	FLOAT64,
improvement_surcharge FLOAT64,
total_amount	FLOAT64,
congestion_surcharge FLOAT64)
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc-data-lake-cloudstorage-bucketname/green/green_tripdata_2022-*.parquet']
);

-- Check External Green Dataset
SELECT * FROM dtc-de-course-2024-411721.ny_taxi.external_green_tripdata limit 10;

```

## Question 1. What is count of records for the 2022 Green Taxi Data?

```SELECT COUNT(*) FROM dtc-de-course-2024-411721.ny_taxi.external_green_tripdata;```


## Question 2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

First, Creating a Materialized table.

```SQL
CREATE OR REPLACE TABLE dtc-de-course-2024-411721.ny_taxi.green_tripdata AS
SELECT * FROM dtc-de-course-2024-411721.ny_taxi.external_green_tripdata;
```

Then, counting the number of distinct PULocationIDs.

```SQL
  SELECT COUNT(DISTINCT(pulocationid)) FROM dtc-de-course-2024-411721.ny_taxi.external_green_tripdata;

  SELECT COUNT(DISTINCT(pulocationid)) FROM dtc-de-course-2024-411721.ny_taxi.green_tripdata;
```

## Question 3. How many records have a fare_amount of 0?

```SQL
SELECT COUNT(*) FROM dtc-de-course-2024-411721.ny_taxi.green_tripdata WHERE fare_amount=0;
```

## Question 4. What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

```SQL
CREATE OR REPLACE TABLE dtc-de-course-2024-411721.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY pulocationid AS
SELECT * FROM dtc-de-course-2024-411721.ny_taxi.external_green_tripdata;
```

## Question 5. Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive) Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

```SQL
-- Materialized Table : Query scans 12.82 MB
  SELECT count(DISTINCT(pulocationid)) as distinctPUID FROM dtc-de-course-2024-411721.ny_taxi.green_tripdata
  WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Partitioned & Clustered Table : Query scans  1.12 MB
  SELECT count(DISTINCT(pulocationid)) as distinctPUID FROM dtc-de-course-2024-411721.ny_taxi.green_tripdata_partitoned_clustered
  WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```

## Question 6. Where is the data stored in the External Table you created?

The data is stored in GCP Bucket.

## Question 7. It is best practice in Big Query to always cluster your data

No, It is not. As it may not be efficient when the data size is less than 1 GB and query is filtered on the field other than the field used for clustered.

## Question 8. Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```SQL
SELECT COUNT(*) FROM dtc-de-course-2024-411721.ny_taxi.green_tripdata;
-- It says it will compute 0 B when run.
```