## Week 3 Homework

This exercise will be using the 2022 Green Taxi Trip Record Parquet Files from the New York City Taxi Data found here:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.
Stop with loading the files into a bucket.

NOTE: You will need to use the PARQUET option files when creating an External Table

SETUP:
Create an external table using the Green Taxi Trip Records Data for 2022.
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).

After setting up the Google Cloud Credentials.
```bash
    gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
```

We push data to GCP Cloud Storage which are partioned by month.

```python
    import io
    import os
    import requests
    import pandas as pd
    import pyarrow.parquet as pq
    from google.cloud import storage

    # switch out the bucketname
    BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-cloudstorage-bucketname")
    print(BUCKET)

    def upload_to_gcs(bucket, object_name, local_file):
        """
        Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
        """
        # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
        # # (Ref: https://github.com/googleapis/python-storage/issues/74)
        storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
        storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)

    def web_to_gcs(year, service):
        for i in range(12):
            # sets the month part of the file_name string
            month = '0'+str(i+1)
            month = month[-2:]

            # csv file_name
            file_name = f"{service}_tripdata_{year}-{month}.parquet"
            # request url for week 3 homework
            request_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month}.parquet'
            print(request_url)
            #request_url = f"{init_url}{service}/{file_name}"
            r = requests.get(request_url)
            open(file_name, 'wb').write(r.content)
            print(f"Local: {file_name}")

            df = pq.read_table(file_name)
            #df.to_parquet(file_name, engine='pyarrow')
            print(f"Parquet: {file_name}")
            # upload it to gcs 
            upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
            print(f"GCS: {service}/{file_name}")

    # The following two datasets are used in the Week 3 Video Modules
    web_to_gcs('2019', 'yellow')
    web_to_gcs('2020', 'yellow')

    # The following dataset is necessary to complete the Week 3 Homework Questions
    web_to_gcs('2022', 'green')
```

The Exercise is repeated as shown in the Zoomcamp as:

```SQL
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-2024-411721.ny_taxi.external_yellow_tripdata`
(vendorid INTEGER,
tpep_pickup_datetime DATETIME,
tpep_dropoff_datetime	DATETIME,
passenger_count	FLOAT64,
trip_distance	FLOAT64,
ratecodeid FLOAT64,
store_and_fwd_flag STRING,
pulocationid INTEGER,
dolocationid INTEGER,
payment_type	INTEGER,
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
  uris = ['gs://dtc-data-lake-cloudstorage-bucketname/yellow/yellow_tripdata_2019-*.parquet', 'gs://dtc-data-lake-cloudstorage-bucketname/yellow/yellow_tripdata_2020-*.parquet']
);

-- Check yello trip data
SELECT * FROM dtc-de-course-2024-411721.ny_taxi.external_yellow_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-course-2024-411721.ny_taxi.yellow_tripdata_non_partitoned AS
SELECT * FROM dtc-de-course-2024-411721.ny_taxi.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-course-2024-411721.ny_taxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM dtc-de-course-2024-411721.ny_taxi.external_yellow_tripdata;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM dtc-de-course-2024-411721.ny_taxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM dtc-de-course-2024-411721.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE dtc-de-course-2024-411721.ny_taxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM dtc-de-course-2024-411721.ny_taxi.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM dtc-de-course-2024-411721.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM dtc-de-course-2024-411721.ny_taxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

```