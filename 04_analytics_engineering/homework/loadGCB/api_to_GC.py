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
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{service}_tripdata_{year}-{month}.csv.gz'
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # defining datatypes for fhv file
        taxi_dtypes={
            'dispatching_base_num': str,
            'PUlocationID':pd.Int64Dtype(),
            'DOlocationID':pd.Int64Dtype(),
            'SR_Flag': pd.Int64Dtype(),
            'Affiliated_base_number': str,
            'pickup_datetime': str,
            'dropOff_datetime': str
        }
        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip', dtype=taxi_dtypes)
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


# The following dataset is necessary to complete the Week 4 Homework Questions
web_to_gcs('2019', 'fhv')