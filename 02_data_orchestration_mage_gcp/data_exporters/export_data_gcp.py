from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq
import os
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# google cloud setup
os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/home/src/mageAgentHss.json"
bucket_name = "magedataorchestration"
project_id = "dtc-de-course-2024-411721"

table_name = "green_taxi_data"
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    #pyarrow
    table = pa.Table.from_pandas(df)

    #get the filesystem
    gcs = pa.fs.GcsFileSystem()

    #write to GCS
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
    
