from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter



@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'mage-zoomcamp-barbara-dekeyser'
    object_key = 'greentaxi_22_hw3_parquet.parquet'

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Write the PyArrow Table to Parquet format
    pq.write_table(table, 'temporary_file.parquet', coerce_timestamps='us')

    # Initialize Google Cloud Storage client
    gcs = GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile))

    # Export the Parquet file to Google Cloud Storage
    gcs.export(
        df,
        bucket_name,
        object_key,
    )

    # Delete the temporary Parquet file
    os.remove('temporary_file.parquet')