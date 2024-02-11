import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    # Define the common URL path
    # url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    import urllib.request
    url_path = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-'
    # urllib.request.urlretrieve(url, 'green_tripdata_2020-10.csv.gz')
    # List of month strings
    months = ["10", "11", "12"]

    # Initialize an empty list to store dataframes
    dataframes = []

#Loop through the months and read the CSV files
    for month in months:
        url = f"{url_path}{month}.csv.gz"
        print(url)
        # Define the dtypes for each column as per your requirements
        taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }
        parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        # Read the CSV file into a dataframe
        df = pd.read_csv(url, dtype=taxi_dtypes, parse_dates=parse_dates)
        dataframes.append(df)
        
    data = pd.concat(dataframes, ignore_index=True)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
