import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """


    # Initialize an empty list to store DataFrames
    dfs = []
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

# Iterate over months from 01 to 12
    for month in range(1, 13):
    # Format the URL for the current month
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month:02d}.parquet"
    
        # Read the parquet file into a DataFrame and append it to the list
        df = pd.read_parquet(url)
        dfs.append(df)

    # Concatenate all DataFrames into one
    merged_df = pd.concat(dfs, ignore_index=True)

    merged_df = merged_df.astype({
    'VendorID': pd.Int64Dtype(),
    'passenger_count': pd.Int64Dtype(),
    'trip_distance': float,
    'RatecodeID': pd.Int64Dtype(),
    'store_and_fwd_flag': str,
    'PULocationID': pd.Int64Dtype(),
    'DOLocationID': pd.Int64Dtype(),
    'payment_type': pd.Int64Dtype(),
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'tip_amount': float,
    'tolls_amount': float,
    'improvement_surcharge': float,
    'total_amount': float,
    'congestion_surcharge': float
    })

    # Parse dates
    merged_df['lpep_pickup_datetime'] = pd.to_datetime(merged_df['lpep_pickup_datetime'])
    merged_df['lpep_dropoff_datetime'] = pd.to_datetime(merged_df['lpep_dropoff_datetime'])

    return merged_df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

