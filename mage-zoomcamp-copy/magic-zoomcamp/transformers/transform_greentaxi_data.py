if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    # check zero passengers and zero trip distance
    print("rows with zero passengers : ", data['passenger_count'].isin([0]).sum())
    print("rows with zero trip distance : ", data['trip_distance'].isin([0]).sum())

    # eliminating zero passengers and zero tripdistance rows
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    
    # column names to snake case
    num_transformed_columns = sum(col.endswith('ID') for col in data.columns)
    print(f"Number of columns transformed: {num_transformed_columns}")

    # Rename columns from Camel Case to Snake Case for columns ending with "ID"
    
    data.columns = [col[:-2].lower() + '_id' if col.endswith('ID') else col for col in data.columns]

    print(data.columns)

    # add date column for partitioning
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # print unique vendor ids
    print("unique vendor ids : ",data['vendor_id'].unique())

    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    return data

# test = assertion - you can have more than one assertion
@test
def test_output(output, *args):
    """
    Template code for testing the output of the block.
    """
    # assert output is not None, 'The output is undefined'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'
    assert output['vendor_id'].isin(output['vendor_id'].unique()).all(), 'There are invalid vendor_ids'