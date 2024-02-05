if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import re

@transformer
def transform(data, *args, **kwargs):
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

    # Filter data based on conditions
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    # # Convert 'lpep_pickup_datetime' column to datetime if needed
    # data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])

    # Extract date from 'lpep_pickup_datetime' and assign it to 'lpep_pickup_date' column
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    #Rename Columns in Camel Case to Snake Case
    
    def camel_to_snake(column_names):
        """
        Convert CamelCase to snake_case using regular expressions.
        """
        snake_case_names = []
        for name in column_names:
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            snake_case_names.append(re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower())
        return snake_case_names
    data.columns = camel_to_snake(data.columns)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
