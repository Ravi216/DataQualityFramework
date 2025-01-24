# data_quality_rules.py

from pyspark.sql.functions import col

def check_null_values(df, columns):
    """
    Check for null values in specified columns.
    """
    for column in columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"Column {column} has {null_count} null values.")
        else:
            print(f"Column {column} has no null values.")

def check_data_types(df, column_types):
    """
    Check if columns have the correct data types.
    """
    for column, expected_type in column_types.items():
        actual_type = df.schema[column].dataType
        if actual_type != expected_type:
            print(f"Column {column} has incorrect data type. Expected: {expected_type}, Got: {actual_type}")
        else:
            print(f"Column {column} has correct data type.")

def check_value_ranges(df, column_ranges):
    """
    Check if column values fall within specified ranges.
    """
    for column, (min_value, max_value) in column_ranges.items():
        out_of_range_count = df.filter((col(column) < min_value) | (col(column) > max_value)).count()
        if out_of_range_count > 0:
            print(f"Column {column} has {out_of_range_count} values out of range ({min_value}, {max_value}).")
        else:
            print(f"Column {column} has all values within range ({min_value}, {max_value}).")
