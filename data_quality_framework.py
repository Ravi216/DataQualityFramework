# data_quality_framework.py

from pyspark.sql import SparkSession
from data_quality_rules import check_null_values, check_data_types, check_value_ranges
from pyspark.sql.types import IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Quality Framework") \
    .getOrCreate()

# Load your data
df = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)

# Define your data quality rules
columns_to_check_nulls = ["column1", "column2"]
column_types = {
    "column1": IntegerType(),
    "column2": StringType()
}
column_ranges = {
    "column1": (0, 100)
}

# Perform data quality checks
check_null_values(df, columns_to_check_nulls)
check_data_types(df, column_types)
check_value_ranges(df, column_ranges)

# Stop the Spark session
spark.stop()
