# DataQualityFramework
Sample DQ Notebook 
# DataQualityFramework

This project contains a Data Quality checking framework using Apache Spark.

## Setup

1. Install Apache Spark and ensure it is set up correctly on your system.

2. Clone this repository and navigate to the project directory:
    ```sh
    git clone https://github.com/Ravi216/DataQualityFramework.git
    cd DataQualityFramework
    ```

3. Install the required Python packages:
    ```sh
    pip install pyspark
    ```

4. Update `data_quality_framework.py` to point to your data file:
    ```python
    # Load your data
    df = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)
    ```

5. Run the data quality checks:
    ```sh
    python data_quality_framework.py
