from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, BooleanType


import os
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, isnan, when
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce

from src.py.sc.utils.data_validation_cls  import DataValidation

def get_json_files(directory):
    import os
    """

    Get a list of all JSON files in the specified directory.
    Parameters:
    directory (str): The directory to search for JSON files.
    Returns:
    list: A list of paths to JSON files in the directory.
    
    """
    
    return [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.json')]

def merge_spark_dataframes(df_list):
    from functools import reduce
    """
    Merges a list of PySpark DataFrames into a single DataFrame.

    Parameters:
    df_list (list): A list of PySpark DataFrames to merge.
    Each DataFrame should have the same schema.
    The DataFrames will be merged using unionByName, which allows for different column orders.
   
    Returns:    

    DataFrame: A single DataFrame containing all rows from the input DataFrames.
    """
    return reduce(lambda df1, df2: df1.unionByName(df2), df_list)
    

if __name__ == "__main__":
    spark = None
    # Initialize Spark Session
    try:
        spark = SparkSession.builder \
            .appName("JSON Data Validation") \
            .getOrCreate()
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        sys.exit(1)
    
     # Create a DataFrame from the data with explicit schema
    schema = StructType([
        StructField("unique_id", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("date_of_birth", DateType(), False),
        StructField("age", IntegerType(), False),
        StructField("ssn", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("airline_of_choice", StringType(), True),
        StructField("mileage_flown", IntegerType(), False),
        StructField("fares_paid_to_date", DoubleType(), False),
        StructField("status", StringType(), False)
    ])
    
    # Read the JSON file with the defined schema

    # Get the list of JSON files in the directory
    # Get current working directory
    json_data_dir = os.getcwd() + "/data/json/"
    json_files = get_json_files(json_data_dir)
    if not json_files:
        print("No JSON files found in the directory.")
        sys.exit(1)
    print(f"Found JSON files: {json_files}")
    
    # Iterate over the JSON files and read them into a DataFrame
    # Read the JSON files into a DataFrame

    df_list = []
    read_options = {
        "mode": "DROPMALFORMED",
        "columnNameOfCorruptRecord": "corrupt_record"
    }

    for json_file in json_files:
        print(f"Reading JSON file: {json_file}")
        try:
            df = (spark.read.schema(schema)
                .options(**read_options)
                .json(json_file))
            df_list.append(df)
        except AnalysisException as e:
            print(f"Error reading {json_file}: {e}")
            continue

    # Iterate over the DataFrames and perform validation
    for df in df_list:
        data_validation = DataValidation(df)
    
        print("Starting to clean the JSON data...")
         # Perform data cleaning operations here
         # For example, you can drop rows with missing values

        duplicates = data_validation.drop_duplicate_records()
        missing = data_validation.drop_missing_values()
        negative_or_zero_mileage = data_validation.drop_negative_or_zero_mileage()
        data_validation.print_validation_report()

        print("JSON Data cleaning completed.")
        # Show the cleaned DataFrame
        data_validation.get_df().show(truncate=False)
        
    # Stop the Spark session
    if spark:
        spark.stop()

    print("+++++ Spark session stopped +++++")
   
       