"""
A combination ChatGPT, CodePilot, and docs used to generate code sample for testing
"""

import sys
sys.path.append('.')

from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import SparkSession
from IPython.display import display
import pandas as pd
import pyspark.pandas as ps
import numpy as np

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("local[*]")
                .appName("Pyspark Pandas Example 3") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")


    # Create a DataFrame with 1 million rows and 25 columns of random numeric data
    np.random.seed(42)  # For reproducibility
    rows = 100_000
    columns = 25
    data = np.random.rand(rows, columns)
    df = pd.DataFrame(data, columns=[f"Col_{i}" for i in range(1, columns + 1)])

    # Add a categorical column for groupby operations
    df["Group"] = np.random.choice(["A", "B", "C", "D"], size=rows)

    # Convert to Spark PySpark Pandas DataFrame
    df = ps.DataFrame(df)
    print(f"++++Ensure it is PySpark Pandas datatype++++:{type(df)}")
    print_header("Generated Main DataFrame with 100,000 rows:")
    # Display basic info
    print("DataFrame info:")
    print(df.head())
    print_seperator()

    # Select specific columns
    operation_str = "BASIC DATAFERAME OPERATION:SELECT"
    print_header(operation_str)
    selected_columns = df[["Col_1", "Col_5", "Group"]]
    print_header("Selected columns:")
    print(selected_columns.head())
    print_seperator()

    operation_str = "BASIC DATAFERAME OPERATION:GROUPBY"
    print_header(operation_str)
    # Group by 'Group' and calculate the mean of other columns
    grouped_mean = df.groupby("Group").mean()
    print("\nGroup-wise mean:")
    print(grouped_mean.head())
    print_seperator()

    # Sort the DataFrame by 'Col_1' in descending order
    operation_str = "BASIC DATAFERAME OPERATION:SORT"
    print_header(operation_str)
    sorted_df = df.sort_values(by="Col_1", ascending=False)
    print("\nTop 5 rows sorted by 'Col_1':")
    print(sorted_df.head())

    # Aggregate data by 'Group' with multiple functions
    operation_str = "BASIC DATAFERAME OPERATION:AGGREGATION"
    print_header(operation_str)
    aggregated_data = df.groupby("Group").agg(
        {
            "Col_1": ["mean", "max"],
            "Col_2": ["sum"],
            "Col_3": ["min"],
        }
    )
    print("\nAggregated data:")
    print(aggregated_data)
    print_seperator()

    # Filter rows where 'Col_1' > 0.9 and 'Col_2' < 0.1
    operation_str = "BASIC DATAFERAME OPERATION:FILTER"
    print_header(operation_str)
    filtered_df = df[(df["Col_1"] > 0.9) & (df["Col_2"] < 0.1)]
    print("\nFiltered rows where 'Col_1' > 0.9 and 'Col_2' < 0.1:")
    print(filtered_df.head())
    print_seperator()