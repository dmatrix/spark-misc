"""
ChatGPT, CodePilot, and docs used to generate code sample for testing
"""

import sys
sys.path.append('.')

from src.py.sc.utils.print_utils import print_header, print_seperator
import pyspark.pandas as ps
import pandas as pd
from pyspark.sql import SparkSession
from dateutil.relativedelta import relativedelta
from datetime import datetime
import random

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("PySpark Pandas Example 1") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Set pandas-on-Spark options to use the Spark backend
    ps.set_option("compute.default_index_type", "distributed")

    # Generate sample data
    rows = 10000
    columns = ["col_" + str(i) for i in range(15)]

    data = pd.DataFrame({
        "col_0": range(rows),
        "col_1": [random.choice(["A", "B", "C"]) for _ in range(rows)],
        "col_2": [random.uniform(10, 100) for _ in range(rows)],
        "col_3": [datetime.now() - relativedelta(days=random.randint(0, 1000)) for _ in range(rows)],
        **{f"col_{j}": [random.randint(1, 100) for _ in range(rows)] for j in range(4, 15)}
    })

    # Create a pandas-on-Spark DataFrame
    df = ps.from_pandas(data)
    print(f"++++Ensure it is PySpark Pandas datatype++++:{type(df)}")

    # Example 1: `select` (column selection)
    selected_df = df[["col_0", "col_1", "col_2"]]
    print("Selected Columns:")
    print(selected_df.head())

    # Example 2: `groupby` (grouping and aggregation)
    grouped_df = df.groupby("col_1")["col_2"].mean()
    print("Grouped by col_1 with mean of col_2:")
    print(grouped_df)

    # Example 3: UDF (uppercase a string column)
    def capitalize_first_letter(val):
        return val.capitalize() if val else None

    df["col_1_capitalized"] = df["col_1"].apply(capitalize_first_letter)
    print("With UDF applied:")
    print(df[["col_1", "col_1_capitalized"]].head())

    # Example 4: `merge` (joining two DataFrames)
    df_small = df.iloc[:100]  # Create a smaller DataFrame to join
    merged_df = df.merge(df_small, on="col_0", how="inner")
    print("Merged DataFrame:")
    print(merged_df.head())

    # Example 5: Using `python-dateutil` for dynamic dates
    date_example = datetime.now() - relativedelta(months=6)
    print(f"Date 6 months ago: {date_example}")