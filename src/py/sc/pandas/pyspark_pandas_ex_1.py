"""
A combination ChatGPT, CodePilot, and docs used to generate code sample for testing
"""
import os 
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")  # Ignore warnings coming from Arrow optimizations. 
from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator
import pyspark.pandas as ps
import pandas as pd

from dateutil.relativedelta import relativedelta
from datetime import datetime
import random

if __name__ == "__main__":
    spark = None
    # Create a new session with Spark Connect mode={"dbconnect", "connect", "classic"}
    if len(sys.argv) <= 1:
        args = ["dbconnect", "classic", "connect"]
        print(f"Command line must be one of these values: {args}")
        sys.exit(1)  

    mode = sys.argv[1]
    print(f"++++ Using Spark Connect mode: {mode}")
    
    # create Spark Connect type based on type of SparkSession you want
    if mode == "dbconnect":
        cluster_id = os.environ.get("clusterID")
        assert cluster_id
        spark = spark = DatabrckSparkSession().get()
    else:
        spark = SparkConnectSession(remote="local[*]", mode=mode,
                                app_name="PySpark Pandas Example 1").get()
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Set pandas-on-Spark options to use the Spark backend
    ps.set_option("compute.default_index_type", "distributed")

    # Generate sample data
    rows = 17_000
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
    print_header("Selected Columns:")
    print(selected_df.head())
    print_seperator()

    # Example 2: `groupby` (grouping and aggregation)
    grouped_df = df.groupby("col_1")["col_2"].mean()
    print_header("Grouped by col_1 with mean of col_2:")
    print(grouped_df)
    print_seperator()

    # Example 3: UDF (uppercase a string column)
    def capitalize_first_letter(val):
        return val.capitalize() if val else None

    df["col_1_capitalized"] = df["col_1"].apply(capitalize_first_letter)
    print_header("With UDF applied:")
    print(df[["col_1", "col_1_capitalized"]].head())
    print_seperator()

    # Example 4: `merge` (joining two DataFrames)
    df_small = df.iloc[:100]  # Create a smaller DataFrame to join
    merged_df = df.merge(df_small, on="col_0", how="inner")
    print_header("Merged DataFrame:")
    print(merged_df.head())
    print_seperator()

    # Example 5: Using `python-dateutil` for dynamic dates
    date_example = datetime.now() - relativedelta(months=6)
    print(f"Date 6 months ago: {date_example}")