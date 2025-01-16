"""
ChatGPT, CodePilot, and docs used to generate partial code sample for testing
"""
import os
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")  # Ignore warnings coming from Arrow optimizations. 
from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession

import pyspark.pandas as ps
from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import SparkSession
from IPython.display import display
import numpy as np
import pandas as pd
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
                                app_name="Pyspark Pandas/Numpy Example 2").get()
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Set default options for PySpark Pandas
    ps.set_option("compute.default_index_type", "distributed")
    rows = 100_000
    # Generate a sample PySpark Pandas DataFrame
    data = {
        "A": [random.randint(1, 100) for _ in range(rows)],
        "B": [random.randint(1, 100) for _ in range(rows)],
        "C": [random.uniform(1.0, 100.0) for _ in range(rows)],
    }

    # df = pd.DataFrame(data)
    # Create a PySpark Pandas DataFrame
    df = ps.DataFrame(data)
    ps.set_option('compute.ops_on_diff_frames', True)

    print("Original Pyspark Pandas DataFrame:")
    print(f"++++Ensure it is PySpark Pandas datatype++++:{type(df)}")
    print("Generated Main DataFrame with 100,000 rows:")
    # Display basic info
    print("DataFrame info:")
    print(df.head())

    # NumPy-style operations on PySpark Pandas DataFrame
    # 1. Element-wise addition
    df["A_plus_B"] = df["A"] + df["B"]
    print("\nElement-wise addition (A + B):")
    print(df)

    # 2. Element-wise multiplication
    df["B_times_C"] = df["B"] * df["C"]
    print("\nElement-wise multiplication (B * C):")
    print(df.head())

    # 3. Apply NumPy universal functions (e.g., sqrt)
    df["C_sqrt"] = np.sqrt(df["C"])
    print("\nSquare root of column C:")
    print(df.head())

    # 4. Row-wise sum
    print("\nRow-wise sum (A + B + C):")
    df["Row_Sum"] = df[["A", "B", "C"]].sum(axis=1)
    print(df.head())

    # 5. Aggregations using NumPy
    col_mean = df[["A", "B", "C"]].mean()
    print("\nColumn-wise mean:")
    print(col_mean)
    print("\nColumn-wise standard deviation:")
    col_std = df[["A", "B", "C"]].std()
    print(col_std)

    # 6. Conditional selection with NumPy
    print("\nConditional column (A > 50):")
    df["A_gt_50"] = df["A"] > 50  # Check if values in A are greater than 50
    print(df.head())

    # 7. NumPy `where` for conditional value assignment (not working as
    # PySpark API not yet implemented)
    # print("\nCategory based on A > 50 (using np.where):")
    # df["Category"] = np.where(df["A"] > 50, "High", "Low")
    # print(df.head())

    spark.stop()