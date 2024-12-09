"""
ChatGPT, CodePilot, and docs used to generate code sample for testing
"""

import sys
sys.path.append('.')

import pyspark.pandas as ps
from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import SparkSession
from IPython.display import display
import numpy as np
import pandas as pd
import random

if __name__ == "__main__":

    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("Pyspark Pandas/Numpy Example 1") 
                .getOrCreate())
    
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

    df = pd.DataFrame(data)

    print("Original DataFrame:")
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
    print(df)

    # 3. Apply NumPy universal functions (e.g., sqrt)
    df["C_sqrt"] = np.sqrt(df["C"])
    print("\nSquare root of column C:")
    print(df)

    # 4. Row-wise sum
    df["Row_Sum"] = df[["A", "B", "C"]].sum(axis=1)
    print("\nRow-wise sum (A + B + C):")
    print(df)

    # 5. Aggregations using NumPy
    col_mean = df[["A", "B", "C"]].mean()
    col_std = df[["A", "B", "C"]].std()

    print("\nColumn-wise mean:")
    print(col_mean)
    print("\nColumn-wise standard deviation:")
    print(col_std)

    # 6. Conditional selection with NumPy
    df["A_gt_50"] = df["A"] > 50  # Check if values in A are greater than 50
    print("\nConditional column (A > 50):")
    print(df)

    # # 7. NumPy `where` for conditional value assignment
    df["Category"] = np.where(df["A"] > 50, "High", "Low")
    print("\nCategory based on A > 50 (using np.where):")
    print(df)

    spark.stop()