import sys
sys.path.append('.')

from src.py.sc.utils.print_utils import print_header, print_seperator

from pyspark.sql import SparkSession
from IPython.display import display
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName('SqlExam[le') 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Creating Basic objects
    operation_str = "BASIC OBJECT CREATION:Series"
    print_header(operation_str)
    # Create a simple series
    s = pd.Series([1, 3, 5, np.nan, 6, 8])
    print(s)

    # Create a pandas DataFrame
    operation_str = "BASIC OBJECT CREATION:DataFrames"
    print_header(operation_str)
    dates = pd.date_range("20241121", periods=6)
    df_1 = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list("ABCD"))
    display(df_1)

    # Creating a DataFrame from a dictionary, where keys are columns and values are 
    # key's values of various types.
    df_2 = pd.DataFrame({"A": 1.0,
        "B": pd.Timestamp("20130102"),
        "C": pd.Series(1, index=list(range(4)), dtype="float32"),
        "D": np.array([3] * 4, dtype="int32"),
        "E": pd.Categorical(["test", "train", "test", "train"]),
        "F": "foo",
    })
    print_seperator(size=8)
    print(df_2.dtypes)
    print_seperator(size=8)
    display(df_2)

    # Viewing Data
    operation_str = "VIEWING DATA: DATAFRAMES"
    print_header(operation_str)
    print(df_1.head())
    print_seperator(size=8)
    print(df_1.columns)
    print_seperator(size=8)
    print(df_1.to_numpy())

    # selecting data
    operation_str = "SELECTING DATA: GETITEM"
    print_header(operation_str)
    print(df_1.A)
    print_seperator()
    print(df_1["A"])
    # selecting all rows : with desired columns
    print_seperator()
    print(df_1.loc[:, ["A", "C"]])
    # boolean indexing; select rows where df_1 A > 0
    print_seperator()
    print(df_1[df_1["A"] > 0])

    # User defined functions
    # DataFrame.agg() and DataFrame.transform() applies a 
    # user defined function that reduces or broadcasts its result respectively.
    operation_str = "USER DEFINED FUNCTIONS: UDF"
    print_header(operation_str)
    df_3 = df_1.agg(lambda x: np.mean(x) * 5.6)
    print(df_3.head())
    print_seperator()
    print(df_1.transform(lambda x: x * -0.2))

    # Groupby operations
    operation_str


