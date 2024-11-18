from pyspark.sql import SparkSession
from IPython.display import display
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.pandas as ps
import numpy as np

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    s = ps.Series([1, 3, 5, np.nan, 6, 8])
    print(s)
    ps_df = ps.DataFrame({'a': [1, 2, 3, 4, 5, 6],
                         'b': [100, 200, 300, 400, 500, 600],
                         'c': ["one", "two", "three", "four", "five", "six"]},
                         index=[10, 20, 30, 40, 50, 60])

display(ps_df)