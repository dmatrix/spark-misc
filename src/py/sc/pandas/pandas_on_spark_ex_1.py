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

    s = pd.Series([1, 3, 5, np.nan, 6, 8])
    print(s)
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6],
                         'b': [100, 200, 300, 400, 500, 600],
                         'c': ["one", "two", "three", "four", "five", "six"]},
                         index=[10, 20, 30, 40, 50, 60])

display(df)
spark.stop()