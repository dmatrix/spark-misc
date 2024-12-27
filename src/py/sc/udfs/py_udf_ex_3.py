import pandas as pd
from pyspark.sql.functions import udf
import sys
sys.path.append('.')
import warnings
warnings.filterwarnings("ignore")

from src.py.sc.utils.print_utils import print_seperator, print_header
from src.py.sc.utils.spark_session_cls import SparkConnectSession
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # Initialize Spark session
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()
    
    spark = SparkConnectSession(remote="local[*]",
                                app_name="PySpark UDF Debugging Example 1").get()
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Non UDF function
    def mult_by_2_udf(x: pd.Series) -> pd.Series:
        # Add a print statement
        print("Debugging output:", x) 
        print("Multipy each value by 2")
        return x * 2
    

    df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
    new_df = df.withColumn("2x",  mult_by_2_udf(df["x"]))
    print(new_df.show())