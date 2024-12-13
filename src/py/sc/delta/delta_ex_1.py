"""
Test to create a simple Delta Table. Some code or partial code
was generated from ChatGPT, CoPilot, and docs samples.
"""
from pyspark.sql import SparkSession
import os
import sys
sys.path.append('.')
from src.py.sc.utils.print_utils import print_header, print_seperator
from src.py.sc.utils.spark_session_cls import SparkConnectSession

import warnings
warnings.filterwarnings("ignore")

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = SparkConnectSession(remote="sc://localhost", 
                                app_name="Delta Example 1").get()

    DELTA_TABLE_PATH = "/tmp/delta/spark_authors_delta_table"
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    columns = ["id", "name"]
    data = [(1, "jules"), (2, "denny"), (3, "brooke"), (4, "td")]
    df = spark.createDataFrame(data).toDF(*columns)
    print_header("SPARK AUTHORS DATAFRAME:")
    print(df.show())
    print_seperator(size=15)

    # Write the DataFrame as a Delta table
    print_header("CREATING AUTHORS DELTA TABLE:")
    df.write.format("delta").mode("overwrite").save(DELTA_TABLE_PATH)
    print(os.listdir(DELTA_TABLE_PATH))
    print_seperator(size=15)

    # read back from the Author's Delta Table
    # read the table back into a PySpark DataFrame
    print_header("READ DATA FROM THE DELTA TABLE:")
    df = spark.read.format("delta").load(DELTA_TABLE_PATH)
    print(df.show())