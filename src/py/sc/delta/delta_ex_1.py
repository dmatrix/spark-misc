"""
Test to create a simple Delta Table. Some code or partial code
was generated from ChatGPT, CoPilot, and docs samples.
"""

import os
import sys

from delta import DeltaTable
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator

import warnings
warnings.filterwarnings("ignore")

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
                                app_name="Delta Example 1").get()


    DELTA_TABLE_NAME = "spark_authors_delta_table"
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
    spark.sql(f"DROP TABLE IF EXISTS {DELTA_TABLE_NAME};")
    df.write.format("delta").saveAsTable(DELTA_TABLE_NAME)
    
    spark.sql(f"desc formatted {DELTA_TABLE_NAME}").show(truncate = False)

    # read back from the Author's Delta Table
    # read the table back into a PySpark DataFrame
    print_header("READ DATA FROM THE DELTA TABLE:")
    df_table = spark.read.table(DELTA_TABLE_NAME)
    print(df_table.show())