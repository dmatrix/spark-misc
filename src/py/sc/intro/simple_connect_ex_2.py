""" A simple Spark Connect application that reads a file and counts characters"""
import os
import sys
sys.path.append('.')

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession


README_FILE = None

if __name__ == "__main__":

    spark = None
    # Create a new session with Spark Connect mode={"dbconnect", "connect", "classic"}
    if len(sys.argv) <= 1:
        args = ["dbconnec", "classic", "connect"]
        print(f"Command line must be one of these values: {args}")
        sys.exit(1)  

    mode = sys.argv[1]
    print(f"++++ Using Spark Connect mode: {mode}")
    
    # create Spark Connect type based on type of SparkSession you want
    if mode == "dbconnect":
        cluster_id = os.environ.get("clusterID")
        assert cluster_id
        spark = spark = DatabrckSparkSession().get()
        README_FILE = "dbfs:/databricks-datasets/SPARK_README.md"
    else:
        spark = SparkConnectSession(remote="local[*]", mode=mode,
                                app_name="PySpark DataFrame Simple Example 2").get()
        README_FILE = "/Users/jules/spark_homes/current/README.md"
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")
    
    print(f"+++++Reading DBFS file: {README_FILE}+++++ ...")
    log_data = spark.read.text(README_FILE).cache()

    # Count character and word spark occurances
    num_of_a = log_data.filter(log_data.value.contains('a')).count()
    num_of_b = log_data.filter(log_data.value.contains('b')).count()

    spark_count = log_data.filter(log_data.value.contains("spark")).count()

    # print values out
    print(f"Character 'a' occurances: {num_of_a}")
    print(f"Character 'b' occurances: {num_of_b}")
    print(f"Word 'spark' occurances : {spark_count}")
    
    spark.stop()

