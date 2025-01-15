import sys
sys.path.append('.')
import os

import warnings
warnings.filterwarnings("ignore")

from src.py.sc.utils.spark_session_cls import SparkConnectSession


if __name__ == "__main__":
    cluster_id = os.environ.get("clusterID")
    assert cluster_id
    spark = SparkConnectSession(remote=None, mode="dbconnect",
                                app_name="PySpark DBConnect Test").get()
    
    # Ensure we are connected vis Spark Connect Session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++ Making sure it's using SparkConnect session:{spark} +++++")

    df = spark.range(10)
    print(df.show())