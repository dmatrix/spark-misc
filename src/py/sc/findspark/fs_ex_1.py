"""
Test to find spark home, after unsetting the SPARK_HOME environment variable
"""
import os
import random
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator

import findspark
from faker import Faker

if __name__ == "__main__":
    spark_home = os.getenv("SPARK_HOME")
    print(f"Before findspark() init: {spark_home}")
    os.environ["SPARK_HOME"] = "" if spark_home else spark_home
    spark_home = os.getenv("SPARK_HOME")
    print(f"Setting spark_home to None and let findspark() find it: {spark_home}")
    spark_home = os.getenv("SPARK_HOME")
    print(f"After findspark.init() SPARK_HOME: {spark_home}")
    findspark.init()

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
                                app_name="Find Spark Example 1").get()
        
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    columns = ["id", "name"]
    data = [(1, "jules"), (2, "denny"), (3, "brooke"), (4, "td")]
    df = spark.createDataFrame(data).toDF(*columns)
    print(df.show())
    print_seperator("---")

    # Create some fake dataframe and so some transformations
    fake = Faker()
    columns = ["id", "name", "age"]
    ROWS = 250_000
    data = [(i, fake.name(), random.randint(18, 65)) for i in range(1, ROWS +1)]
    data_df = spark.createDataFrame(data, schema=columns)
    print_header("FAKE DATAFRAME")
    print(data_df.show())
    print_seperator("---")
    print_header("GROUP BY AGE")
    df_age = data_df.select("*").groupby("age").sum("age").orderBy("sum(age)", ascending=False)
    print(df_age.show(10))
    print_seperator("---")

