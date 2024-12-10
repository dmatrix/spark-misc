"""
Test to find spark home, after unsetting the SPARK_HOME environment variable
"""
from pyspark.sql import SparkSession
import os
import findspark

if __name__ == "__main__":
    spark_home = os.getenv("SPARK_HOME")
    os.environ["SPARK_HOME"] = "" if spark_home else spark_home
    spark_home = os.getenv("SPARK_HOME")
    print(f"SPARK_HOME: {spark_home}")
    findspark.init()
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("local[*]")
                .appName("Find Spark Example 1") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    columns = ["id", "name"]
    data = [(1, "jules"), (2, "denny"), (3, "brooke"), (4, "td")]
    df = spark.createDataFrame(data).toDF(*columns)
    df.show()