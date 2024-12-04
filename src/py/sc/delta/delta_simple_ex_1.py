"""
Test to create a simple Deltat Table. Some code or partial code
was generated from ChatGPT, CodePilot, and docs samples.
"""
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("Delta Example 1") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    columns = ["id", "name"]
    data = [(1, "jules"), (2, "denny"), (3, "brooke"), (4, "td")]
    df = spark.createDataFrame(data).toDF(*columns)

    # Write the DataFrame as a Delta table
    df.write.format("delta").save("/tmp/delta/my_delta_table")