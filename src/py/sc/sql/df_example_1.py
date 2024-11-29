#
# Example used from Learning Spark v2 
#
from __future__ import print_function

from pyspark.sql import SparkSession
from pathlib import Path


DATA_FILE="/Users/jules/git-repos/spark-misc/src/py/sc/sql/data/mm_data.csv"

if __name__ == "__main__":
    # let's stop any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create a new session with Spark Connect
    spark = (SparkSession.builder
             .remote("sc://localhost")
             .appName("MnMCount")
             .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")
    

    # read the file into a Spark DataFrame; this is now
    # using SparkConnect
    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_FILE))
    mnm_df.show(n=5, truncate=False)

    # aggregate count of all colors and groupBy state and color
    # orderBy descending order
    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # show all the resulting aggregation for all the dates and colors
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # find the aggregate count for California by filtering
    ca_count_mnm_df = (mnm_df.select("*")
                       .where(mnm_df.State == 'CA')
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False))

    # show the resulting aggregation for California
    ca_count_mnm_df.show(n=10, truncate=False)
    ca_count_mnm_df.write.mode("overwrite").save("/tmp/tables/mnn_count_table")