#
# Example used from Learning Spark v2 
#
import sys
sys.path.append('.')
from src.py.sc.utils.print_utils import print_header, print_seperator

from pyspark.sql import SparkSession
import os

DATA_FILE="/Users/jules/git-repos/spark-misc/src/py/sc/sql/data/mm_data.csv"

if __name__ == "__main__":
    # let's stop any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create a new session with Spark Connect
    spark = (SparkSession.builder
             .remote("local[*]")
             .appName("PySpark DataFrame API MnMCount Example 1")
             .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")
    print_seperator(size=20)

    # read the file into a Spark DataFrame; this is now
    # using SparkConnect
    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DATA_FILE))
    
    print_header("DATAFRME READ FROM A CSV FILE:")
    mnm_df.show(n=5, truncate=False)
    print_seperator()

    # aggregate count of all colors and groupBy state and color
    # orderBy descending order

    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # show all the resulting aggregation for all the Sates and colors
    print_header("GROUPBY, AGGREGATE BY STATES AND COLOUR: ")
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))
    print_seperator()

    # find the aggregate count for California by filtering
    ca_count_mnm_df = (mnm_df.select("*")
                       .where(mnm_df.State == 'CA')
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False))

    # show the resulting aggregation for California
    print_header("FILTER BY CALIFORINIA STATE")
    ca_count_mnm_df.show(n=10, truncate=False)
    print_seperator(size=10)

    # save as managed table
    print_header("SAVE AS A MANAGED TABLE:")
    ca_count_mnm_df.write.mode("overwrite").save("/tmp/tables/mnn_count_table")
    print(os.listdir("/tmp/tables/mnn_count_table"))
    print_seperator(size=10)
    spark.stop()
          
