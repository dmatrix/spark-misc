#
# Example used from Learning Spark v2 
#
import os
import sys
sys.path.append('.')

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator

DATA_FILE = "/Users/jules/git-repos/spark-misc/src/py/sc/sql/data/mm_data.csv"
STORAGE_LOCATION = "mnn_count_table"

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
        DATA_FILE = "dbfs:/databricks-datasets/learning-spark-v2/mnm_dataset.csv"
        spark = spark = DatabrckSparkSession().get()
    else:
        spark = SparkConnectSession(remote="local[*]", mode=mode,
                                app_name="PySpark DataFrame API MnMCount Example 1").get()

    
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
    print_header("GROUPBY, AGGREGATE BY STATES AND COLOUR: ")
    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False).alias("sum_count"))

    count_mnm_df = count_mnm_df.withColumnRenamed("sum(Count)", "sum_count")

    # show all the resulting aggregation for all the Sates and colors
    print("Total Rows = %d" % (count_mnm_df.count()))
    print_seperator()

    # find the aggregate count for California by filtering
    # show the resulting aggregation for California
    print_header("FILTER BY CALIFORINIA STATE")
    ca_count_mnm_df = (mnm_df.select("*")
                       .where(mnm_df.State == 'CA')
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False).alias("sum_ca_count"))

    ca_count_mnm_df = ca_count_mnm_df.withColumnRenamed("sum(Count)","sum_ca_count")
    ca_count_mnm_df.show(n=10, truncate=False)
    print_seperator(size=10)

    print_header(f"DROPING TABLE {STORAGE_LOCATION} IF EXSITS...")
    spark.sql(f"DROP TABLE IF EXISTS {STORAGE_LOCATION}")
    print_header(f"SAVE DATAFRAME AS A MANAGED TABLE:{STORAGE_LOCATION}")
    ca_count_mnm_df.write.option("mode", "overwrite").saveAsTable(STORAGE_LOCATION)
    print_seperator(size=10)

    print_header("READ DATAFRAME FROM A MANAGED TABLE")
    df = spark.sql(f"select (*) from {STORAGE_LOCATION}")
    df.show(n=10, truncate=False)

    spark.stop()
          
