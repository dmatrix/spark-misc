"""
This PySpark Spark Connect application includes the following features:

1. Read an exisitng delta table created from previous example
2. Use SQL to queries the delta table

Some code or partial code generated from Delta documentation, ChatGPT, and Documentation
Presently, some CRUD operation are not supported yet in 4.0.0rc1
https://docs.delta.io/4.0.0-preview/delta-spark-connect.html#preview-limitations
"""
import os
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit
from src.py.sc.utils.print_utils import print_header
from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession

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
                                app_name="Delta Example CRUD 4").get()

    DELTA_TABLE_NAME = "crud_operations_tbl_1"
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Step 1: CREATE operation - Initialize a Delta table
    data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, columns)

    print_header("CREATING CRUD DELTA TABLE:")
    spark.sql(f"DROP TABLE IF EXISTS {DELTA_TABLE_NAME};")

    # Save as Delta table
    df.write.format("delta").saveAsTable(DELTA_TABLE_NAME)
    print("Delta table created.")

    # Step 2: READ operation - Load and display the Delta table
    delta_table_df = spark.read.table(DELTA_TABLE_NAME)
    print_header("Read Delta table:")
    delta_table_df.show()

    # Step 3: UPDATE operation - Update a record in the Delta table
    delta_table = DeltaTable.forPath(spark, f"{DELTA_TABLE_NAME}")
    delta_table.update(
        condition=col("id") == 2,  # Update the record where id = 2
        set={"age": lit(28)}       # Set the age to 28
    )
    print_header("Updated Delta table:")
    delta_table.toDF().show()

    # Step 4: DELETE operation - Delete a record from the Delta table
    delta_table.delete(condition=col("id") == 3)  # Delete the record where id = 3
    print_header("Deleted record from Delta table:")
    delta_table.toDF().show()

    # Step 5: INSERT operation - Add new records to the Delta table
    new_data = [(4, "David", 40), (5, "Eve", 22)]
    new_df = spark.createDataFrame(new_data, columns)
    delta_table.alias("current").merge(
        new_df.alias("new"),
        "current.id = new.id"  # If matching ID exists, update; else, insert
    ).whenNotMatchedInsertAll().execute()

    print_header("Inserted new records into Delta table:")
    delta_table.toDF().show()

    # Step 6: READ historical data using Time Travel
    print_header("Time travel example - Show the first version of the Delta table:")
    historical_df = spark.read.format("delta").option("versionAsOf", 0).load(f"{DELTA_TABLE_NAME}")
    historical_df.show()

    # Step 7: Clean up (optional) - Vacuum to remove old files
    print_header("Vaccum the delta table:")
    delta_table.vacuum(retentionHours=0)  # Retention period in hours

    # Stop Spark Session
    spark.stop()