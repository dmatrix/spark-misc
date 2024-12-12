"""
This PySpark Spark Connect application includes the following features:

1. Read an exisitng delta table created from previous example
2. Use SQL to queries the delta table

Some code or partial code generated from Delta documentation, ChatGPT, and Documentation
Presently, some CRUD operation are not supported yet in 4.0.0rc1
https://docs.delta.io/4.0.0-preview/delta-spark-connect.html#preview-limitations
"""
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit
from src.py.sc.utils.print_utils import print_seperator, print_header

if __name__ == "__main__":
    # Step 1: Initialize Spark Session with Delta Lake support
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("Delta Table Example (CRUD) 3") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # File path for the Delta table
    delta_table_path = "/tmp/crud_delta_table"

    # Step 2: CREATE operation - Initialize a Delta table
    data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, columns)

    # Save as Delta table
    df.write.format("delta").mode("overwrite").save(delta_table_path)
    print("Delta table created.")

    # Step 3: READ operation - Load and display the Delta table
    delta_table = spark.read.format("delta").load(delta_table_path)
    print_header("Read Delta table:")
    delta_table.show()


    # Step 4: UPDATE operation - Update a record in the Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    delta_table.update(
        condition=col("id") == 2,  # Update the record where id = 2
        set={"age": lit(28)}       # Set the age to 28
    )
    print_header("Updated Delta table:")
    delta_table.toDF().show()

    # Step 5: DELETE operation - Delete a record from the Delta table
    delta_table.delete(condition=col("id") == 3)  # Delete the record where id = 3
    print_header("Deleted record from Delta table:")
    delta_table.toDF().show()

    # Step 6: INSERT operation - Add new records to the Delta table
    new_data = [(4, "David", 40), (5, "Eve", 22)]
    new_df = spark.createDataFrame(new_data, columns)
    delta_table.alias("current").merge(
        new_df.alias("new"),
        "current.id = new.id"  # If matching ID exists, update; else, insert
    ).whenNotMatchedInsertAll().execute()

    print_header("Inserted new records into Delta table:")
    delta_table.toDF().show()

    # Step 7: READ historical data using Time Travel
    print_header("Time travel example - Show the first version of the Delta table:")
    historical_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
    historical_df.show()

    # Step 8: Clean up (optional) - Vacuum to remove old files
    print_header("Vaccum the deltat tabel:")
    delta_table.vacuum(retentionHours=0)  # Retention period in hours

    # Stop Spark Session
    spark.stop()