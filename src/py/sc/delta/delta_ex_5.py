from delta import *
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from faker import Faker

import sys
sys.path.append('.')
from src.py.sc.utils.print_utils import print_header, print_seperator
from src.py.sc.utils.spark_session_cls import SparkConnectSession
import warnings
warnings.filterwarnings("ignore")

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = SparkConnectSession(remote="sc://localhost",
                                app_name="Delta Table Example (CRUD) 5").get()

    # File path for the Delta table
    delta_table_path = "/tmp/crud_delta_table"

    # Step 2: Generate a large dataset using Faker (100,000 rows, 6 columns)
    fake = Faker()
    data = [
        (i, fake.name(), fake.job(), fake.random_int(20, 65), fake.random_int(175_000, 575_000), fake.date_this_decade())
        for i in range(1, 100001)
    ]
    columns = ["id", "name", "job", "age", "salary", "hire_date"]
    df = spark.createDataFrame(data, columns)

    # Step 3: CREATE operation - Write the data as a Delta table
    df.write.format("delta").mode("overwrite").save(delta_table_path)
    print("Delta table created.")

    # Step 4: READ operation - Load and display the Delta table
    delta_table = spark.read.format("delta").load(delta_table_path)
    print("Initial Delta table:")
    delta_table.show(10)

    # Step 5: UPDATE operation - Update salary for employees with age > 50
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    delta_table.update(
        condition=col("age") > 50,
        set={"salary": lit(2000)}  # Set salary to 2000
    )
    print("Updated Delta table (salary updated for age > 50):")
    delta_table.toDF().show(10)

    # Step 6: DELETE operation - Delete employees with salary < 500
    delta_table.delete(condition=col("salary") < 500)
    print("Delta table after deletion (salary < 500 removed):")
    delta_table.toDF().show(10)

    # Step 7: INSERT operation - Add new records (MERGE)
    new_data = [
        (100001, "New Person 1", "Engineer", 30, 250000, "2024-01-01"),
        (100002, "New Person 2", "Designer", 40, 480000, "2023-12-15")
    ]
    new_df = spark.createDataFrame(new_data, columns)

    delta_table.alias("current").merge(
        new_df.alias("new"),
        "current.id = new.id"  # Match records by ID
    ).whenNotMatchedInsertAll().execute()

    print("Delta table after merge (new records inserted):")
    delta_table.toDF().show(10)

    # Step 8: READ historical data using Time Travel
    print("Time travel example - Show the first version of the Delta table:")
    historical_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
    historical_df.show(10)

    # Step 9: VACUUM operation - Clean up old versions
    delta_table.vacuum(retentionHours=0)  # Adjust retention period as needed
    print("Vacuum operation completed.")

    # Stop Spark Session
    spark.stop()