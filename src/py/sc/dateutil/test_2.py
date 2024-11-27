from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import TimestampType

from dateutil.relativedelta import relativedelta
import pandas as pd
from datetime import datetime

if __name__ == "__main__":
    # Initialize Spark session
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("PySpark Dateutil Example 4") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")


    # Enable pandas-on-Spark
    ps.set_option("compute.default_index_type", "distributed")

    # Sample data
    data = [
        (1, "2022-01-01"),
        (2, "2022-06-15"),
        (3, "2023-03-10"),
        (4, "2023-12-25"),
        (5, None),  # Include a null value to test handling
    ]
    columns = ["ID", "Start_Date"]

    # Create PySpark DataFrame
    df = spark.createDataFrame(data, schema=columns)
    df = df.withColumn("Start_Date", df["Start_Date"].cast(TimestampType()))

    print("Original DataFrame:")
    df.show()

    # Define a pandas UDF with explicit decorator
    @pandas_udf(TimestampType())
    def add_one_year(dates: pd.Series) -> pd.Series:
        """
        Adds one year to the given datetime column using relativedelta.
        Handles null values.
        """
        return dates.apply(lambda x: x + relativedelta(years=1) if pd.notnull(x) else None)

    # Apply the UDF to create a new column
    df_with_end_date = df.withColumn("End_Date", add_one_year(df["Start_Date"]))

    print("DataFrame with End Date (1 Year Added):")
    df_with_end_date.show()

    # Stop the Spark session
    spark.stop()