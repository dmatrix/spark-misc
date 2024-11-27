import sys
sys.path.append('.')

from src.py.sc.utils.print_utils import print_seperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, TimestampType

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark.pandas as ps
import pandas as pd
import random

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

    # Helper function to generate random dates
    def random_date(start_date, end_date):
        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        return start_date + timedelta(days=random_days)

    @pandas_udf(TimestampType())
    def add_years_to_date(date, years=2):
        # return date.apply(lambda d: d + relativedelta(years=years))
        return date.apply(lambda x: x + relativedelta(years=1) if pd.notnull(x) else None)


    @pandas_udf(StringType())
    def extract_weekday(date):
        return date.apply(lambda d: d.strftime("%A"))

    @pandas_udf(StringType())
    def is_date_in_past(date):
        return date.apply(lambda d: "Yes" if d < datetime.now() else "No")

    @pandas_udf(StringType())
    def is_value_even_or_odd(value):
        return value.apply(lambda v: "Even" if v % 2 == 0 else "Odd")

    # Create the first DataFrame
    start_date = datetime(2000, 1, 1)
    end_date = datetime(2030, 1, 1)
    data_1 = {

        "id": [i for i in range(1, 10001)],
        "start_date": [random_date(start_date, end_date) for _ in range(10000)],
        "end_date": [random_date(start_date, end_date) for _ in range(10000)],
        "value": [random.randint(1, 100) for _ in range(10000)],
        "category": [random.choice(["A", "B", "C", "D"]) for _ in range(10000)],

    }

    # Use PySpark pandas
    ps.set_option("compute.default_index_type", "distributed")
    ps.set_option('compute.ops_on_diff_frames', True)
    df_1 = ps.DataFrame(data_1)

    # DataFrame Transformations on df_1
    print("Transformation 1: Filter rows where value > 50:")
    d_f1 = df_1[df_1["value"] > 50]  
    print(df_1.head())
    print(df_1.head())
    # Transformation 2: Extract year from date1
    print(" # Transformation 2: Extract year from date1:")    
    df_1["start_year"] = df_1["start_date"].dt.year 
    print(df_1.head())
    print_seperator()

    # Transformation 3: Group and aggregate
    print("# Transformation 3: Group and aggregate:")
    grouped_df = df_1.groupby("category")["value"].mean().reset_index() 
    print(df_1.head())
    print_seperator()

    # Apply lambda & UDFs to df_1
    # Transformation 5: Day difference
    print("# Transformation 5: Day difference:")
    df_1["days_difference"] = df_1.apply(lambda row: (row['end_date'] - row['start_date']).days, axis=1)
    print(df_1.head())
    print_seperator()
                                        
    print("Applying Pandas `months_difference:`")
    df_1["months_diff"] = df_1.apply(lambda row: (row["end_date"] - row["start_date"]).days // 30, axis=1)

    print(df_1.head())
    print_seperator()

    # Now convert Pandas Pyspark --> Pyspark DataFrame to apply 
    # Pandas UDFs

    spark_df_1 = df_1.to_spark()

    print("Applying Pandas `add_years_to_date:`")
    spark_df_1 = spark_df_1.withColumn("start_date_plus_2_years", add_years_to_date(spark_df_1["start_date"]))
    print(spark_df_1.show())
    print_seperator()


    print("Applying Pandas `extract_weekday:`")
    spark_df_1 = spark_df_1.withColumn("weekday", extract_weekday(spark_df_1["start_date"]))
    print(spark_df_1.show())
    print_seperator()

    print("Applying Pandas `is_date_in_past:`")
    spark_df_1 = spark_df_1.withColumn("date_in_past", is_date_in_past(spark_df_1["start_date"]))
    print(spark_df_1.show())
    print_seperator()

    print("Applying Pandas `is_value_even_or_odd:`")
    spark_df_1 = spark_df_1.withColumn("value_even_or_odd", is_value_even_or_odd(spark_df_1["value"]))
    print(spark_df_1.show())
    print_seperator()

    # Create the second DataFrame
    data_2 = {
        "id": [i for i in range(1, 10001)],
        "date2": [random_date(start_date, end_date) for _ in range(10000)],
        "value2": [random.randint(1, 100) for _ in range(10000)],
        "group": [random.choice(["X", "Y", "Z"]) for _ in range(10000)],
    }

    df_2 = ps.DataFrame(data_2)

    # Merge and Join Operations on the original Pandas PySpark Dataframe
    print("Merge and Join Operations: Merge on id")
    merged_df = df_1.merge(df_2, on="id", how="inner")  
    print("# Left join on id:")
    joined_df = df_1.join(df_2.set_index("id"), on="id", how="left") 
    print(joined_df.head())
    print_seperator()

    # Add new columns to merged_df for analysis
    print("Add new columns to merged_df for analysis:")
    merged_df["value_diff"] = merged_df["value"] - merged_df["value2"]  # Value difference
    merged_df["category_group"] = merged_df["category"] + "_" + merged_df["group"]  # Combine category and group

    # Display merged and transformed DataFrame
    print("Merged DataFrame:")
    print(merged_df.head())
    print("\nJoined DataFrame:")
    print(joined_df.head())

    spark.stop()