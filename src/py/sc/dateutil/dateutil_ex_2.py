"""
PySpark Python app to test SparkConnect. This file tests include:

1. Select specific columns
2. Filter rows on ceratan column value 
3. Group by Category and calculate average Value
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from dateutil.relativedelta import relativedelta
from faker import Faker
from datetime import datetime
import random
import pandas as pd

if __name__ == "__main__":
    # Initialize Spark session
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("PySpark Dateutil Example 2") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Initialize Faker for generating random dates
    fake = Faker()

    # Function to generate random dates using dateutil
    def generate_dates(num_rows):
        start_date = datetime(2000, 1, 1)
        end_date = datetime(2023, 1, 1)
        data = []
        for _ in range(num_rows):
            random_date = fake.date_between(start_date=start_date, end_date=end_date)
            modified_date = random_date + relativedelta(months=random.randint(1, 12))
            created_date = random_date - relativedelta(years=random.randint(1, 20))
            data.append({
                "Start_Date": random_date,
                "Modified_Date": modified_date,
                "Created_Date": created_date,
                "Category": random.choice(["A", "B", "C", "D"]),
                "Value": random.randint(10, 1000)
            })
        return pd.DataFrame(data)

    # Generate 1M rows
    num_rows = 1_000_000
    pandas_df = generate_dates(num_rows)

    # Convert Pandas DataFrame to PySpark DataFrame
    spark_df = spark.createDataFrame(pandas_df)

    print("Initial PySpark DataFrame:")
    spark_df.show(5)

    # DataFrame Transformations

    # 1. Select specific columns
    selected_df = spark_df.select("Start_Date", "Modified_Date", "Category", "Value")
    print("\nSelected Columns:")
    selected_df.show(5)

    # 2. Filter rows where Value is greater than 500
    filtered_df = selected_df.filter(col("Value") > 500)
    print("\nFiltered DataFrame (Value > 500):")
    filtered_df.show(5)

    # 3. Group by Category and calculate average Value
    grouped_df = filtered_df.groupBy("Category").agg(avg("Value").alias("Average_Value"))
    print("\nGrouped DataFrame (Average Value by Category):")
    grouped_df.show()

    # Stop the Spark session
    spark.stop()