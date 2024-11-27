# This script, generaged by CodePilot, includes the following elements:
# 	1.	A DataFrame with 100,000 rows and 5 date-related columns using python-dateutil.
# 	2.	Random US state column.
# 	3.	Transformations: select, filter, groupby, UDF, and sort.
# 	4.	Pandas UDFs for applying custom transformations.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from faker import Faker
from dateutil.relativedelta import relativedelta
from datetime import datetime

import random
import pandas as pd

STATES = ['New York', 'Oregon', 'Iowa', 'Iowa', 'California', 
           'Georgia', 'Illinois', 'Wyoming', 'Alaska', 'Connecticut', 
           'Oklahoma', 'Connecticut', 'Kentucky', 'Colorado', 'North Dakota', 
        ]


# Generate sample data
def generate_data(f, num_rows):
    start_date = datetime(2000, 1, 1)
    end_date = datetime(2023, 1, 1)

    data = []
    for _ in range(num_rows):
        base_date = f.date_between(start_date=start_date, end_date=end_date)
        data.append({
            "ID": random.randint(1, 100000),
            "Start_Date": base_date,
            "End_Date": base_date + relativedelta(days=random.randint(1, 365)),
            "Created_At": base_date - relativedelta(years=random.randint(1, 10)),
            "Modified_At": base_date + relativedelta(months=random.randint(1, 24)),
            "State": random.choice(STATES)
    })
    return pd.DataFrame(data)

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("PySpark Pandas UDF Dateutil Example 3") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Initialize Faker for generating random dates
    fake = Faker()

    # Create DataFrame with 100,000 rows
    num_rows = 100000
    pandas_df = generate_data(fake, num_rows)
    spark_df = spark.createDataFrame(pandas_df)

    print("Initial Spark DataFrame:")
    spark_df.show(5)

    # Transformation Operations
    # 1. Select specific columns
    selected_df = spark_df.select("ID", "Start_Date", "End_Date", "State")
    print("\nSelected Columns:")
    selected_df.show(5)

    # 2. Filter rows where End_Date is greater than Start_Date
    filtered_df = spark_df.filter(col("End_Date") > col("Start_Date"))
    print("\nFiltered DataFrame (End_Date > Start_Date):")
    filtered_df.show(5)

    # 3. Sort by Modified_At column
    sorted_df = spark_df.orderBy("Modified_At")
    print("\nSorted DataFrame (by Modified_At):")
    sorted_df.show(5)

    # Stop the Spark session
    spark.stop()