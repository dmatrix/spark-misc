"""
This script, with certain parts generaged by CodePilot, includes the following tests:
	1.	A DataFrame with 100,000 rows and 5 date-related columns using python-dateutil.
	2.	Random US state column.
	3.	Transformations: select, filter, groupby, and sort.

Some code or partial code was generated from ChatGPT and CodePilot
"""

import sys
sys.path.append('.')
import warnings
warnings.filterwarnings("ignore")

from src.py.sc.utils.print_utils import print_seperator, print_header
from src.py.sc.utils.spark_session_cls import SparkConnectSession
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
    spark = SparkConnectSession(remote="local[*]",
                                app_name="PySpark Pandas UDF Dateutil Example 3").get()
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Initialize Faker for generating random dates
    fake = Faker()

    # Create DataFrame with 100,000 rows
    num_rows = 100_000
    pandas_df = generate_data(fake, num_rows)
    spark_df = spark.createDataFrame(pandas_df)

    print_header("Initial Spark DataFrame:")
    spark_df.show(5)
    print_seperator(size=15)

    # Transformation Operations
    # 1. Select specific columns
    selected_df = spark_df.select("ID", "Start_Date", "End_Date", "State")
    print_header("\nSelected Columns:")
    selected_df.show(5)
    print_seperator(size=15)

    # 2. Filter rows where End_Date is greater than Start_Date
    filtered_df = spark_df.filter(col("End_Date") > col("Start_Date"))
    print_header("\nFiltered DataFrame (End_Date > Start_Date):")
    filtered_df.show(5)
    print_seperator(size=15)


    # 3. Sort by Modified_At column
    sorted_df = spark_df.orderBy("Modified_At")
    print_header("\nSorted DataFrame (by Modified_At):")
    sorted_df.show(5)
    print_seperator(size=15)

    # Stop the Spark session
    spark.stop()