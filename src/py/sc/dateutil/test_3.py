import sys
sys.path.append('.')

from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DateType, IntegerType, StringType
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark.pandas as ps
import random

if __name__ == "__main__":
    # Initialize Spark session
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("PySpark Dateutil Example Debug") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Helper function to generate random dates
    def random_date(start_date, end_date):
        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        return start_date + timedelta(days=random_days)

    # Create the first DataFrame
    start_date = datetime(2000, 1, 1)
    end_date = datetime(2030, 1, 1)
    num_rows = 5
    data_1 = {
        "id": [i for i in range(1,  num_rows+1)],
        "date1": [random_date(start_date, end_date) for _ in range(num_rows)]
    }

    df_1 = ps.DataFrame(data_1)
    print(type(df_1["date1"]))
    print(f"type={type(df_1['date1'][0])};{type(df_1['date1'][0].date())} ;value={df_1['date1'][0]}")
    days = (random_date(start_date, end_date) - df_1["date1"][0])
    print(f"days={days.days};type={type(days)}")
    rd = relativedelta(random_date(start_date, end_date), df_1["date1"][0])
    print(f"Difference: {rd.years} years, {rd.months} months, {rd.days} days")

    print("---" * 5)
    #df_1["days_difference"] = df_1.apply(lambda row: (relativedelta(random_date(start_date, end_date), df_1["date1"]).days))
    df_1["days_difference"] = df_1.apply(lambda row: row["date1"].days)

  
    rd = relativedelta(random_date(start_date, end_date), df_1["date1"][0])
    print(f"Difference: {rd.years} years, {rd.months} months, {rd.days} days")
    print(df_1.head())
    spark.stop()