import os

import sys
sys.path.append('.')

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DateType, IntegerType, StringType
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark.pandas as ps
import random

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
                                app_name="PySpark Dateutil Test Example 3").get()
    
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
    # df_1["days_difference"] = df_1.apply(lambda row: (relativedelta(random_date(start_date, end_date), df_1["date1"]).days))
    # df_1["days_difference"] = df_1.apply(lambda row: df_1["date1"].days)

    rd = relativedelta(random_date(start_date, end_date), df_1["date1"][0])
    print(f"Difference: {rd.years} years, {rd.months} months, {rd.days} days")
    print(df_1.head())

    spark.stop()