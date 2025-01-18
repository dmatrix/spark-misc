import sys
sys.path.append('.')
import os

import warnings
warnings.filterwarnings("ignore")
from IPython.display import display

from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator
import pyspark.sql.functions as F


if __name__ == "__main__":
    cluster_id = os.environ.get("clusterID")
    assert cluster_id
    spark = DatabrckSparkSession().get()
    
    # Ensure we are connected vis Spark Connect Session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++ Making sure it's using SparkConnect session:{spark} +++++")

    FILE = "dbfs:///databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz"
    print_header("READING NYC TAXI FILE FOR 2019-12")
    df = spark.read.csv(FILE, header=True)
    print(df.show(truncate=False))
    print(f"Number of Rows = {df.count()}")
    print_seperator("----")
    
    # Do some minor transformations
    print_header("NYC DATAFRAME TRANSFORMATIONS AN ACTIONS")
    df_2 = (df.filter("Passenger_count = 1")
            .select("Trip_distance", "Payment_type")
            .groupBy("Payment_type").agg(F.avg("Trip_distance").alias("avg_trip_distance")))
    print(df_2.show(10, truncate=False))
    print_seperator("----")
