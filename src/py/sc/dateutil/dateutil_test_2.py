import os
import sys
sys.path.append('.')

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header
import pyspark.pandas as ps
from pyspark.sql.functions import pandas_udf, count
from pyspark.sql.types import TimestampType

from dateutil.relativedelta import relativedelta
import pandas as pd

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
                                app_name="PySpark Dateutil Test Example 2").get()
    
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

    print_header("ORIGINAL DATAFRAME:")
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

    print_header("DATAFRAME WITH END DATE (1 YEAR ADDED):")
    df_with_end_date.show()

    # Stop the Spark session
    spark.stop()