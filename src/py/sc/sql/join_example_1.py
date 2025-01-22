#
# Examples from pyspark-cookbook guide
#
import os
import sys
sys.path.append('.')

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import Row

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
                                app_name="PySpark DataFrame Function APIs Example 2").get()
    
    # Ensure we are conneccted to the spark session
    print_header("ASSERTING SPARK CONNECT USAGE:")
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    column_str = "THE ART OF JOINING....(DEFAULT:INNER JOIN)"
    print_header(column_str)
    
    # Create a DataFrame using Rows
    df_1 = spark.createDataFrame([
        Row(age=10, height=80.0, name="alice"),
        Row(age=9, height=78.9, name="josh"),
        Row(age=18, height=82.3, name="bush"),
        Row(age=7, height=75.3, name="tom"),
])

    df_2 = spark.createDataFrame([
        Row(incomes=[123.0, 456.0, 789.0], name="alice"),
        Row(incomes=[234.0, 567.0], name="bob"),
        Row(incomes=[79.0, 128.0], name="josh"),
        Row(incomes=[123.0, 145.0, 178.0], name="bush"),
        Row(incomes=[111.0, 187.0, 451.0, 188.0, 199.0], name="jerry"),
    ])

    # Join by name
    print_header("JOIN BY COLUMN NAME:")
    df_3 = df_1.join(df_2, on="name")
    df_3.show(truncate=False)
    print_seperator(size=10)

    # Let’s take LEFT join as another example. A left join includes all of the records 
    # from the first (left) of two tables, even if there are no matching values for records 
    # in the second (right) table.
    column_str = "THE ART OF JOINING....:LEFT JOIN)"
    print_header(column_str)
    df_4 = df_1.join(df_2, on="name", how="left")
    df_4.show(truncate=False)
    print_seperator(size=10)
    
    # And a RIGHT join keeps all of the records from the right table.
    column_str = "THE ART OF JOINING....:RIGHT JOIN)"
    print_header(column_str)
    df_5 = df_1.join(df_2, on="name", how="right")
    df_5.show(truncate=False)

