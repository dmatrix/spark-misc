"""
This PySpark Spark Connect application includes the following features:

1. Read an exisitng delta table created from previous example
2. Use SQL to queries the delta table

Some code or partial code generated from Delta documentation.
"""
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, pandas_udf
from src.py.sc.utils.print_utils import print_seperator, print_header
from src.py.sc.utils.web_utils import generate_valid_urls
from src.py.sc.utils.spark_session_cls import SparkConnectSession

from delta.tables import DeltaTable

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = SparkConnectSession(remote="sc://localhost",
                                app_name="Delta Table Example 3").get()
    
    delta_table_path = "/tmp/delta/website_analysis"

    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    print_header("USE SPARK SQL TO QUERY THE LOCAL DELTA TABLE OF THE SORTED WEBSITES BY CONTENT LENGTH:")
    # Query the Delta table using Spark SQL
    query_result = spark.sql("""
        SELECT 
            url, content_length, status_code, response_time, content_type
        FROM 
            website_analysis
        WHERE 
            status_code = 200
        ORDER BY 
            content_length DESC
        LIMIT 25
    """)

    # Show the query result
    query_result.show(truncate=False)

    # order by response time
    print_header("USE SPARK SQL TO QUERY THE LOCAL DELTA TABLE OF THE SORTED WEBSITES BY RESPONSE TIME:")
    query_result = spark.sql("""
        SELECT 
            url, content_length, status_code, response_time, content_type
        FROM 
            website_analysis
        WHERE 
            status_code = 200
        ORDER BY 
            response_time DESC
        LIMIT 25
    """)
    # Show the query result
    query_result.show(truncate=False)

    # Describe table details
    print_header("USE SPARK SQL TO QUERY THE DELTA TABLE OF THE WEBSITES USING DESCRIBE:")
    query_result = spark.sql("DESCRIBE DETAIL website_analysis")
    # Show the query result
    query_result.show()

    # Describe table history
    print_header("USE SPARK SQL TO QUERY THE DELTA TABLE OF THE WEBSITES USING HISTORY:")
    query_result = spark.sql("DESCRIBE HISTORY website_analysis")
    # Show the query result
    query_result.show()

    # Use Python API
    print_header("USE PYTHON DELTA API TO CONVERT TO DF:")
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    query_result = delta_table.toDF()
    # Show the query result
    print(query_result.show())

    # Stop the Spark session
    spark.stop()



