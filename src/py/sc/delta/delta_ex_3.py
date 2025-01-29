"""
This PySpark Spark Connect application includes the following features:

1. Read an exisitng delta table created from previous example
2. Use SQL to queries the delta table

Some code or partial code generated from Delta documentation.
"""
import os
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_seperator, print_header
from src.py.sc.utils.web_utils import generate_valid_urls
from src.py.sc.utils.spark_session_cls import SparkConnectSession

from delta.tables import DeltaTable

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
                                app_name="Delta Example 3").get()

    DELTA_TABLE_NAME = "website_analysis"

    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    print_header(f"USE SPARK SQL TO QUERY THE DELTA TABLE {DELTA_TABLE_NAME} OF THE SORTED WEBSITES BY CONTENT LENGTH:")
    # Query the Delta table using Spark SQL
    query_result = spark.sql(f"""
        SELECT 
            url, content_length, status_code, response_time, content_type
        FROM 
            {DELTA_TABLE_NAME}
        WHERE 
            status_code = 200
        ORDER BY 
            content_length DESC
        LIMIT 25
    """)

    # Show the query result
    query_result.show(truncate=False)

    # order by response time
    print_header(F"USE SPARK SQL TO QUERY THE MANAGED DELTA TABLE {DELTA_TABLE_NAME} OF THE SORTED WEBSITES BY RESPONSE TIME:")
    query_result = spark.sql(f"""
        SELECT 
            url, content_length, status_code, response_time, content_type
        FROM 
            {DELTA_TABLE_NAME}
        WHERE 
            status_code = 200
        ORDER BY 
            response_time DESC
        LIMIT 25
    """)
    # Show the query result
    query_result.show(truncate=False)

    # Describe table details
    print_header(f"USE SPARK SQL TO QUERY THE DELTA TABLE {DELTA_TABLE_NAME} OF THE WEBSITES USING DESCRIBE:")
    query_result = spark.sql("DESCRIBE DETAIL website_analysis")
    # Show the query result
    query_result.show()

    # Describe table history
    print_header("USE SPARK SQL TO QUERY THE DELTA TABLE OF THE WEBSITES USING HISTORY:")
    query_result = spark.sql(f"DESCRIBE HISTORY {DELTA_TABLE_NAME}")
    # Show the query result
    query_result.show()

    # Stop the Spark session
    spark.stop()



