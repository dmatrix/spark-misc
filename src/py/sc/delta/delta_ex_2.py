"""
This PySpark Spark Connect application includes the following features:

1. Generate a large number of websites URLs, and select a random number to process
2. Download the content usuing Request Python package
3. Use DataFrame API features for filtering and sorting
4. Create a delta table from the final dataframe
5. Use SQL to query the delta table

Some code or partial code generated from Delta documentation, extended with ChatGPT, and 
borrowed from Delta documentation.

"""
import os
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from pyspark.sql.functions import col, monotonically_increasing_id, pandas_udf
from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_seperator, print_header
from src.py.sc.utils.web_utils import generate_valid_urls
from src.py.sc.utils.spark_session_cls import SparkConnectSession

import pandas as pd
import random
import requests
import time

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
                                app_name="Delta Example 2").get()


    DELTA_TABLE_NAME = "website_analysis"
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Generate URLs and select random URLs
    number_of_urls = 6000
    random_urls =  int(number_of_urls / 3) 
    all_urls = generate_valid_urls(number_of_urls)
    random_websites = random.sample(all_urls, random_urls)

    # Create a DataFrame from the random sample of websites
    websites_df = spark.createDataFrame([(url,) for url in random_websites], ["url"])

    # Add a unique ID column
    websites_df = websites_df.withColumn("unique_id", monotonically_increasing_id())

    # Define a Pandas UDF to fetch additional information using requests
    @pandas_udf("struct<content_length:int, status_code:int, response_time:float, content_type:string>")
    def fetch_website_details(urls: pd.Series) -> pd.DataFrame:
        data = []
        for url in urls:
            try:
                start_time = time.time()
                response = requests.get(url, timeout=5)
                elapsed_time = time.time() - start_time
                data.append({
                    "content_length": len(response.content) if response.status_code == 200 else -1,
                    "status_code": response.status_code,
                    "response_time": elapsed_time,
                    "content_type": response.headers.get("Content-Type", "Unknown")
                })
            except Exception:
                data.append({
                    "content_length": -1,
                    "status_code": -1,
                    "response_time": - 1.0,
                    "content_type": "Error"
                })
        return pd.DataFrame(data)

    # Apply the UDF to add the columns
    websites_with_details = websites_df.withColumn("details", fetch_website_details(col("url")))

    # Extract individual columns from the struct column
    websites_with_details = websites_with_details.select(
        col("url"),
        col("unique_id"),
        col("details.content_length").alias("content_length"),
        col("details.status_code").alias("status_code"),
        col("details.response_time").alias("response_time"),
        col("details.content_type").alias("content_type")
    )
    # Filter out invalid responses
    print_header("FILTER OUT INVALID RESPONSES:")
    valid_websites = websites_with_details.filter(col("content_length") > 0)
    valid_websites.limit(10).show(truncate=False)
    print_seperator()

    # Sort the DataFrame by content length
    print_header("SORT WEBSITES BY CONTENT_LENGTH:")
    sorted_websites = valid_websites.orderBy(col("content_length").desc())
    sorted_websites.limit(25).show(truncate=False)

    # Save the DataFrame as a Delta table
    print_header("CREATE A MANAGED DELTA TABLE OF THE SORTED WEBSITES BY CONTENT LENGTH:")
    spark.sql(f"DROP TABLE IF EXISTS {DELTA_TABLE_NAME};")
    sorted_websites.write.format("delta").saveAsTable(DELTA_TABLE_NAME)

    # Get the Deltat table name properties
    spark.sql(f"desc formatted {DELTA_TABLE_NAME}").show(truncate = False)
    print_seperator("---")

    print_header("USE SPARK SQL TO QUERY THE MANGAGED DELTA TABLE OF THE SORTED WEBSITES BY CONTENT LENGTH:")
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

    # Stop the Spark session
    spark.stop()

