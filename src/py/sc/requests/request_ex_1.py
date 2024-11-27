import sys
sys.path.append('.')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, pandas_udf
from src.py.sc.utils.print_utils import print_seperator
from src.py.sc.utils.web_utils import generate_valid_urls

import pandas as pd
import random
import requests
import time

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

    # Generate 1,000 URLs and select 200 random URLs
    all_urls = generate_valid_urls(1000)
    random_websites = random.sample(all_urls, 200)

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
    valid_websites = websites_with_details.filter(col("content_length") > 0)

    # Sort the DataFrame by content length
    sorted_websites = valid_websites.orderBy(col("content_length").desc())

    # Show the result
    sorted_websites.show(100, truncate=False)

    # Stop the Spark session
    spark.stop()

