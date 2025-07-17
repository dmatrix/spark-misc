#!/usr/bin/env python3
"""
Example usage of the PySpark REST DataSource package.

This script demonstrates how to use the pyspark-rest-datasource package
to read from and write to REST APIs using Spark DataFrames.
"""

import json
import logging
from pyspark.sql import SparkSession
from pyspark_rest_datasource import RestApiDataSource

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Example usage of the REST API data source"""
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PySpark REST DataSource Example") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        logger.info("✅ REST API data source registered successfully")
        
        # Example 1: Read from JSONPlaceholder API
        print("\n" + "="*80)
        print("📖 Example 1: Reading users from JSONPlaceholder API")
        print("="*80)
        
        users_df = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/users") \
            .option("method", "GET") \
            .load()
        
        print("📊 Users DataFrame:")
        users_df.show(5, truncate=False)
        print(f"📈 Total users: {users_df.count()}")
        
        # Example 2: Read posts and use SQL
        print("\n" + "="*80)
        print("📖 Example 2: Reading posts and using SQL queries")
        print("="*80)
        
        posts_df = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("method", "GET") \
            .load()
        
        # Create temporary view for SQL queries
        posts_df.createOrReplaceTempView("posts")
        
        # Query with SQL
        sql_result = spark.sql("""
            SELECT userId, COUNT(*) as post_count 
            FROM posts 
            GROUP BY userId 
            ORDER BY post_count DESC 
            LIMIT 5
        """)
        
        print("📊 Top 5 users by post count:")
        sql_result.show()
        
        # Example 3: Read a single resource
        print("\n" + "="*80)
        print("📖 Example 3: Reading a single user")
        print("="*80)
        
        single_user_df = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/users/1") \
            .option("method", "GET") \
            .load()
        
        print("👤 Single user data:")
        single_user_df.show(truncate=False)
        
        # Example 4: Custom headers
        print("\n" + "="*80)
        print("📖 Example 4: Using custom headers")
        print("="*80)
        
        headers = {
            "User-Agent": "PySpark-REST-DataSource/0.1.0",
            "Accept": "application/json"
        }
        
        custom_headers_df = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/users/1") \
            .option("method", "GET") \
            .option("headers", json.dumps(headers)) \
            .load()
        
        print("🔧 Data with custom headers:")
        custom_headers_df.show(1, truncate=False)
        
        # Example 5: Create sample data for writing (demo only)
        print("\n" + "="*80)
        print("✍️  Example 5: Preparing data for writing (demo)")
        print("="*80)
        
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        schema = StructType([
            StructField("title", StringType(), True),
            StructField("body", StringType(), True),
            StructField("userId", StringType(), True)
        ])
        
        sample_data = [
            ("Sample Post 1", "This is the content of post 1", "1"),
            ("Sample Post 2", "This is the content of post 2", "2"),
        ]
        
        sample_df = spark.createDataFrame(sample_data, schema)
        print("📝 Sample data to write:")
        sample_df.show(truncate=False)
        
        # Note: Uncomment the following to actually write to the API
        # sample_df.write \
        #     .format("restapi") \
        #     .option("url", "https://jsonplaceholder.typicode.com/posts") \
        #     .option("method", "POST") \
        #     .mode("append") \
        #     .save()
        
        print("\n🎉 All examples completed successfully!")
        print("📚 You can now use this package in your projects!")
        
    except Exception as e:
        logger.error(f"❌ Error in examples: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("🛑 Spark session stopped")


if __name__ == "__main__":
    print("🚀 Starting PySpark REST DataSource Examples")
    main() 