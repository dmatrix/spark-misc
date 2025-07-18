#!/usr/bin/env python3
"""
Example usage of REST API Data Source with Partitioning

This file demonstrates how to use the REST API data source with different partitioning strategies
for parallel processing of REST API calls.
"""

from pyspark.sql import SparkSession
from restapi import RestApiDataSource

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("RestApiPartitionExample") \
        .getOrCreate()
    
    # Register the REST API data source
    spark.dataSource.register(RestApiDataSource)
    
    print("=== REST API Data Source Partitioning Examples ===\n")
    
    # Example 1: Single partition (default behavior)
    print("1. Single Partition Example:")
    print("   Reading from a single REST API endpoint")
    try:
        df1 = spark.read.format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts/1") \
            .option("method", "GET") \
            .load()
        
        print(f"   Single partition setup complete")
        print(f"   Row count: {df1.count()}")
        df1.show(5)
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n" + "="*60 + "\n")
    
    # Example 2: URL-based partitioning
    print("2. URL-based Partitioning Example:")
    print("   Reading from multiple URLs in parallel")
    try:
        df2 = spark.read.format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", "https://jsonplaceholder.typicode.com/posts/1,https://jsonplaceholder.typicode.com/posts/2,https://jsonplaceholder.typicode.com/posts/3") \
            .option("method", "GET") \
            .load()
        
        print(f"   URL-based partitioning setup complete")
        print(f"   Row count: {df2.count()}")
        df2.show(5)
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n" + "="*60 + "\n")
    
    # Example 3: Page-based partitioning
    print("3. Page-based Partitioning Example:")
    print("   Reading paginated data in parallel")
    try:
        df3 = spark.read.format("restapi") \
            .option("partitionStrategy", "pages") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("totalPages", "3") \
            .option("pageSize", "10") \
            .option("method", "GET") \
            .load()
        
        print(f"   Page-based partitioning setup complete")
        print(f"   Row count: {df3.count()}")
        df3.show(5)
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n" + "="*60 + "\n")
    
    # Example 4: Custom headers with partitioning
    print("4. Custom Headers with Partitioning:")
    print("   Using custom headers with URL-based partitioning")
    try:
        custom_headers = '{"User-Agent": "PySpark-RestAPI-DataSource", "Accept": "application/json"}'
        df4 = spark.read.format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", "https://jsonplaceholder.typicode.com/users/1,https://jsonplaceholder.typicode.com/users/2") \
            .option("headers", custom_headers) \
            .option("method", "GET") \
            .load()
        
        print(f"   Custom headers partitioning setup complete")
        print(f"   Row count: {df4.count()}")
        df4.show(5)
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n" + "="*60 + "\n")
    
    # Example 5: Partition analysis
    print("5. Partition Analysis:")
    print("   Analyzing how data is distributed across partitions")
    try:
        df5 = spark.read.format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", "https://jsonplaceholder.typicode.com/posts/1,https://jsonplaceholder.typicode.com/posts/2,https://jsonplaceholder.typicode.com/posts/3,https://jsonplaceholder.typicode.com/posts/4") \
            .option("method", "GET") \
            .load()
        
        print(f"   Partition analysis setup complete")
        print(f"   Total rows: {df5.count()}")
        
        # Show partition distribution using DataFrame API
        from pyspark.sql.functions import spark_partition_id
        partition_counts = [row['count'] for row in df5.select(spark_partition_id().alias("partition_id")).groupBy("partition_id").count().orderBy("partition_id").collect()]
        for i, count in enumerate(partition_counts):
            print(f"   Partition {i}: {count} rows")
            
        df5.show(10)
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n" + "="*60 + "\n")
    
    # Performance comparison
    print("6. Performance Comparison:")
    print("   Comparing single vs multiple partition performance")
    
    import time
    
    # Single partition timing
    try:
        start_time = time.time()
        df_single = spark.read.format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("method", "GET") \
            .load()
        single_count = df_single.count()
        single_time = time.time() - start_time
        print(f"   Single partition: {single_count} rows in {single_time:.2f}s")
    except Exception as e:
        print(f"   Single partition error: {e}")
        single_time = float('inf')
    
    # Multiple partition timing (simulated with different endpoints)
    try:
        start_time = time.time()
        df_multi = spark.read.format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", "https://jsonplaceholder.typicode.com/posts/1,https://jsonplaceholder.typicode.com/posts/2,https://jsonplaceholder.typicode.com/posts/3,https://jsonplaceholder.typicode.com/posts/4") \
            .option("method", "GET") \
            .load()
        multi_count = df_multi.count()
        multi_time = time.time() - start_time
        print(f"   Multi partition: {multi_count} rows in {multi_time:.2f}s")
        
        if single_time != float('inf') and multi_time > 0:
            speedup = single_time / multi_time
            print(f"   Speedup: {speedup:.2f}x")
    except Exception as e:
        print(f"   Multi partition error: {e}")
    
    spark.stop()

if __name__ == "__main__":
    main() 