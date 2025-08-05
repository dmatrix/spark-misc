#!/usr/bin/env python3
"""
Example usage of the PySpark REST DataSource package.

This script demonstrates how to use the pyspark-rest-datasource package
to read from and write to REST APIs using Spark DataFrames.
"""

import json
import logging
from pyspark.sql import SparkSession
from restapi import RestApiDataSource

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
            "User-Agent": "PySpark-REST-DataSource/0.2.0",
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
        
        # Example 6: URL-based partitioning for parallel requests
        print("\n" + "="*80)
        print("🚀 Example 6: URL-based partitioning (100+ parallel requests)")
        print("="*80)
        
        # Create URLs for posts 1-50 (50 parallel requests)
        post_urls = [f"https://jsonplaceholder.typicode.com/posts/{i}" for i in range(1, 51)]
        urls_string = ",".join(post_urls)
        
        partitioned_posts_df = spark.read \
            .format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", urls_string) \
            .option("method", "GET") \
            .load()
        
        print(f"📊 Posts fetched with URL partitioning: {partitioned_posts_df.count()}")
        print("📈 Sample of partitioned posts:")
        partitioned_posts_df.show(5, truncate=False)
        
        # Example 7: Combined datasets for 100+ requests
        print("\n" + "="*80)
        print("🔗 Example 7: Combined datasets (100+ requests)")
        print("="*80)
        
        # Fetch all posts (100 posts)
        all_posts_df = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("method", "GET") \
            .load()
        
        # Fetch all comments (500 comments)
        all_comments_df = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/comments") \
            .option("method", "GET") \
            .load()
        
        # Fetch all albums (100 albums)
        all_albums_df = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/albums") \
            .option("method", "GET") \
            .load()
        
        print(f"📊 Posts: {all_posts_df.count()}")
        print(f"💬 Comments: {all_comments_df.count()}")
        print(f"📸 Albums: {all_albums_df.count()}")
        
        total_records = all_posts_df.count() + all_comments_df.count() + all_albums_df.count()
        print(f"🎯 Total records fetched: {total_records}")
        
        # Create views for complex queries
        all_posts_df.createOrReplaceTempView("all_posts")
        all_comments_df.createOrReplaceTempView("all_comments")
        all_albums_df.createOrReplaceTempView("all_albums")
        
        # Complex analysis across datasets
        analysis_result = spark.sql("""
            SELECT 
                p.userId,
                COUNT(DISTINCT p.id) as posts_count,
                COUNT(DISTINCT c.id) as comments_count,
                COUNT(DISTINCT a.id) as albums_count
            FROM all_posts p
            LEFT JOIN all_comments c ON p.id = c.postId
            LEFT JOIN all_albums a ON p.userId = a.userId
            GROUP BY p.userId
            ORDER BY posts_count DESC
            LIMIT 10
        """)
        
        print("📈 User activity analysis:")
        analysis_result.show()
        
        # Example 8: Page-based partitioning simulation
        print("\n" + "="*80)
        print("📄 Example 8: Page-based partitioning simulation")
        print("="*80)
        
        # Simulate page-based partitioning by fetching different ranges
        page_dfs = []
        page_size = 10
        total_pages = 10  # This will give us 100 posts
        
        for page in range(1, total_pages + 1):
            start_id = (page - 1) * page_size + 1
            end_id = page * page_size
            
            # Create URLs for this "page" of posts
            page_urls = [f"https://jsonplaceholder.typicode.com/posts/{i}" 
                        for i in range(start_id, min(end_id + 1, 101))]  # JSONPlaceholder has 100 posts
            
            if page_urls:  # Only if we have URLs to fetch
                page_urls_string = ",".join(page_urls)
                
                page_df = spark.read \
                    .format("restapi") \
                    .option("partitionStrategy", "urls") \
                    .option("urls", page_urls_string) \
                    .option("method", "GET") \
                    .load()
                
                page_dfs.append(page_df)
        
        # Union all page DataFrames
        if page_dfs:
            combined_pages_df = page_dfs[0]
            for df in page_dfs[1:]:
                combined_pages_df = combined_pages_df.union(df)
            
            print(f"📊 Total posts from page-based approach: {combined_pages_df.count()}")
            print("📈 Sample from combined pages:")
            combined_pages_df.show(5, truncate=False)
        
        # Example 9: Performance comparison and timing
        print("\n" + "="*80)
        print("⚡ Example 9: Performance comparison (sequential vs parallel)")
        print("="*80)
        
        import time
        
        # Sequential approach - fetch individual posts one by one (simulated with single batch)
        start_time = time.time()
        sequential_df = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("method", "GET") \
            .load()
        sequential_count = sequential_df.count()
        sequential_time = time.time() - start_time
        
        # Parallel approach - use URL partitioning
        start_time = time.time()
        parallel_urls = [f"https://jsonplaceholder.typicode.com/posts/{i}" for i in range(1, 21)]  # First 20 posts
        parallel_df = spark.read \
            .format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", ",".join(parallel_urls)) \
            .option("method", "GET") \
            .load()
        parallel_count = parallel_df.count()
        parallel_time = time.time() - start_time
        
        print(f"⏱️  Sequential: {sequential_count} records in {sequential_time:.2f}s")
        print(f"🚀 Parallel: {parallel_count} records in {parallel_time:.2f}s")
        
        if sequential_time > 0 and parallel_time > 0:
            speedup = sequential_time / parallel_time if parallel_count > 0 else 0
            print(f"📈 Speedup factor: {speedup:.2f}x")
        
        # Example 10: Large-scale data aggregation
        print("\n" + "="*80)
        print("📊 Example 10: Large-scale data aggregation")
        print("="*80)
        
        # Use the previously fetched data for aggregations
        all_posts_df.createOrReplaceTempView("posts_for_aggregation")
        
        # Comprehensive aggregation queries
        aggregation_queries = [
            ("Post length analysis", """
                SELECT 
                    LENGTH(body) as body_length_category,
                    COUNT(*) as count
                FROM posts_for_aggregation
                GROUP BY LENGTH(body)
                ORDER BY body_length_category
                LIMIT 10
            """),
            ("User posting patterns", """
                SELECT 
                    userId,
                    COUNT(*) as total_posts,
                    AVG(LENGTH(title)) as avg_title_length,
                    AVG(LENGTH(body)) as avg_body_length
                FROM posts_for_aggregation
                GROUP BY userId
                ORDER BY total_posts DESC
            """),
            ("Content analysis", """
                SELECT 
                    CASE 
                        WHEN LENGTH(body) < 100 THEN 'Short'
                        WHEN LENGTH(body) < 200 THEN 'Medium'
                        ELSE 'Long'
                    END as post_category,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM posts_for_aggregation
                GROUP BY CASE 
                    WHEN LENGTH(body) < 100 THEN 'Short'
                    WHEN LENGTH(body) < 200 THEN 'Medium'
                    ELSE 'Long'
                END
            """)
        ]
        
        for query_name, query in aggregation_queries:
            print(f"\n📈 {query_name}:")
            result = spark.sql(query)
            result.show()
        
        # Summary of all examples
        print("\n" + "="*80)
        print("📋 SUMMARY: Data fetched across all examples")
        print("="*80)
        
        total_api_calls = (
            partitioned_posts_df.count() +  # 50 parallel requests
            all_posts_df.count() +          # ~100 posts
            all_comments_df.count() +       # ~500 comments  
            all_albums_df.count() +         # ~100 albums
            (combined_pages_df.count() if 'combined_pages_df' in locals() else 0) +  # ~100 more posts
            parallel_df.count()             # 20 more parallel requests
        )
        
        print(f"🎯 Total API responses processed: {total_api_calls}")
        print(f"🚀 Demonstrated partitioning strategies: URL-based, Page-based, Combined datasets")
        print(f"⚡ Performance optimizations: Parallel processing, Batch operations")
        print(f"📊 Advanced analytics: Cross-dataset joins, Aggregations, Content analysis")

        print("\n🎉 All examples completed successfully!")
        print("📚 You can now use this package for large-scale REST API data processing!")
        
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