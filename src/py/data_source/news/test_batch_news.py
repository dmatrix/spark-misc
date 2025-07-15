#!/usr/bin/env python3
"""
Test script for batch reading news data using the custom news data source.

This script demonstrates how to use the NewsDataSource for batch processing
of news articles from NewsAPI.org.
"""

import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from main import NewsDataSource


def test_batch_news_basic():
    """Test basic batch news reading."""
    print("=" * 60)
    print("Testing Basic Batch News Reading")
    print("=" * 60)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("NewsDataSourceBatchTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        
        # Test 1: Basic news reading with default parameters
        print("\n1. Reading news with default parameters...")
        df = spark.read.format("news").load()
        
        print(f"Schema: {df.schema}")
        print(f"Number of articles: {df.count()}")
        
        # Show first few articles
        print("\nFirst 5 articles:")
        df.select("source_name", "title", "published_at").show(5, truncate=False)
        
        # Test 2: Reading news with specific query
        print("\n2. Reading news about 'artificial intelligence'...")
        df_ai = spark.read.format("news") \
            .option("query", "artificial intelligence") \
            .option("max_pages", "1") \
            .load()
        
        print(f"Number of AI articles: {df_ai.count()}")
        df_ai.select("source_name", "title").show(3, truncate=False)
        
        # Test 3: Reading news from specific sources
        print("\n3. Reading news from specific sources...")
        df_sources = spark.read.format("news") \
            .option("query", "technology") \
            .option("sources", "techcrunch,the-verge") \
            .option("max_pages", "1") \
            .load()
        
        print(f"Number of tech articles from TechCrunch and The Verge: {df_sources.count()}")
        df_sources.select("source_name", "title").show(3, truncate=False)
        
        # Test 4: Reading news with date range
        print("\n4. Reading news with date range...")
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y-%m-%d')
        
        df_date = spark.read.format("news") \
            .option("query", "python") \
            .option("from_date", yesterday) \
            .option("to_date", today) \
            .option("max_pages", "1") \
            .load()
        
        print(f"Number of Python articles from yesterday to today: {df_date.count()}")
        if df_date.count() > 0:
            df_date.select("source_name", "title", "published_at").show(3, truncate=False)
        
        # Test 5: News analytics
        print("\n5. Basic news analytics...")
        
        # Count articles by source
        print("Articles by source:")
        df.groupBy("source_name").count().orderBy("count", ascending=False).show(10)
        
        # Count articles by day
        print("Articles by day:")
        df.withColumn("date", df.published_at.cast("date")) \
          .groupBy("date").count().orderBy("date", ascending=False).show(10)
        
        print("\n✅ All batch tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during batch testing: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        spark.stop()
    
    return True


def test_batch_news_advanced():
    """Test advanced batch news reading with filters and transformations."""
    print("\n" + "=" * 60)
    print("Testing Advanced Batch News Reading")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("NewsDataSourceAdvancedTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        
        # Test 1: Multiple queries with different sorting
        print("\n1. Testing different sorting options...")
        
        for sort_by in ['publishedAt', 'relevancy', 'popularity']:
            print(f"\nSorting by {sort_by}:")
            df = spark.read.format("news") \
                .option("query", "climate change") \
                .option("sort_by", sort_by) \
                .option("max_pages", "1") \
                .load()
            
            df.select("title", "published_at").show(3, truncate=False)
        
        # Test 2: SQL queries on news data
        print("\n2. Running SQL queries on news data...")
        
        df = spark.read.format("news") \
            .option("query", "startup") \
            .option("max_pages", "1") \
            .load()
        
        df.createOrReplaceTempView("news")
        
        # Find articles with certain keywords in title
        print("Articles with 'funding' in title:")
        funding_articles = spark.sql("""
            SELECT source_name, title, published_at 
            FROM news 
            WHERE LOWER(title) LIKE '%funding%'
            ORDER BY published_at DESC
            LIMIT 5
        """)
        funding_articles.show(truncate=False)
        
        # Get word count statistics
        print("\nTitle length statistics:")
        spark.sql("""
            SELECT 
                AVG(LENGTH(title)) as avg_title_length,
                MIN(LENGTH(title)) as min_title_length,
                MAX(LENGTH(title)) as max_title_length
            FROM news
        """).show()
        
        # Test 3: Joining with other data
        print("\n3. Testing joins with other data...")
        
        # Create a simple lookup table for source categories
        source_categories = spark.createDataFrame([
            ('techcrunch', 'technology'),
            ('bbc-news', 'general'),
            ('cnn', 'general'),
            ('reuters', 'business'),
            ('the-verge', 'technology'),
            ('wired', 'technology')
        ], ['source_id', 'category'])
        
        # Join news with categories
        joined_df = df.join(source_categories, df.source_id == source_categories.source_id, "left")
        
        print("Articles by category:")
        joined_df.groupBy("category").count().show()
        
        # Test 4: Data export
        print("\n4. Testing data export...")
        
        # Save to parquet format
        output_path = "/tmp/news_data_test"
        df.select("source_name", "title", "url", "published_at") \
          .coalesce(1) \
          .write \
          .mode("overwrite") \
          .parquet(output_path)
        
        # Read back and verify
        saved_df = spark.read.parquet(output_path)
        print(f"Saved {saved_df.count()} articles to {output_path}")
        
        print("\n✅ All advanced batch tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during advanced testing: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        spark.stop()
    
    return True


def main():
    """Main function to run all batch tests."""
    print("News Data Source - Batch Reading Tests")
    print("=====================================")
    
    # Check for API key
    api_key = os.getenv('NEWSAPI_KEY')
    if not api_key:
        print("❌ NEWSAPI_KEY environment variable not set!")
        print("Please set your NewsAPI key:")
        print("export NEWSAPI_KEY=your_api_key_here")
        print("Get your free API key at: https://newsapi.org/register")
        return False
    
    print(f"✅ API key found: {api_key[:8]}...")
    
    # Run tests
    success = True
    
    success &= test_batch_news_basic()
    success &= test_batch_news_advanced()
    
    if success:
        print("\n🎉 All batch tests passed!")
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)
    
    return success


if __name__ == "__main__":
    main() 