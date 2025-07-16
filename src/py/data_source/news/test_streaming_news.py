#!/usr/bin/env python3
"""
Test script for streaming news data using the custom news data source.

This script demonstrates how to use the NewsDataSource for streaming processing
of real-time news articles from NewsAPI.org.
"""

import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, desc, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from news import NewsDataSource


def test_streaming_news_basic():
    """Test basic streaming news reading."""
    print("=" * 60)
    print("Testing Basic Streaming News Reading")
    print("=" * 60)
    
    # Create Spark session with streaming configuration
    spark = SparkSession.builder \
        .appName("NewsDataSourceStreamingTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/news_streaming_checkpoint") \
        .getOrCreate()
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        
        # Test 1: Basic streaming with console output
        print("\n1. Basic streaming with console output...")
        print("Starting stream (will run for 60 seconds)...")
        
        streaming_df = spark.readStream.format("news") \
            .option("query", "breaking news") \
            .option("interval", "30") \
            .load()
        
        # Start the streaming query with console output
        query = streaming_df.select("source_name", "title", "published_at") \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "3") \
            .start()
        
        # Let it run for 60 seconds
        time.sleep(60)
        query.stop()
        
        print("\n✅ Basic streaming test completed!")
        
    except Exception as e:
        print(f"❌ Error during basic streaming test: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        spark.stop()
    
    return True


def test_streaming_news_advanced():
    """Test advanced streaming news with windowed aggregations."""
    print("\n" + "=" * 60)
    print("Testing Advanced Streaming News Processing")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("NewsDataSourceAdvancedStreamingTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/news_streaming_checkpoint_advanced") \
        .getOrCreate()
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        
        # Test 1: Streaming with aggregations
        print("\n1. Streaming with windowed aggregations...")
        
        streaming_df = spark.readStream.format("news") \
            .option("query", "technology") \
            .option("interval", "30") \
            .load()
        
        # Add processing timestamp
        streaming_df = streaming_df.withColumn("processing_time", current_timestamp())
        
        # Window aggregation - count articles by source in 2-minute windows
        windowed_counts = streaming_df \
            .groupBy(
                window(col("processing_time"), "2 minutes", "1 minute"),
                col("source_name")
            ) \
            .agg(count("*").alias("article_count")) \
            .orderBy(desc("article_count"))
        
        # Start windowed aggregation query
        query1 = windowed_counts \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "10") \
            .start()
        
        # Test 2: Streaming with filtering
        print("\n2. Streaming with content filtering...")
        
        # Filter for articles containing specific keywords
        filtered_df = streaming_df.filter(
            col("title").contains("AI") | 
            col("title").contains("artificial intelligence") |
            col("title").contains("machine learning")
        )
        
        query2 = filtered_df.select("source_name", "title", "processing_time") \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "5") \
            .start()
        
        # Let both queries run for 90 seconds
        time.sleep(90)
        
        # Stop queries
        query1.stop()
        query2.stop()
        
        print("\n✅ Advanced streaming tests completed!")
        
    except Exception as e:
        print(f"❌ Error during advanced streaming test: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        spark.stop()
    
    return True


def test_streaming_news_to_storage():
    """Test streaming news data to storage (Delta/Parquet)."""
    print("\n" + "=" * 60)
    print("Testing Streaming News to Storage")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("NewsDataSourceStorageTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/news_streaming_checkpoint_storage") \
        .getOrCreate()
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        
        # Test 1: Stream to Parquet
        print("\n1. Streaming to Parquet files...")
        
        streaming_df = spark.readStream.format("news") \
            .option("query", "startup") \
            .option("interval", "45") \
            .load()
        
        # Add processing timestamp for partitioning
        streaming_df = streaming_df.withColumn("processing_time", current_timestamp())
        
        # Write to parquet with partitioning
        output_path = "/tmp/streaming_news_output"
        
        query = streaming_df.select(
            "source_name", "title", "url", "published_at", "processing_time"
        ) \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", "/tmp/news_streaming_checkpoint_parquet") \
        .trigger(processingTime="30 seconds") \
        .start()
        
        # Let it run for 2 minutes
        print("Streaming to storage for 2 minutes...")
        time.sleep(120)
        query.stop()
        
        # Verify the written data
        print("\n2. Verifying written data...")
        if os.path.exists(output_path):
            saved_df = spark.read.parquet(output_path)
            count = saved_df.count()
            print(f"Successfully saved {count} articles to {output_path}")
            
            if count > 0:
                print("\nSample of saved data:")
                saved_df.select("source_name", "title", "processing_time").show(5, truncate=False)
        else:
            print("No data was written to storage.")
        
        print("\n✅ Storage streaming test completed!")
        
    except Exception as e:
        print(f"❌ Error during storage streaming test: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        spark.stop()
    
    return True


def test_streaming_news_multiple_sources():
    """Test streaming from multiple news sources simultaneously."""
    print("\n" + "=" * 60)
    print("Testing Multiple Source Streaming")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("NewsDataSourceMultiStreamTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/news_streaming_checkpoint_multi") \
        .getOrCreate()
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        
        # Stream 1: Technology news
        print("\n1. Setting up multiple news streams...")
        
        tech_stream = spark.readStream.format("news") \
            .option("query", "technology") \
            .option("sources", "techcrunch,the-verge,wired") \
            .option("interval", "60") \
            .load()
        
        # Stream 2: Business news
        business_stream = spark.readStream.format("news") \
            .option("query", "business") \
            .option("sources", "reuters,bloomberg") \
            .option("interval", "60") \
            .load()
        
        # Add stream identifiers
        tech_stream = tech_stream.withColumn("stream_type", col("source_name").cast("string"))
        business_stream = business_stream.withColumn("stream_type", col("source_name").cast("string"))
        
        # Union streams (Note: In real scenarios, you'd want to use structured streaming unions)
        print("\n2. Processing combined streams...")
        
        # Start separate queries for each stream
        tech_query = tech_stream.select("stream_type", "source_name", "title") \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "3") \
            .start()
        
        business_query = business_stream.select("stream_type", "source_name", "title") \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "3") \
            .start()
        
        # Let both streams run for 2 minutes
        time.sleep(120)
        
        # Stop queries
        tech_query.stop()
        business_query.stop()
        
        print("\n✅ Multiple source streaming test completed!")
        
    except Exception as e:
        print(f"❌ Error during multiple source streaming test: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        spark.stop()
    
    return True


def main():
    """Main function to run all streaming tests."""
    print("News Data Source - Streaming Tests")
    print("==================================")
    
    # Check for API key
    api_key = os.getenv('NEWSAPI_KEY')
    if not api_key:
        print("❌ NEWSAPI_KEY environment variable not set!")
        print("Please set your NewsAPI key:")
        print("export NEWSAPI_KEY=your_api_key_here")
        print("Get your free API key at: https://newsapi.org/register")
        return False
    
    print(f"✅ API key found: {api_key[:8]}...")
    
    # Warning about rate limits
    print("\n⚠️  Note: NewsAPI has rate limits (1000 requests/day for free tier)")
    print("These streaming tests will make frequent API calls.")
    print("Consider using a paid plan for production streaming use cases.")
    
    choice = input("\nContinue with streaming tests? (y/n): ").lower()
    if choice != 'y':
        print("Streaming tests cancelled.")
        return True
    
    # Run tests
    success = True
    
    success &= test_streaming_news_basic()
    success &= test_streaming_news_advanced()
    success &= test_streaming_news_to_storage()
    success &= test_streaming_news_multiple_sources()
    
    if success:
        print("\n🎉 All streaming tests passed!")
    else:
        print("\n❌ Some streaming tests failed!")
        sys.exit(1)
    
    return success


if __name__ == "__main__":
    main() 