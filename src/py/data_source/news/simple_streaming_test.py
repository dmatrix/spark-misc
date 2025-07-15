#!/usr/bin/env python3
"""
Simple test script for streaming news data using the custom news data source.

This script demonstrates basic streaming functionality with real-time news updates.
Run with: uv run simple_streaming_test.py
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from main import NewsDataSource


def test_simple_streaming():
    """Test basic streaming news reading with console output."""
    print("=" * 60)
    print("Simple Streaming News Test")
    print("=" * 60)
    
    # Check for API key
    api_key = os.getenv('NEWSAPI_KEY')
    if not api_key:
        print("❌ NEWSAPI_KEY environment variable not set!")
        print("Please set your NewsAPI key:")
        print("export NEWSAPI_KEY=your_api_key_here")
        print("Get your free API key at: https://newsapi.org/register")
        return False
    
    print(f"✅ API key found: {api_key[:8]}...")
    print("📊 Configured for NewsAPI free tier (conservative rate limiting)")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SimpleNewsStreamingTest") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/simple_news_checkpoint") \
        .getOrCreate()
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        print("✅ News data source registered successfully!")
        
        # Create streaming DataFrame
        print("\n📡 Creating streaming DataFrame...")
        streaming_df = spark.readStream.format("news") \
            .option("query", "technology") \
            .option("interval", "300") \
            .load()
        
        # Add processing timestamp
        streaming_df = streaming_df.withColumn("processing_time", current_timestamp())
        
        # Select columns to display
        display_df = streaming_df.select(
            "source_name", 
            "title", 
            "published_at", 
            "processing_time"
        )
        
        print("✅ Streaming DataFrame created!")
        print(f"📊 Schema: {len(streaming_df.columns)} columns")
        
        # Start the streaming query
        print("\n🚀 Starting streaming query...")
        print("This will run for 6 minutes (free tier friendly) and show real-time news updates...")
        print("API calls will be made every 5 minutes (interval=300s)")
        print("💡 You'll see the first batch of articles immediately, then updates every 5 minutes")
        print("Press Ctrl+C to stop early\n")
        
        query = display_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "5") \
            .start()
        
        # Let it run for 6 minutes (enough for 1-2 API calls)
        try:
            time.sleep(360)
        except KeyboardInterrupt:
            print("\n⏹️  Stopping stream due to user interruption...")
        
        # Stop the query
        query.stop()
        print("\n✅ Streaming test completed successfully!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during streaming test: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        spark.stop()
        print("🔄 Spark session stopped.")


def main():
    """Main function to run the simple streaming test."""
    print("News Data Source - Simple Streaming Test")
    print("========================================")
    
    # Warning about rate limits
    print("\n⚠️  FREE TIER FRIENDLY CONFIGURATION:")
    print("- NewsAPI free tier: 1000 requests/day total")
    print("- Top headlines endpoint: 500 requests/day")
    print("- This test uses 5-minute intervals (conservative)")
    print("- Will make approximately 1-2 API calls total")
    print("- Safe for free tier usage")
    
    success = test_simple_streaming()
    
    if success:
        print("\n🎉 Simple streaming test passed!")
    else:
        print("\n❌ Simple streaming test failed!")
        return False
    
    return True


if __name__ == "__main__":
    main() 