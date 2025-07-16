#!/usr/bin/env python3
"""
Quick streaming test to verify the news data source streaming functionality.
Runs for only 1 minute to quickly test streaming without long waits.
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from news import NewsDataSource


def test_quick_streaming():
    """Test streaming with a very short duration."""
    print("=" * 50)
    print("Quick Streaming Test (1 minute)")
    print("=" * 50)
    
    # Check for API key
    api_key = os.getenv('NEWSAPI_KEY')
    if not api_key:
        print("❌ NEWSAPI_KEY environment variable not set!")
        return False
    
    print(f"✅ API key found: {api_key[:8]}...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("QuickNewsStreamingTest") \
        .master("local[2]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/quick_news_checkpoint") \
        .getOrCreate()
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        print("✅ News data source registered!")
        
        # Create streaming DataFrame with small batch size for free tier
        streaming_df = spark.readStream.format("news") \
            .option("query", "technology") \
            .option("batch_size", "5") \
            .load()
        
        # Add processing timestamp
        streaming_df = streaming_df.withColumn("processing_time", current_timestamp())
        
        # Select key columns
        display_df = streaming_df.select(
            "source_name", 
            "title", 
            "published_at"
        )
        
        print("✅ Streaming DataFrame created!")
        print("\n🚀 Starting 1-minute streaming test...")
        print("Will fetch news articles and display them...")
        
        # Start the streaming query
        query = display_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "3") \
            .start()
        
        # Run for 1 minute only
        print("⏱️  Running for 1 minute...")
        time.sleep(60)
        
        # Stop the query
        query.stop()
        print("\n✅ Quick streaming test completed!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        spark.stop()


def main():
    """Main function."""
    print("News Data Source - Quick Streaming Test")
    print("Free tier friendly: Uses 5-minute intervals")
    print("Will make 1 API call maximum\n")
    
    success = test_quick_streaming()
    
    if success:
        print("\n🎉 Quick streaming test passed!")
    else:
        print("\n❌ Quick streaming test failed!")
    
    return success


if __name__ == "__main__":
    main() 