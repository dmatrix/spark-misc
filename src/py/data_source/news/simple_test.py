#!/usr/bin/env python3
"""
Simple test to verify the news data source is working.
"""

import os
from pyspark.sql import SparkSession
from main import NewsDataSource

def simple_test():
    """Simple test of the news data source."""
    print("🔥 Testing News Data Source for Apache Spark 4.0")
    print("=" * 50)
    
    # Check API key
    api_key = os.getenv('NEWSAPI_KEY')
    if not api_key:
        print("❌ NEWSAPI_KEY environment variable not set!")
        return False
    
    print(f"✅ API key found: {api_key[:8]}...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("NewsDataSourceSimpleTest") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        print("✅ News data source registered successfully")
        
        # Read news with basic query
        print("\n📰 Reading tech news...")
        df = spark.read.format("news") \
            .option("query", "technology") \
            .option("max_pages", "1") \
            .load()
        
        # Show basic info
        print(f"✅ Successfully loaded {df.count()} articles")
        print(f"✅ Schema: {len(df.columns)} columns")
        
        # Show sample data
        print("\n📋 Sample articles:")
        df.select("source_name", "title", "published_at").show(3, truncate=False)
        
        # Test basic transformations
        print("\n📊 Top sources:")
        df.groupBy("source_name").count().orderBy("count", ascending=False).show(5)
        
        print("\n🎉 All tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    success = simple_test()
    exit(0 if success else 1) 