#!/usr/bin/env python3
"""
Comprehensive examples for the News Data Source.

This script demonstrates various use cases and patterns for using the 
NewsDataSource with Apache Spark.
"""

import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, length, split, explode, lower, desc, count, avg
from news import NewsDataSource


def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session with optimized configuration."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()


def example_1_basic_usage():
    """Example 1: Basic usage of the news data source."""
    print("=" * 60)
    print("Example 1: Basic Usage")
    print("=" * 60)
    
    spark = create_spark_session("NewsDataSource-Example1")
    
    try:
        # Register the news data source
        spark.dataSource.register(NewsDataSource)
        
        # Read news with default settings
        df = spark.read.format("news").load()
        
        print("News DataFrame Schema:")
        df.printSchema()
        
        print(f"\nTotal articles: {df.count()}")
        
        # Show sample data
        print("\nSample articles:")
        df.select("source_name", "title", "published_at").show(3, truncate=False)
        
    finally:
        spark.stop()


def example_2_advanced_queries():
    """Example 2: Advanced queries and filtering."""
    print("\n" + "=" * 60)
    print("Example 2: Advanced Queries and Filtering")
    print("=" * 60)
    
    spark = create_spark_session("NewsDataSource-Example2")
    
    try:
        spark.dataSource.register(NewsDataSource)
        
        # Read AI and machine learning news
        df = spark.read.format("news") \
            .option("query", "artificial intelligence OR machine learning") \
            .option("sort_by", "relevancy") \
            .option("max_pages", "3") \
            .load()
        
        print(f"AI/ML articles found: {df.count()}")
        
        # Filter for articles with substantial content
        substantial_articles = df.filter(
            (col("content").isNotNull()) & 
            (length(col("content")) > 200)
        )
        
        print(f"Articles with substantial content: {substantial_articles.count()}")
        
        # Categorize articles by title keywords
        categorized_df = substantial_articles.withColumn(
            "category",
            when(lower(col("title")).contains("openai"), "OpenAI")
            .when(lower(col("title")).contains("google"), "Google")
            .when(lower(col("title")).contains("microsoft"), "Microsoft")
            .when(lower(col("title")).contains("meta"), "Meta")
            .otherwise("Other")
        )
        
        print("\nArticles by category:")
        categorized_df.groupBy("category").count().orderBy(desc("count")).show()
        
        # Top sources for AI/ML content
        print("\nTop sources for AI/ML content:")
        df.groupBy("source_name").count().orderBy(desc("count")).show(10)
        
    finally:
        spark.stop()


def example_3_time_series_analysis():
    """Example 3: Time series analysis of news data."""
    print("\n" + "=" * 60)
    print("Example 3: Time Series Analysis")
    print("=" * 60)
    
    spark = create_spark_session("NewsDataSource-Example3")
    
    try:
        spark.dataSource.register(NewsDataSource)
        
        # Read news from the last week
        one_week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        df = spark.read.format("news") \
            .option("query", "cryptocurrency OR bitcoin") \
            .option("from_date", one_week_ago) \
            .option("sort_by", "publishedAt") \
            .option("max_pages", "5") \
            .load()
        
        print(f"Cryptocurrency articles from last week: {df.count()}")
        
        # Group by date
        daily_counts = df.withColumn("date", col("published_at").cast("date")) \
            .groupBy("date") \
            .count() \
            .orderBy("date")
        
        print("\nDaily article counts:")
        daily_counts.show()
        
        # Hourly distribution
        hourly_counts = df.withColumn("hour", col("published_at").cast("string").substr(12, 2)) \
            .groupBy("hour") \
            .count() \
            .orderBy("hour")
        
        print("\nHourly distribution:")
        hourly_counts.show()
        
        # Average article length by source
        article_stats = df.withColumn("content_length", length(col("content"))) \
            .groupBy("source_name") \
            .agg(
                count("*").alias("article_count"),
                avg("content_length").alias("avg_content_length")
            ) \
            .orderBy(desc("article_count"))
        
        print("\nArticle statistics by source:")
        article_stats.show()
        
    finally:
        spark.stop()


def example_4_sentiment_keywords():
    """Example 4: Keyword and sentiment analysis."""
    print("\n" + "=" * 60)
    print("Example 4: Keyword and Sentiment Analysis")
    print("=" * 60)
    
    spark = create_spark_session("NewsDataSource-Example4")
    
    try:
        spark.dataSource.register(NewsDataSource)
        
        # Read tech news
        df = spark.read.format("news") \
            .option("query", "technology startup") \
            .option("sources", "techcrunch,the-verge,wired") \
            .option("max_pages", "4") \
            .load()
        
        print(f"Tech startup articles: {df.count()}")
        
        # Extract keywords from titles
        words_df = df.select(
            "source_name",
            "title",
            explode(split(lower(col("title")), " ")).alias("word")
        ).filter(length(col("word")) > 3)  # Filter out short words
        
        # Count word frequency
        word_counts = words_df.groupBy("word") \
            .count() \
            .orderBy(desc("count"))
        
        print("\nTop keywords in titles:")
        word_counts.show(20)
        
        # Simple sentiment analysis based on keywords
        positive_words = ["innovative", "breakthrough", "success", "growth", "winner", "best"]
        negative_words = ["failure", "problem", "crisis", "worst", "decline", "loss"]
        
        sentiment_df = df.withColumn(
            "sentiment",
            when(
                sum([col("title").contains(word) for word in positive_words]) > 0,
                "positive"
            ).when(
                sum([col("title").contains(word) for word in negative_words]) > 0,
                "negative"
            ).otherwise("neutral")
        )
        
        print("\nSentiment distribution:")
        sentiment_df.groupBy("sentiment").count().show()
        
        # Show examples of each sentiment
        print("\nPositive sentiment examples:")
        sentiment_df.filter(col("sentiment") == "positive") \
            .select("title") \
            .show(3, truncate=False)
        
    finally:
        spark.stop()


def example_5_streaming_to_dashboard():
    """Example 5: Streaming data for real-time dashboard."""
    print("\n" + "=" * 60)
    print("Example 5: Streaming for Real-time Dashboard")
    print("=" * 60)
    
    spark = create_spark_session("NewsDataSource-Example5")
    
    try:
        spark.dataSource.register(NewsDataSource)
        
        # Create streaming DataFrame
        streaming_df = spark.readStream.format("news") \
            .option("query", "breaking news") \
            .option("interval", "60") \
            .load()
        
        # Process streaming data for dashboard
        dashboard_df = streaming_df.select(
            "source_name",
            "title",
            "published_at",
            length("content").alias("content_length"),
            when(col("url_to_image").isNotNull(), "Yes").otherwise("No").alias("has_image")
        )
        
        # Start streaming query
        query = dashboard_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "5") \
            .start()
        
        print("Streaming dashboard data for 2 minutes...")
        import time
        time.sleep(120)
        query.stop()
        
    finally:
        spark.stop()


def example_6_data_pipeline():
    """Example 6: Complete data pipeline with ETL."""
    print("\n" + "=" * 60)
    print("Example 6: Complete Data Pipeline with ETL")
    print("=" * 60)
    
    spark = create_spark_session("NewsDataSource-Example6")
    
    try:
        spark.dataSource.register(NewsDataSource)
        
        # Extract: Read raw news data
        raw_df = spark.read.format("news") \
            .option("query", "climate change") \
            .option("sort_by", "publishedAt") \
            .option("max_pages", "3") \
            .load()
        
        print(f"Extracted {raw_df.count()} articles")
        
        # Transform: Clean and enrich data
        cleaned_df = raw_df.filter(
            col("title").isNotNull() & 
            col("description").isNotNull() &
            col("published_at").isNotNull()
        )
        
        # Add derived columns
        enriched_df = cleaned_df.withColumn(
            "title_word_count", 
            col("title").split(" ").size()
        ).withColumn(
            "description_word_count",
            col("description").split(" ").size()
        ).withColumn(
            "processing_date",
            col("published_at").cast("date")
        ).withColumn(
            "source_type",
            when(col("source_name").isin("bbc-news", "cnn", "reuters"), "mainstream")
            .when(col("source_name").isin("techcrunch", "the-verge"), "tech")
            .otherwise("other")
        )
        
        print(f"Cleaned and enriched {enriched_df.count()} articles")
        
        # Load: Save to different formats
        output_base = "/tmp/news_pipeline_output"
        
        # Save as Parquet (for analytics)
        enriched_df.write \
            .mode("overwrite") \
            .partitionBy("processing_date", "source_type") \
            .parquet(f"{output_base}/parquet")
        
        # Save as JSON (for API consumption)
        enriched_df.select("title", "description", "url", "published_at", "source_name") \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .json(f"{output_base}/json")
        
        # Create summary statistics
        summary_df = enriched_df.groupBy("source_type", "processing_date") \
            .agg(
                count("*").alias("article_count"),
                avg("title_word_count").alias("avg_title_words"),
                avg("description_word_count").alias("avg_description_words")
            )
        
        print("\nPipeline Summary Statistics:")
        summary_df.show()
        
        # Save summary
        summary_df.write \
            .mode("overwrite") \
            .csv(f"{output_base}/summary", header=True)
        
        print(f"Pipeline completed! Data saved to {output_base}")
        
    finally:
        spark.stop()


def example_7_multi_source_comparison():
    """Example 7: Multi-source news comparison."""
    print("\n" + "=" * 60)
    print("Example 7: Multi-source News Comparison")
    print("=" * 60)
    
    spark = create_spark_session("NewsDataSource-Example7")
    
    try:
        spark.dataSource.register(NewsDataSource)
        
        # Read same topic from different sources
        tech_sources = ["techcrunch", "the-verge", "wired", "ars-technica"]
        business_sources = ["reuters", "bloomberg", "fortune"]
        
        # Tech perspective
        tech_df = spark.read.format("news") \
            .option("query", "electric vehicles") \
            .option("sources", ",".join(tech_sources)) \
            .option("max_pages", "2") \
            .load() \
            .withColumn("perspective", col("source_name").cast("string"))
        
        # Business perspective
        business_df = spark.read.format("news") \
            .option("query", "electric vehicles") \
            .option("sources", ",".join(business_sources)) \
            .option("max_pages", "2") \
            .load() \
            .withColumn("perspective", col("source_name").cast("string"))
        
        # Union both perspectives
        combined_df = tech_df.union(business_df)
        
        print(f"Total articles from both perspectives: {combined_df.count()}")
        
        # Compare coverage
        coverage_comparison = combined_df.groupBy("source_name") \
            .agg(
                count("*").alias("article_count"),
                avg(length("description")).alias("avg_description_length")
            ) \
            .orderBy(desc("article_count"))
        
        print("\nCoverage comparison:")
        coverage_comparison.show()
        
        # Find common themes
        print("\nSample articles from different perspectives:")
        combined_df.select("source_name", "title") \
            .orderBy("source_name") \
            .show(10, truncate=False)
        
    finally:
        spark.stop()


def main():
    """Run all examples."""
    print("News Data Source - Comprehensive Examples")
    print("=========================================")
    
    # Check for API key
    api_key = os.getenv('NEWSAPI_KEY')
    if not api_key:
        print("❌ NEWSAPI_KEY environment variable not set!")
        print("Please set your NewsAPI key:")
        print("export NEWSAPI_KEY=your_api_key_here")
        print("Get your free API key at: https://newsapi.org/register")
        return
    
    print(f"✅ API key found: {api_key[:8]}...")
    
    # Run examples
    examples = [
        example_1_basic_usage,
        example_2_advanced_queries,
        example_3_time_series_analysis,
        example_4_sentiment_keywords,
        example_5_streaming_to_dashboard,
        example_6_data_pipeline,
        example_7_multi_source_comparison
    ]
    
    for i, example in enumerate(examples, 1):
        try:
            example()
            print(f"\n✅ Example {i} completed successfully!")
        except Exception as e:
            print(f"\n❌ Example {i} failed: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n🎉 All examples completed!")


if __name__ == "__main__":
    main() 