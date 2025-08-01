"""
PySpark Window Functions Demo: Ranking Operations

USE CASE:
This demo showcases ranking functions (ROW_NUMBER, RANK, DENSE_RANK) for performance
analysis, employee evaluation, and bonus calculations. Perfect for scenarios where you
need to rank employees, identify top performers, or handle tied values in rankings.

KEY BENEFITS:
• Handle tied values in three different ways based on business requirements
• Create fair and transparent performance evaluation systems
• Calculate performance-based bonuses and incentives
• Identify top N performers across different regions/departments
• Generate leaderboards and performance dashboards

WINDOW FUNCTIONS DEMONSTRATED:
• ROW_NUMBER() - Always unique ranks (1,2,3,4...)
• RANK() - Same rank for ties, skips next ranks (1,1,3,4...)
• DENSE_RANK() - Same rank for ties, no gaps (1,1,2,3...)

REAL-WORLD APPLICATIONS:
• Sales performance rankings and bonus calculations
• Employee performance reviews and promotions
• Academic grading and class rankings
• Sports leaderboards and tournament rankings
• Product popularity rankings in e-commerce

BUSINESS SCENARIOS COVERED:
• Regional sales performance analysis
• Top performer identification
• Tie-breaking strategies for fair evaluation
• Performance-based compensation systems
• Multi-level ranking comparisons

Usage: python ranking_operations_demo.py

NOTE: This demo uses Spark Connect.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, dense_rank, desc, when
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def create_sales_data(spark):
    """Create expanded sales data with ties to demonstrate ranking differences"""
    data = [
        # North Region - 12 salespeople with strategic ties
        ("Alice Johnson", "North", 145000.0, 2024),     # Top performer
        ("Bob Smith", "North", 132000.0, 2024),
        ("Carol Davis", "North", 128000.0, 2024),
        ("David Wilson", "North", 118000.0, 2024),
        ("Emma Thompson", "North", 115000.0, 2024),
        ("Frank Roberts", "North", 108000.0, 2024),
        ("Grace Kim", "North", 102000.0, 2024),
        ("Henry Park", "North", 98000.0, 2024),
        ("Iris Chang", "North", 95000.0, 2024),
        ("Jack Turner", "North", 87000.0, 2024),
        ("Kelly Brown", "North", 87000.0, 2024),        # Tie with Jack
        ("Liam Davis", "North", 82000.0, 2024),
        
        # South Region - 12 salespeople with clear performance tiers
        ("Maria Rodriguez", "South", 155000.0, 2024),   # Regional champion
        ("Nathan Wilson", "South", 142000.0, 2024),
        ("Olivia Martinez", "South", 138000.0, 2024),
        ("Paul Anderson", "South", 125000.0, 2024),
        ("Quinn Johnson", "South", 118000.0, 2024),
        ("Ryan Miller", "South", 112000.0, 2024),
        ("Sarah Lee", "South", 105000.0, 2024),
        ("Thomas Chen", "South", 98000.0, 2024),
        ("Uma Patel", "South", 92000.0, 2024),
        ("Victor Garcia", "South", 88000.0, 2024),
        ("Wendy Liu", "South", 85000.0, 2024),
        ("Xavier Smith", "South", 78000.0, 2024),
        
        # East Region - 12 salespeople with multiple ties
        ("Ava Thompson", "East", 138000.0, 2024),       # Top performer
        ("Ben Rodriguez", "East", 125000.0, 2024),
        ("Chloe Wilson", "East", 122000.0, 2024),
        ("Dylan Martinez", "East", 115000.0, 2024),
        ("Ella Johnson", "East", 108000.0, 2024),
        ("Felix Anderson", "East", 102000.0, 2024),
        ("Gina Miller", "East", 96000.0, 2024),
        ("Hugo Chen", "East", 90000.0, 2024),
        ("Ivy Garcia", "East", 90000.0, 2024),          # Tie with Hugo
        ("James Lee", "East", 85000.0, 2024),
        ("Kate Patel", "East", 82000.0, 2024),
        ("Leo Smith", "East", 79000.0, 2024),
        
        # West Region - 12 salespeople with performance clusters
        ("Maya Johnson", "West", 165000.0, 2024),       # Star performer
        ("Noah Wilson", "West", 148000.0, 2024),
        ("Sophia Martinez", "West", 140000.0, 2024),
        ("Tyler Anderson", "West", 132000.0, 2024),
        ("Zoe Rodriguez", "West", 128000.0, 2024),
        ("Alex Miller", "West", 122000.0, 2024),
        ("Bella Chen", "West", 115000.0, 2024),
        ("Chris Garcia", "West", 110000.0, 2024),
        ("Diana Lee", "West", 105000.0, 2024),
        ("Ethan Patel", "West", 98000.0, 2024),
        ("Fiona Smith", "West", 92000.0, 2024),
        ("Gabriel Liu", "West", 88000.0, 2024),
    ]
    
    schema = StructType([
        StructField("salesperson", StringType(), False),
        StructField("region", StringType(), False),
        StructField("sales_amount", DoubleType(), False),
        StructField("year", IntegerType(), False)
    ])
    
    return spark.createDataFrame(data, schema)

def demonstrate_ranking_functions(df):
    """Show the differences between row_number, rank, and dense_rank"""
    
    print("🎯 RANKING OPERATIONS DEMO")
    print("=" * 60)
    
    # Create window specification: partition by region, order by sales (highest first)
    window_spec = Window.partitionBy("region").orderBy(desc("sales_amount"))
    
    # Add three types of ranking functions:
    # - row_number: Always unique ranks (1,2,3,4...)
    # - rank: Skips ranks after ties (1,1,3,4...)  
    # - dense_rank: No gaps in ranks (1,1,2,3...)
    df_ranked = df.withColumn("row_number", row_number().over(window_spec)) \
                  .withColumn("rank", rank().over(window_spec)) \
                  .withColumn("dense_rank", dense_rank().over(window_spec))
    
    print("📊 Sales Rankings by Region (Highest to Lowest)")
    print("-" * 60)
    df_ranked.select("region", "salesperson", "sales_amount", 
                     "row_number", "rank", "dense_rank") \
             .orderBy("region", "row_number") \
             .show(20, truncate=False)
    
    return df_ranked

def get_top_performers(df_ranked):
    """Get top 3 performers per region using different ranking methods"""
    
    print("\n🏆 TOP 3 PERFORMERS PER REGION")
    print("=" * 60)
    
    # Method 1: Using row_number (always exactly 3 per region)
    print("\n📍 Method 1: Top 3 using ROW_NUMBER (exactly 3 per region)")
    print("-" * 50)
    top3_row_number = df_ranked.filter(col("row_number") <= 3) \
                              .select("region", "salesperson", "sales_amount", "row_number") \
                              .orderBy("region", "row_number")
    top3_row_number.show(truncate=False)
    
    # Method 2: Using rank (may get more than 3 due to ties)
    print("\n📍 Method 2: Top 3 using RANK (includes ties)")
    print("-" * 50)
    top3_rank = df_ranked.filter(col("rank") <= 3) \
                         .select("region", "salesperson", "sales_amount", "rank") \
                         .orderBy("region", "rank")
    top3_rank.show(truncate=False)
    
    # Method 3: Using dense_rank (most inclusive with ties)
    print("\n📍 Method 3: Top 3 using DENSE_RANK (most inclusive)")
    print("-" * 50)
    top3_dense_rank = df_ranked.filter(col("dense_rank") <= 3) \
                               .select("region", "salesperson", "sales_amount", "dense_rank") \
                               .orderBy("region", "dense_rank")
    top3_dense_rank.show(truncate=False)
    
    # Show count differences
    print("\n📈 COMPARISON: Number of 'Top 3' records by method")
    print("-" * 50)
    print(f"ROW_NUMBER method: {top3_row_number.count()} records")
    print(f"RANK method: {top3_rank.count()} records")
    print(f"DENSE_RANK method: {top3_dense_rank.count()} records")

def real_world_example(df):
    """Show practical business application"""
    
    print("\n💼 REAL-WORLD APPLICATION: Performance Bonuses")
    print("=" * 60)
    
    window_spec = Window.partitionBy("region").orderBy(desc("sales_amount"))
    
    # Create performance tiers and bonuses based on regional ranking:
    # - Get regional rank for each salesperson
    # - Assign performance tier (Top Performer vs Standard)
    # - Calculate bonus percentage based on rank
    df_with_tiers = df.withColumn("regional_rank", row_number().over(window_spec)) \
                      .withColumn("performance_tier",
                                 when(col("regional_rank") == 1, "🥇 Top Performer")
                                 .otherwise("📊 Standard")) \
                      .withColumn("bonus_percentage",
                                 when(col("regional_rank") == 1, 15.0)
                                 .otherwise(5.0))
    
    print("🎖️ Performance Tiers and Bonus Calculations")
    print("-" * 50)
    df_with_tiers.select("region", "salesperson", "sales_amount", 
                         "regional_rank", "performance_tier", "bonus_percentage") \
                 .orderBy("region", "regional_rank") \
                 .show(20, truncate=False)
    
    # Summary by region
    print("\n📊 Regional Performance Summary")
    print("-" * 50)
    from pyspark.sql.functions import sum as spark_sum, avg as spark_avg, count
    
    summary = df_with_tiers.groupBy("region") \
                          .agg(count("*").alias("total_salespeople"),
                               spark_sum("sales_amount").alias("total_regional_sales"),
                               spark_avg("sales_amount").alias("avg_regional_sales")) \
                          .orderBy(desc("total_regional_sales"))
    
    summary.show(truncate=False)

if __name__ == "__main__":
    # Initialize Spark Connect
    spark = SparkSession.builder \
        .appName("RankingOperationsDemo") \
        .config("spark.api.mode", "connect") \
        .remote("local[*]") \
        .getOrCreate()
    
    # Note: spark.sparkContext is not available in Spark Connect
    # Log level configuration is handled server-side
    
    # Create sample data
    df = create_sales_data(spark)
    
    print("📋 Original Sales Data")
    print("-" * 30)
    df.orderBy("region", desc("sales_amount")).show(truncate=False)
    
    # Demonstrate ranking functions
    df_ranked = demonstrate_ranking_functions(df)
    
    # Show top performers using different methods
    get_top_performers(df_ranked)
    
    # Real-world business application
    real_world_example(df)
    
    print("\n✅ DEMO COMPLETED!")
    print("\n💡 Key Takeaways:")
    print("• ROW_NUMBER: Always unique ranks (1,2,3,4,5...)")
    print("• RANK: Same rank for ties, skips next ranks (1,1,3,4,5...)")
    print("• DENSE_RANK: Same rank for ties, no gaps (1,1,2,3,4...)")
    print("• Choose based on business requirements for handling ties")
    
    spark.stop() 