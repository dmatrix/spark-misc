from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, dense_rank, desc, when
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def create_sales_data(spark):
    """Create simple sales data with ties to demonstrate ranking differences"""
    data = [
        # North Region - with one tie to show ranking differences
        ("Alice Johnson", "North", 95000.0, 2024),
        ("Bob Smith", "North", 87000.0, 2024),
        ("Carol Davis", "North", 87000.0, 2024),  # Tie with Bob
        ("David Wilson", "North", 80000.0, 2024),
        
        # South Region - clear hierarchy
        ("Frank Miller", "South", 110000.0, 2024),
        ("Grace Lee", "South", 95000.0, 2024),
        ("Henry Chen", "South", 85000.0, 2024),
        ("Iris Taylor", "South", 75000.0, 2024),
        
        # East Region - simple tie
        ("Jack Anderson", "East", 90000.0, 2024),
        ("Kelly White", "East", 90000.0, 2024),  # Tie with Jack
        ("Luis Garcia", "East", 85000.0, 2024),
        
        # West Region
        ("Olivia Martinez", "West", 100000.0, 2024),
        ("Paul Rodriguez", "West", 90000.0, 2024),
        ("Quinn Thompson", "West", 80000.0, 2024),
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
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("RankingOperationsDemo") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
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