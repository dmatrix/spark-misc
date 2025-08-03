#!/usr/bin/env python3
"""
Test file to verify all WINDOW_FUNCTIONS_STRATEGY_GUIDE.md code examples are runnable
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def create_test_spark_session():
    """Create a test Spark session"""
    return SparkSession.builder \
        .appName("TestStrategyCode") \
        .remote("local[*]") \
        .getOrCreate()

def create_sample_data(spark):
    """Create sample data for testing strategy guide examples"""
    schema = StructType([
        StructField("region", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("daily_sales", DoubleType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("revenue", DoubleType(), True)
    ])
    
    data = [
        ("North", "Electronics", 1, "2024-01-01", 500.0, 1000.0, 800.0),
        ("South", "Clothing", 2, "2024-01-02", 600.0, 1500.0, 900.0),
        ("North", "Electronics", 3, "2024-01-03", 550.0, 800.0, 750.0),
        ("West", "Home", 4, "2024-01-04", 700.0, 1200.0, 1000.0),
        ("South", "Clothing", 5, "2024-01-05", 480.0, 900.0, 700.0),
        ("North", "Books", 6, "2024-01-06", 520.0, 1100.0, 850.0),
        ("West", "Home", 7, "2024-01-07", 630.0, 1300.0, 950.0)
    ]
    
    return spark.createDataFrame(data, schema)

def test_spark_connect_configuration():
    """Test Spark Connect configuration from strategy guide"""
    print("Testing Spark Connect configuration...")
    
    # Test the configuration code from strategy guide
    spark = SparkSession.builder \
        .appName("WindowFunctionAnalysis") \
        .remote("local[*]") \
        .getOrCreate()
    
    # Verify session is created successfully
    print(f"✅ Spark session created: {spark.sparkContext.appName}")
    print(f"✅ Spark version: {spark.version}")
    return spark

def test_partitioning_strategies():
    """Test partitioning strategies from strategy guide"""
    print("Testing partitioning strategies...")
    
    spark = create_test_spark_session()
    df = create_sample_data(spark)
    
    # Test different partitioning approaches mentioned in strategy guide
    
    # 1. Single column partitioning
    region_window = Window.partitionBy("region").orderBy("sales_amount")
    df_region = df.withColumn("region_rank", rank().over(region_window))
    
    # 2. Multi-column partitioning  
    multi_window = Window.partitionBy("region", "product_category").orderBy("sales_amount")
    df_multi = df.withColumn("category_rank", rank().over(multi_window))
    
    # 3. No partitioning (global)
    global_window = Window.orderBy("sales_amount")
    df_global = df.withColumn("global_rank", rank().over(global_window))
    
    df_combined = df_region.join(df_multi, ["region", "product_category", "customer_id"]) \
                           .join(df_global, ["region", "product_category", "customer_id"])
    
    df_combined.select("region", "product_category", "sales_amount", 
                      "region_rank", "category_rank", "global_rank").show()
    
    print("✅ Partitioning strategies test passed")

def test_frame_specifications():
    """Test frame specifications from strategy guide"""
    print("Testing frame specifications...")
    
    spark = create_test_spark_session()
    df = create_sample_data(spark)
    
    # Different frame types mentioned in strategy guide
    
    # 1. Unbounded preceding to current row
    running_window = Window.partitionBy("region").orderBy("transaction_date") \
                           .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    # 2. Fixed window size
    moving_window = Window.partitionBy("region").orderBy("transaction_date") \
                          .rowsBetween(-2, Window.currentRow)
    
    # 3. Full partition window
    full_window = Window.partitionBy("region").orderBy("transaction_date") \
                        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    df_frames = (df.withColumn("running_sum", sum("sales_amount").over(running_window))
                   .withColumn("moving_avg", avg("sales_amount").over(moving_window))
                   .withColumn("total_revenue", sum("sales_amount").over(full_window)))
    
    df_frames.select("region", "transaction_date", "sales_amount", 
                    "running_sum", "moving_avg", "total_revenue").show()
    
    print("✅ Frame specifications test passed")

def test_common_patterns():
    """Test common patterns from strategy guide"""
    print("Testing common window function patterns...")
    
    spark = create_test_spark_session()
    df = create_sample_data(spark)
    
    # Pattern 1: Ranking with tie handling
    ranking_window = Window.partitionBy("region").orderBy(desc("sales_amount"))
    df_ranking = (df.withColumn("rank", rank().over(ranking_window))
                    .withColumn("dense_rank", dense_rank().over(ranking_window))
                    .withColumn("row_number", row_number().over(ranking_window)))
    
    # Pattern 2: Running calculations
    running_window = Window.partitionBy("region").orderBy("transaction_date") \
                           .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_running = (df.withColumn("running_total", sum("sales_amount").over(running_window))
                    .withColumn("running_count", count("*").over(running_window)))
    
    # Pattern 3: Lead/Lag comparisons
    comparison_window = Window.partitionBy("region").orderBy("transaction_date")
    df_comparison = (df.withColumn("prev_sales", lag("sales_amount", 1).over(comparison_window))
                       .withColumn("next_sales", lead("sales_amount", 1).over(comparison_window))
                       .withColumn("sales_change", col("sales_amount") - col("prev_sales")))
    
    # Combine all patterns
    result_df = df_ranking.join(df_running, ["region", "customer_id"]) \
                          .join(df_comparison, ["region", "customer_id"])
    
    result_df.select("region", "sales_amount", "rank", "running_total", "sales_change").show()
    
    print("✅ Common patterns test passed")

def test_optimization_considerations():
    """Test optimization considerations from strategy guide"""
    print("Testing optimization considerations...")
    
    spark = create_test_spark_session()
    df = create_sample_data(spark)
    
    # Test memory-efficient window operations
    
    # 1. Proper partitioning to avoid shuffles
    efficient_window = Window.partitionBy("region").orderBy("transaction_date")
    
    # 2. Reuse window specifications
    reusable_window = Window.partitionBy("region", "product_category").orderBy("sales_amount")
    
    df_optimized = (df.withColumn("category_rank", rank().over(reusable_window))
                      .withColumn("category_pct", percent_rank().over(reusable_window))
                      .withColumn("prev_amount", lag("sales_amount", 1).over(efficient_window)))
    
    df_optimized.select("region", "product_category", "sales_amount", 
                       "category_rank", "category_pct", "prev_amount").show()
    
    print("✅ Optimization considerations test passed")

if __name__ == "__main__":
    print("🧪 Testing WINDOW_FUNCTIONS_STRATEGY_GUIDE.md code examples...")
    
    try:
        spark = test_spark_connect_configuration() 
        test_partitioning_strategies()
        test_frame_specifications()
        test_common_patterns()
        test_optimization_considerations()
        
        print("\n🎉 All WINDOW_FUNCTIONS_STRATEGY_GUIDE.md code examples are working correctly!")
        print("✅ Spark Connect configuration is properly set up")
        print("✅ All window function patterns are runnable")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        raise
    
    finally:
        # Clean up
        spark = SparkSession.getActiveSession()
        if spark:
            spark.stop()