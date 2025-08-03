#!/usr/bin/env python3
"""
Test file to verify all README.md code examples are runnable
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def create_test_spark_session():
    """Create a test Spark session"""
    return SparkSession.builder \
        .appName("TestREADMECode") \
        .remote("local[*]") \
        .getOrCreate()

def create_sample_data(spark):
    """Create sample data for testing"""
    schema = StructType([
        StructField("salesperson", StringType(), True),
        StructField("region", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("closing_price", DoubleType(), True),
        StructField("daily_sales", DoubleType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("transaction_date", StringType(), True)
    ])
    
    data = [
        ("Alice", "North", 1000.0, 45.5, 500.0, 800.0, 1, "2024-01-01"),
        ("Bob", "South", 1500.0, 50.2, 600.0, 900.0, 1, "2024-01-02"),
        ("Charlie", "North", 800.0, 47.8, 550.0, 750.0, 2, "2024-01-01"),
        ("Diana", "West", 1200.0, 52.1, 700.0, 1000.0, 2, "2024-01-02"),
        ("Eve", "South", 900.0, 48.9, 480.0, 700.0, 3, "2024-01-01")
    ]
    
    return spark.createDataFrame(data, schema)

def test_basic_window_specification():
    """Test basic window specification from README"""
    print("Testing basic window specification...")
    
    spark = create_test_spark_session()
    df = create_sample_data(spark)
    
    # Test the window specification code from README
    window_spec = Window.partitionBy("region").orderBy(desc("sales_amount"))
    
    # Test ranking functions
    df_ranked = df.withColumn("rank", rank().over(window_spec)) \
                  .withColumn("dense_rank", dense_rank().over(window_spec)) \
                  .withColumn("row_number", row_number().over(window_spec))
    
    df_ranked.show()
    print("✅ Basic window specification test passed")

def test_performance_tiers():
    """Test performance tier logic from README"""
    print("Testing performance tiers...")
    
    spark = create_test_spark_session()
    df = create_sample_data(spark)
    
    window_spec = Window.partitionBy("region").orderBy(desc("sales_amount"))
    
    df_tiers = df.withColumn("rank", rank().over(window_spec)) \
                 .withColumn("performance_tier",
                    when(col("rank") <= 2, "Top Performer")
                    .when(col("rank") <= 5, "Above Average")
                    .otherwise("Needs Improvement"))
    
    df_tiers.show()
    print("✅ Performance tiers test passed")

def test_running_aggregations():
    """Test running aggregations from README"""
    print("Testing running aggregations...")
    
    spark = create_test_spark_session()
    df = create_sample_data(spark)
    
    window_spec = Window.partitionBy("region").orderBy("transaction_date") \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    df_running = (df.withColumn("running_total", sum("sales_amount").over(window_spec))
                    .withColumn("running_count", count("*").over(window_spec))
                    .withColumn("running_avg", avg("sales_amount").over(window_spec)))
    
    df_running.show()
    print("✅ Running aggregations test passed")

if __name__ == "__main__":
    print("🧪 Testing README.md code examples...")
    
    try:
        test_basic_window_specification()
        test_performance_tiers()
        test_running_aggregations()
        
        print("\n🎉 All README.md code examples are working correctly!")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        raise
    
    finally:
        # Clean up
        spark = SparkSession.getActiveSession()
        if spark:
            spark.stop()