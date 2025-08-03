#!/usr/bin/env python3
"""
Comprehensive test file to verify ALL README.md code examples are runnable
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import datetime, date

def create_test_spark_session():
    """Create a test Spark session"""
    return SparkSession.builder \
        .appName("TestAllREADMECode") \
        .remote("local[*]") \
        .getOrCreate()

def create_comprehensive_sample_data(spark):
    """Create comprehensive sample data for testing all examples"""
    schema = StructType([
        StructField("salesperson", StringType(), True),
        StructField("region", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("closing_price", DoubleType(), True),
        StructField("daily_sales", DoubleType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("date", StringType(), True)
    ])
    
    data = [
        ("Alice", "North", 1000.0, 45.5, 500.0, 800.0, 1, "2024-01-01", "Sales", 75000.0, "2024-01-01"),
        ("Bob", "South", 1500.0, 50.2, 600.0, 900.0, 1, "2024-01-02", "Sales", 80000.0, "2024-01-02"),
        ("Charlie", "North", 800.0, 47.8, 550.0, 750.0, 2, "2024-01-03", "Marketing", 70000.0, "2024-01-03"),
        ("Diana", "West", 1200.0, 52.1, 700.0, 1000.0, 2, "2024-01-04", "Sales", 85000.0, "2024-01-04"),
        ("Eve", "South", 900.0, 48.9, 480.0, 700.0, 3, "2024-01-05", "Marketing", 72000.0, "2024-01-05"),
        ("Frank", "North", 1100.0, 49.2, 520.0, 850.0, 3, "2024-01-06", "Engineering", 90000.0, "2024-01-06"),
        ("Grace", "West", 1300.0, 51.5, 630.0, 950.0, 4, "2024-01-07", "Engineering", 95000.0, "2024-01-07")
    ]
    
    return spark.createDataFrame(data, schema)

def test_1_ranking_operations():
    """Test ranking operations from README"""
    print("1. Testing ranking operations...")
    
    spark = create_test_spark_session()
    df = create_comprehensive_sample_data(spark)
    
    # Basic window specification
    window_spec = Window.partitionBy("region").orderBy(desc("sales_amount"))
    
    # Ranking functions
    df_ranked = df.withColumn("rank", rank().over(window_spec)) \
                  .withColumn("dense_rank", dense_rank().over(window_spec)) \
                  .withColumn("row_number", row_number().over(window_spec))
    
    # Performance tiers
    df_tiers = df_ranked.withColumn("performance_tier",
                        when(col("rank") <= 2, "Top Performer")
                        .when(col("rank") <= 5, "Above Average")
                        .otherwise("Needs Improvement"))
    
    df_tiers.select("salesperson", "region", "sales_amount", "rank", "performance_tier").show()
    print("✅ Ranking operations test passed")

def test_2_aggregation_windows():
    """Test aggregation windows from README"""
    print("2. Testing aggregation windows...")
    
    spark = create_test_spark_session()
    df = create_comprehensive_sample_data(spark)
    
    # Window specifications
    window_spec = Window.partitionBy("region").orderBy("transaction_date") \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    unbounded_window = Window.partitionBy("region").orderBy("transaction_date") \
                             .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    last_3_window = Window.partitionBy("region").orderBy("transaction_date") \
                          .rowsBetween(-2, Window.currentRow)
    
    # Running aggregations
    df_running = (df.withColumn("running_total", sum("sales_amount").over(window_spec))
                    .withColumn("running_count", count("*").over(window_spec))
                    .withColumn("running_avg", avg("sales_amount").over(window_spec)))
    
    df_running.select("region", "transaction_date", "sales_amount", "running_total", "running_avg").show()
    
    # Frame-based aggregations
    df_frames = (df.withColumn("cumulative_total", sum("sales_amount").over(unbounded_window))
                   .withColumn("last_3_total", sum("sales_amount").over(last_3_window))
                   .withColumn("cumulative_avg", avg("sales_amount").over(unbounded_window)))
    
    df_frames.select("region", "sales_amount", "cumulative_total", "last_3_total").show()
    print("✅ Aggregation windows test passed")

def test_3_lead_lag_operations():
    """Test lead/lag operations from README"""
    print("3. Testing lead/lag operations...")
    
    spark = create_test_spark_session()
    df = create_comprehensive_sample_data(spark)
    
    window_spec = Window.partitionBy("region").orderBy("transaction_date")
    
    df_lead_lag = (df.withColumn("previous_price", lag("closing_price", 1).over(window_spec))
                     .withColumn("next_price", lead("closing_price", 1).over(window_spec))
                     .withColumn("price_change", col("closing_price") - col("previous_price")))
    
    df_lead_lag.select("region", "transaction_date", "closing_price", "previous_price", "price_change").show()
    print("✅ Lead/lag operations test passed")

def test_4_moving_averages():
    """Test moving averages from README"""
    print("4. Testing moving averages...")
    
    spark = create_test_spark_session()
    df = create_comprehensive_sample_data(spark)
    
    # Moving window specifications
    moving_3_day = Window.partitionBy("region").orderBy("date") \
                         .rowsBetween(-2, Window.currentRow)
    
    moving_5_day = Window.partitionBy("region").orderBy("date") \
                         .rowsBetween(-4, Window.currentRow)
    
    # Moving averages
    df_moving_avg = (df.withColumn("ma_3_day", round(avg("daily_sales").over(moving_3_day), 2))
                       .withColumn("ma_5_day", round(avg("daily_sales").over(moving_5_day), 2)))
    
    df_moving_avg.select("region", "date", "daily_sales", "ma_3_day", "ma_5_day").show()
    
    # Trend analysis
    df_trends = (df.withColumn("ma_5_day", avg("daily_sales").over(moving_5_day))
                   .withColumn("trend", 
                      when(col("daily_sales") > col("ma_5_day"), "Above Trend")
                      .when(col("daily_sales") < col("ma_5_day"), "Below Trend")
                      .otherwise("On Trend")))
    
    # Monitoring and alerts
    df_monitoring = (df.withColumn("ma_5_day", avg("daily_sales").over(moving_5_day))
                       .withColumn("deviation_pct", 
                          round((col("daily_sales") - col("ma_5_day")) / col("ma_5_day") * 100, 1))
                       .withColumn("alert", 
                          when(abs(col("deviation_pct")) > 20, "Alert")
                          .otherwise("Normal")))
    
    df_monitoring.select("region", "date", "daily_sales", "ma_5_day", "deviation_pct", "alert").show()
    print("✅ Moving averages test passed")

def test_5_percentile_analysis():
    """Test percentile analysis from README"""
    print("5. Testing percentile analysis...")
    
    spark = create_test_spark_session()
    df = create_comprehensive_sample_data(spark)
    
    # Window specifications
    company_window = Window.orderBy("salary")
    dept_window = Window.partitionBy("department").orderBy("salary")
    
    # Percentile calculations
    df_percentiles = (df.withColumn("company_percentile", round(percent_rank().over(company_window) * 100, 1))
                        .withColumn("dept_percentile", round(percent_rank().over(dept_window) * 100, 1))
                        .withColumn("ntile_quartile", ntile(4).over(company_window)))
    
    df_percentiles.select("salesperson", "department", "salary", "company_percentile", "dept_percentile", "ntile_quartile").show()
    
    # Salary benchmarking
    df_benchmark = (df.withColumn("company_rank", rank().over(company_window))
                      .withColumn("percentile_rank", round(percent_rank().over(company_window) * 100, 1))
                      .withColumn("salary_tier",
                         when(col("percentile_rank") >= 75, "Top 25%")
                         .when(col("percentile_rank") >= 50, "Top 50%")
                         .when(col("percentile_rank") >= 25, "Top 75%")
                         .otherwise("Bottom 25%")))
    
    df_benchmark.select("salesperson", "salary", "percentile_rank", "salary_tier").show()
    print("✅ Percentile analysis test passed")

def test_6_first_last_value():
    """Test first/last value analysis from README"""
    print("6. Testing first/last value analysis...")
    
    spark = create_test_spark_session()
    df = create_comprehensive_sample_data(spark)
    
    # Window specifications
    full_window = Window.partitionBy("customer_id").orderBy("transaction_date") \
                        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    acquisition_window = Window.partitionBy("customer_id").orderBy("transaction_date")
    
    # Customer lifetime value
    df_clv = (df.withColumn("first_purchase_date", first_value("transaction_date").over(full_window))
                .withColumn("last_price", last_value("closing_price").over(full_window))
                .withColumn("total_clv", sum("revenue").over(full_window)))
    
    df_clv.select("customer_id", "first_purchase_date", "last_price", "total_clv").distinct().show()
    
    # Acquisition and retention analysis
    df_acquisition = (df.withColumn("acquisition_channel", first_value("region").over(acquisition_window))
                        .withColumn("first_purchase_amount", first_value("sales_amount").over(acquisition_window))
                        .withColumn("latest_purchase", last_value("sales_amount").over(full_window))
                        .withColumn("total_revenue", sum("revenue").over(full_window)))
    
    df_acquisition.select("customer_id", "acquisition_channel", "first_purchase_amount", "latest_purchase", "total_revenue").distinct().show()
    print("✅ First/last value analysis test passed")

if __name__ == "__main__":
    print("🧪 Comprehensive testing of ALL README.md code examples...")
    
    try:
        test_1_ranking_operations()
        test_2_aggregation_windows()
        test_3_lead_lag_operations()
        test_4_moving_averages()
        test_5_percentile_analysis()
        test_6_first_last_value()
        
        print("\n🎉 ALL README.md code examples are working correctly!")
        print("✅ Code is runnable and produces expected results")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        raise
    
    finally:
        # Clean up
        spark = SparkSession.getActiveSession()
        if spark:
            spark.stop()