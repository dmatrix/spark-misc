"""
PySpark Window Functions Demo: Moving Averages & Trend Analysis

USE CASE:
This demo demonstrates moving averages and rolling window calculations for trend
analysis, noise reduction, and performance monitoring. Essential for smoothing
volatile data and identifying genuine trends versus random fluctuations.

KEY BENEFITS:
• Smooth out daily volatility to reveal underlying trends
• Create early warning systems for performance issues
• Monitor progress against targets with trend analysis
• Build sophisticated performance monitoring dashboards
• Implement data-driven alerting and notification systems

WINDOW FUNCTIONS DEMONSTRATED:
• AVG() with rolling window frames for moving averages
• SUM(), MAX(), MIN() with bounded windows for rolling statistics
• LAG() combined with moving averages for trend direction analysis
• Conditional logic based on trend comparisons

REAL-WORLD APPLICATIONS:
• Sales performance monitoring and trend analysis
• Financial market analysis and trading strategies
• Quality control and process monitoring
• Website traffic and user engagement analysis
• Operational metrics and SLA monitoring

BUSINESS SCENARIOS COVERED:
• 3-day and 5-day moving averages comparison
• Rolling window statistics (sum, max, min)
• Trend direction analysis using moving average comparisons
• Performance monitoring with target achievement tracking
• Alert generation based on moving average thresholds

Usage: python moving_averages_demo.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg as spark_avg, sum as spark_sum, max as spark_max, min as spark_min, count, desc, round as spark_round, when
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from datetime import date

def create_daily_sales_data(spark):
    """Create simple daily sales data for moving window demonstrations"""
    data = [
        # Alice - 8 days of sales data
        ("Alice Johnson", date(2024, 1, 1), 5000.0),
        ("Alice Johnson", date(2024, 1, 2), 4800.0),
        ("Alice Johnson", date(2024, 1, 3), 6000.0),
        ("Alice Johnson", date(2024, 1, 4), 5500.0),
        ("Alice Johnson", date(2024, 1, 5), 7000.0),
        ("Alice Johnson", date(2024, 1, 8), 4900.0),
        ("Alice Johnson", date(2024, 1, 9), 5800.0),
        ("Alice Johnson", date(2024, 1, 10), 6200.0),
        
        # Bob - 8 days of sales data
        ("Bob Smith", date(2024, 1, 1), 4000.0),
        ("Bob Smith", date(2024, 1, 2), 4500.0),
        ("Bob Smith", date(2024, 1, 3), 3800.0),
        ("Bob Smith", date(2024, 1, 4), 5200.0),
        ("Bob Smith", date(2024, 1, 5), 4700.0),
        ("Bob Smith", date(2024, 1, 8), 5100.0),
        ("Bob Smith", date(2024, 1, 9), 4300.0),
        ("Bob Smith", date(2024, 1, 10), 4900.0),
    ]
    
    schema = StructType([
        StructField("salesperson", StringType(), False),
        StructField("sale_date", DateType(), False),
        StructField("daily_sales", DoubleType(), False)
    ])
    
    return spark.createDataFrame(data, schema)

def demonstrate_moving_averages(df):
    """Show different moving average calculations"""
    
    print("🎯 MOVING AVERAGES DEMO")
    print("=" * 60)
    
    # Window specification for different moving averages
    window_spec = Window.partitionBy("salesperson").orderBy("sale_date")
    
    # Different moving windows
    moving_3_day = window_spec.rowsBetween(-2, 0)    # Last 3 days (including current)
    moving_5_day = window_spec.rowsBetween(-4, 0)    # Last 5 days (including current)
    moving_7_day = window_spec.rowsBetween(-6, 0)    # Last 7 days (including current)
    
    # Calculate different moving averages:
    # - ma_3_day: 3-day moving average of sales
    # - ma_5_day: 5-day moving average of sales
    df_moving_avg = (df.withColumn("ma_3_day", spark_round(spark_avg("daily_sales").over(moving_3_day), 2))
                       .withColumn("ma_5_day", spark_round(spark_avg("daily_sales").over(moving_5_day), 2)))
    
    print("📊 Moving Averages Comparison (3 and 5 days)")
    print("-" * 60)
    df_moving_avg.select("salesperson", "sale_date", "daily_sales",
                         "ma_3_day", "ma_5_day") \
                 .orderBy("salesperson", "sale_date") \
                 .show(truncate=False)
    
    return df_moving_avg

def demonstrate_rolling_calculations(df):
    """Show different rolling window calculations"""
    
    print("\n🎯 ROLLING CALCULATIONS")
    print("=" * 60)
    
    window_spec = Window.partitionBy("salesperson").orderBy("sale_date")
    
    # Different rolling windows
    rolling_3_day = window_spec.rowsBetween(-2, 0)
    rolling_7_day = window_spec.rowsBetween(-6, 0)
    
    # Calculate rolling window statistics:
    # - rolling_3_day_total: Sum of last 3 days including current
    # - rolling_3_day_max: Maximum of last 3 days
    # - rolling_3_day_min: Minimum of last 3 days
    df_rolling = (df.withColumn("rolling_3_day_total", spark_sum("daily_sales").over(rolling_3_day))
                    .withColumn("rolling_3_day_max", spark_max("daily_sales").over(rolling_3_day))
                    .withColumn("rolling_3_day_min", spark_min("daily_sales").over(rolling_3_day)))
    
    print("📊 Rolling Totals and Statistics")
    print("-" * 60)
    df_rolling.select("salesperson", "sale_date", "daily_sales",
                      "rolling_3_day_total", "rolling_3_day_max", "rolling_3_day_min") \
              .orderBy("salesperson", "sale_date") \
              .show(truncate=False)
    
    return df_rolling

def analyze_sales_trends(df):
    """Analyze sales trends using moving averages"""
    
    print("\n🎯 SALES TREND ANALYSIS")
    print("=" * 60)
    
    window_spec = Window.partitionBy("salesperson").orderBy("sale_date")
    moving_5_day = window_spec.rowsBetween(-4, 0)
    
    from pyspark.sql.functions import lag
    
    # Analyze sales trends using moving averages:
    # - ma_5_day: Current 5-day moving average
    # - prev_ma_5_day: Previous 5-day moving average
    # - trend_direction: Compare current vs previous to show trend
    df_trends = (df.withColumn("ma_5_day", spark_avg("daily_sales").over(moving_5_day))
                   .withColumn("prev_ma_5_day", lag("ma_5_day", 1).over(window_spec))
                   .withColumn("trend_direction",
                              when(col("ma_5_day") > col("prev_ma_5_day"), "📈 IMPROVING")
                              .otherwise("📉 DECLINING")))
    
    print("📊 Trend Analysis Using 5-Day Moving Average")
    print("-" * 60)
    df_trends.select("salesperson", "sale_date", "daily_sales",
                     "ma_5_day", "trend_direction") \
             .orderBy("salesperson", "sale_date") \
             .show(truncate=False)
    
    return df_trends

def real_world_example(df):
    """Show practical application: Performance Monitoring"""
    
    print("\n💼 REAL-WORLD APPLICATION: Performance Monitoring & Alerts")
    print("=" * 60)
    
    window_spec = Window.partitionBy("salesperson").orderBy("sale_date")
    moving_5_day = window_spec.rowsBetween(-4, 0)
    
    # Define performance targets
    daily_target = 5000.0
    
    # Monitor performance against targets:
    # - ma_5_day: 5-day moving average for trend analysis
    # - target_achievement: Check if daily target was met
    # - performance_alert: Alert based on moving average vs target
    df_monitoring = (df.withColumn("ma_5_day", spark_avg("daily_sales").over(moving_5_day))
                       .withColumn("target_achievement",
                                  when(col("daily_sales") >= daily_target, "✅ TARGET MET")
                                  .otherwise("❌ BELOW TARGET"))
                       .withColumn("performance_alert",
                                  when(col("ma_5_day") < daily_target, "⚠️ MONITOR CLOSELY")
                                  .otherwise("✅ PERFORMING WELL")))
    
    print("📊 Daily Performance Monitoring")
    print("-" * 60)
    df_monitoring.select("salesperson", "sale_date", "daily_sales", "ma_5_day",
                         "target_achievement", "performance_alert") \
                 .orderBy("salesperson", "sale_date") \
                 .show(truncate=False)
    
    # Show performance summary
    print("\n📈 Performance Summary")
    print("-" * 40)
    
    performance_summary = df_monitoring.groupBy("salesperson") \
                                      .agg(spark_avg("daily_sales").alias("avg_daily_sales"),
                                           count(when(col("daily_sales") >= daily_target, 1)).alias("days_above_target"),
                                           count("*").alias("total_days")) \
                                      .orderBy(desc("avg_daily_sales"))
    
    performance_summary.show(truncate=False)

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("MovingAveragesDemo") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Create sample data
    df = create_daily_sales_data(spark)
    
    print("📋 Original Daily Sales Data")
    print("-" * 40)
    df.orderBy("salesperson", "sale_date").show(truncate=False)
    
    # Demonstrate moving averages
    df_moving_avg = demonstrate_moving_averages(df)
    
    # Show rolling calculations
    df_rolling = demonstrate_rolling_calculations(df)
    
    # Analyze trends
    df_trends = analyze_sales_trends(df)
    
    # Real-world application
    real_world_example(df)
    
    print("\n✅ DEMO COMPLETED!")
    print("\n💡 Key Takeaways:")
    print("• Moving windows use rowsBetween() to define frame boundaries")
    print("• rowsBetween(-n, 0) creates n+1 day moving calculations")
    print("• Moving averages smooth out daily fluctuations")
    print("• Perfect for trend analysis and performance monitoring")
    print("• Essential for identifying patterns and setting up alerts")
    print("• Combine with LAG/LEAD for comprehensive analysis")
    
    spark.stop() 