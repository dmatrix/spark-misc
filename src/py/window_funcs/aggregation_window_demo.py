from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg as spark_avg, max as spark_max, min as spark_min, desc
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from datetime import date

def create_daily_sales_data(spark):
    """Create simple daily sales data for aggregation demonstrations - 6 months per rep"""
    data = [
        # Sales Rep Alice - 6 months (Jan-Jun 2024)
        ("Alice Johnson", "North", 15000.0, date(2024, 1, 15)),
        ("Alice Johnson", "North", 22000.0, date(2024, 2, 20)),
        ("Alice Johnson", "North", 18000.0, date(2024, 3, 10)),
        ("Alice Johnson", "North", 25000.0, date(2024, 4, 12)),
        ("Alice Johnson", "North", 21000.0, date(2024, 5, 18)),
        ("Alice Johnson", "North", 24000.0, date(2024, 6, 22)),
        
        # Sales Rep Bob - 6 months (Jan-Jun 2024)
        ("Bob Smith", "North", 12000.0, date(2024, 1, 8)),
        ("Bob Smith", "North", 16000.0, date(2024, 2, 14)),
        ("Bob Smith", "North", 20000.0, date(2024, 3, 25)),
        ("Bob Smith", "North", 18000.0, date(2024, 4, 9)),
        ("Bob Smith", "North", 19000.0, date(2024, 5, 14)),
        ("Bob Smith", "North", 23000.0, date(2024, 6, 28)),
        
        # Sales Rep Carol - South region, 6 months
        ("Carol Davis", "South", 28000.0, date(2024, 1, 12)),
        ("Carol Davis", "South", 32000.0, date(2024, 2, 18)),
        ("Carol Davis", "South", 26000.0, date(2024, 3, 15)),
        ("Carol Davis", "South", 30000.0, date(2024, 4, 20)),
        ("Carol Davis", "South", 34000.0, date(2024, 5, 25)),
        ("Carol Davis", "South", 31000.0, date(2024, 6, 30)),
    ]
    
    schema = StructType([
        StructField("salesperson", StringType(), False),
        StructField("region", StringType(), False),
        StructField("sales_amount", DoubleType(), False),
        StructField("sale_date", DateType(), False)
    ])
    
    return spark.createDataFrame(data, schema)

def demonstrate_running_totals(df):
    """Show running totals and cumulative calculations"""
    
    print("🎯 RUNNING TOTALS & CUMULATIVE CALCULATIONS")
    print("=" * 60)
    
    # Window specification: partition by salesperson, order by date
    window_spec = Window.partitionBy("salesperson").orderBy("sale_date")
    
    # Calculate cumulative aggregations for each salesperson:
    # - running_total: Sum of all sales from start to current row
    # - running_count: Count of transactions processed so far  
    # - running_avg: Running average of sales amounts
    df_running = df.withColumn("running_total", spark_sum("sales_amount").over(window_spec)) \
                   .withColumn("running_count", count("*").over(window_spec)) \
                   .withColumn("running_avg", spark_avg("sales_amount").over(window_spec))
    
    print("📊 Running Aggregations by Salesperson")
    print("-" * 60)
    df_running.select("salesperson", "sale_date", "sales_amount",
                      "running_total", "running_count", "running_avg") \
              .orderBy("salesperson", "sale_date") \
              .show(truncate=False)
    
    return df_running

def demonstrate_window_frames(df):
    """Show different window frame types"""
    
    print("\n🎯 WINDOW FRAMES: UNBOUNDED vs BOUNDED")
    print("=" * 60)
    
    window_spec = Window.partitionBy("salesperson").orderBy("sale_date")
    
    # Different window frames
    unbounded_window = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    last_3_window = window_spec.rowsBetween(-2, Window.currentRow)  # Last 3 sales including current
    last_5_window = window_spec.rowsBetween(-4, Window.currentRow)  # Last 5 sales including current
    
    # Compare different window frame types:
    # - unbounded_window: From start of partition to current row
    # - last_3_window: Only last 3 rows including current row
    # - last_5_window: Only last 5 rows including current row
    df_frames = (df.withColumn("cumulative_total", spark_sum("sales_amount").over(unbounded_window))
                   .withColumn("last_3_total", spark_sum("sales_amount").over(last_3_window))
                   .withColumn("last_5_total", spark_sum("sales_amount").over(last_5_window)))
    
    print("📊 Comparison: Cumulative vs Last 3 vs Last 5 Sales")
    print("-" * 60)
    df_frames.select("salesperson", "sale_date", "sales_amount",
                     "cumulative_total", "last_3_total", "last_5_total") \
             .orderBy("salesperson", "sale_date") \
             .show(truncate=False)
    
    return df_frames

def real_world_example(df):
    """Show practical business application: Sales Performance Tracking"""
    
    print("\n💼 REAL-WORLD APPLICATION: Sales Performance Tracking")
    print("=" * 60)
    
    # Window for individual performance
    individual_window = Window.partitionBy("salesperson").orderBy("sale_date")
    
    # Window for regional totals
    regional_window = Window.partitionBy("region").orderBy("sale_date")
    
    # Track performance metrics using different window partitions:
    # - individual_ytd: Each person's year-to-date total
    # - regional_ytd: Region's year-to-date total
    # - contribution_pct: Individual's percentage of regional total
    df_performance = df.withColumn("individual_ytd", spark_sum("sales_amount").over(individual_window)) \
                       .withColumn("regional_ytd", spark_sum("sales_amount").over(regional_window)) \
                       .withColumn("contribution_pct", 
                                  col("individual_ytd") / col("regional_ytd") * 100)
    
    print("🎯 Year-to-Date Performance Tracking")
    print("-" * 50)
    df_performance.select("region", "salesperson", "sale_date", "sales_amount",
                          "individual_ytd", "contribution_pct") \
                  .orderBy("region", "salesperson", "sale_date") \
                  .show(truncate=False)
    
    # Show final YTD summary
    print("\n📊 Final YTD Summary by Salesperson")
    print("-" * 50)
    final_summary = df_performance.select("region", "salesperson", "individual_ytd", "contribution_pct") \
                                  .groupBy("region", "salesperson") \
                                  .agg(spark_max("individual_ytd").alias("total_ytd_sales"),
                                       spark_max("contribution_pct").alias("region_contribution_pct")) \
                                  .orderBy("region", desc("total_ytd_sales"))
    
    final_summary.show(truncate=False)

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("AggregationWindowDemo") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Create sample data
    df = create_daily_sales_data(spark)
    
    print("📋 Original Daily Sales Data")
    print("-" * 40)
    df.orderBy("salesperson", "sale_date").show(truncate=False)
    
    # Demonstrate running totals
    df_running = demonstrate_running_totals(df)
    
    # Show window frames
    df_frames = demonstrate_window_frames(df)
    
    # Real-world application
    real_world_example(df)
    
    print("\n✅ DEMO COMPLETED!")
    print("\n💡 Key Takeaways:")
    print("• Window aggregations calculate running totals, averages, and other metrics")
    print("• Unbounded windows include all rows from start to current row")
    print("• Bounded windows limit the frame size (e.g., last 3, last 5 rows)")
    print("• With 6 months of data, you can see meaningful trends and patterns")
    print("• Perfect for YTD calculations, performance tracking, and contribution analysis")
    print("• Essential for financial reporting and business analytics")
    
    spark.stop() 