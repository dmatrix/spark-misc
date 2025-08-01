"""
PySpark Window Functions Demo: First/Last Value Analysis for Customer Journey

USE CASE:
This demo showcases FIRST_VALUE and LAST_VALUE functions for customer journey
analysis, marketing attribution, and lifecycle tracking. Perfect for understanding
customer acquisition channels, conversion paths, and lifetime value patterns.

KEY BENEFITS:
• Track complete customer journeys from acquisition to conversion
• Implement marketing attribution models (first-touch, last-touch)
• Calculate customer lifetime value (CLV) with acquisition context
• Identify most effective marketing channels and touchpoints
• Build comprehensive customer analytics and segmentation

WINDOW FUNCTIONS DEMONSTRATED:
• FIRST_VALUE() - Capture initial touchpoints and acquisition channels
• LAST_VALUE() - Identify final interactions and conversion events
• Window frames with unboundedFollowing for true last values
• Conditional aggregations for revenue and conversion tracking

REAL-WORLD APPLICATIONS:
• Marketing attribution and channel effectiveness analysis
• Customer acquisition cost (CAC) and lifetime value (LTV) analysis
• E-commerce conversion funnel optimization
• Customer journey mapping and experience optimization
• Retention and churn analysis with acquisition context

BUSINESS SCENARIOS COVERED:
• Customer touchpoint and channel identification
• First purchase detection and acquisition analysis
• Customer lifetime value calculation and segmentation
• Marketing attribution modeling (first-touch vs. last-touch)
• Customer journey optimization insights

Usage: python first_last_value_demo.py

NOTE: This demo uses Spark Connect.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, last, desc, when, round as spark_round, max as spark_max, min as spark_min
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from datetime import date

def create_customer_journey_data(spark):
    """Create simple customer journey data for first/last value demonstrations"""
    data = [
        # Customer A journey - Website to purchase
        ("CUST_001", "Landing Page", "Website", date(2024, 1, 1), 0.0),
        ("CUST_001", "Add to Cart", "Website", date(2024, 1, 2), 0.0),
        ("CUST_001", "Purchase", "Website", date(2024, 1, 3), 299.99),
        ("CUST_001", "Repeat Purchase", "Mobile App", date(2024, 1, 15), 199.99),
        
        # Customer B journey - Email to purchase 
        ("CUST_002", "Email Click", "Email", date(2024, 1, 5), 0.0),
        ("CUST_002", "Product View", "Website", date(2024, 1, 6), 0.0),
        ("CUST_002", "Purchase", "Website", date(2024, 1, 7), 149.99),
        
        # Customer C journey - Social media to purchase
        ("CUST_003", "Social Media Ad", "Instagram", date(2024, 1, 8), 0.0),
        ("CUST_003", "Product View", "Website", date(2024, 1, 8), 0.0),
        ("CUST_003", "Purchase", "Website", date(2024, 1, 9), 89.99),
        
        # Customer D journey - Search to purchase
        ("CUST_004", "Google Search", "Google", date(2024, 1, 10), 0.0),
        ("CUST_004", "Product View", "Website", date(2024, 1, 11), 0.0),
        ("CUST_004", "Purchase", "Website", date(2024, 1, 12), 449.99),
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("touchpoint", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("event_date", DateType(), False),
        StructField("revenue", DoubleType(), False)
    ])
    
    return spark.createDataFrame(data, schema)

def demonstrate_first_last_values(df):
    """Show first_value and last_value functions"""
    
    print("🎯 FIRST_VALUE & LAST_VALUE FUNCTIONS DEMO")
    print("=" * 60)
    
    # Window specification: partition by customer, order by date
    window_spec = Window.partitionBy("customer_id").orderBy("event_date")
    full_window = window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    # Simple first/last analysis - max 3 columns
    # Analyze customer journey using first and last values:
    # - first_touchpoint: Customer's initial interaction
    # - last_touchpoint: Customer's final interaction
    # - first_channel: How customer was acquired
    df_first_last = (df.withColumn("first_touchpoint", first("touchpoint").over(window_spec))
                       .withColumn("last_touchpoint", last("touchpoint").over(full_window))
                       .withColumn("first_channel", first("channel").over(window_spec)))
    
    print("📊 Customer Journey: First & Last Touchpoints")
    print("-" * 60)
    df_first_last.select("customer_id", "event_date", "touchpoint", 
                         "first_touchpoint", "last_touchpoint", "first_channel") \
                 .orderBy("customer_id", "event_date") \
                 .show(truncate=False)
    
    return df_first_last

def analyze_customer_acquisition(df):
    """Analyze customer acquisition using first_value"""
    
    print("\n🎯 CUSTOMER ACQUISITION ANALYSIS")
    print("=" * 60)
    
    window_spec = Window.partitionBy("customer_id").orderBy("event_date")
    
    # Simple acquisition analysis - max 3 columns
    # Analyze customer acquisition patterns:
    # - acquisition_channel: How customer was first acquired
    # - first_purchase_amount: Customer's first purchase value
    # - customer_type: Categorize customer by acquisition channel
    df_acquisition = (df.withColumn("acquisition_channel", first("channel").over(window_spec))
                        .withColumn("first_purchase_amount",
                                   first(when(col("revenue") > 0, col("revenue"))).over(window_spec))
                        .withColumn("customer_type",
                                   when(col("acquisition_channel") == "Email", "📧 EMAIL")
                                   .otherwise("🌐 OTHER")))
    
    print("📊 Customer Acquisition Analysis")
    print("-" * 60)
    df_acquisition.select("customer_id", "acquisition_channel", 
                          "first_purchase_amount", "customer_type") \
                  .dropDuplicates(["customer_id"]) \
                  .orderBy("customer_id") \
                  .show(truncate=False)
    
    return df_acquisition

def track_customer_lifetime_value(df):
    """Track customer lifetime value using window functions"""
    
    print("\n🎯 CUSTOMER LIFETIME VALUE TRACKING")
    print("=" * 60)
    
    window_spec = Window.partitionBy("customer_id").orderBy("event_date")
    full_window = window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    from pyspark.sql.functions import sum as spark_sum
    
    # Simple CLV analysis - max 3 columns
    # Track customer lifetime value:
    # - first_purchase_date: Date of customer's first purchase
    # - total_clv: Total revenue from customer
    # - customer_segment: Classify customer by value
    df_clv = (df.withColumn("first_purchase_date",
                           first(when(col("revenue") > 0, col("event_date"))).over(window_spec))
                .withColumn("total_clv", spark_sum("revenue").over(full_window))
                .withColumn("customer_segment",
                           when(col("total_clv") >= 300, "💎 HIGH VALUE")
                           .otherwise("📊 STANDARD")))
    
    print("📊 Customer Lifetime Value Analysis")
    print("-" * 60)
    df_clv.select("customer_id", "first_purchase_date", "total_clv", "customer_segment") \
           .dropDuplicates(["customer_id"]) \
           .orderBy(desc("total_clv")) \
           .show(truncate=False)
    
    return df_clv

def real_world_example(df):
    """Show practical application: Simple Attribution Analysis"""
    
    print("\n💼 REAL-WORLD APPLICATION: Simple Attribution Analysis")
    print("=" * 60)
    
    window_spec = Window.partitionBy("customer_id").orderBy("event_date")
    full_window = window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    from pyspark.sql.functions import sum as spark_sum
    
    # Simple attribution - max 3 columns
    # Attribution analysis using first and last touchpoints:
    # - first_touch_channel: First marketing touchpoint
    # - last_touch_channel: Last marketing touchpoint
    # - total_revenue: Total revenue from customer
    df_attribution = (df.withColumn("first_touch_channel", first("channel").over(window_spec))
                        .withColumn("last_touch_channel", last("channel").over(full_window))
                        .withColumn("total_revenue", spark_sum("revenue").over(full_window)))
    
    print("📊 Attribution Analysis")
    print("-" * 60)
    df_attribution.select("customer_id", "first_touch_channel", 
                          "last_touch_channel", "total_revenue") \
                  .dropDuplicates(["customer_id"]) \
                  .orderBy(desc("total_revenue")) \
                  .show(truncate=False)

if __name__ == "__main__":
    # Initialize Spark Connect
    spark = SparkSession.builder \
        .appName("FirstLastValueDemo") \
        .config("spark.api.mode", "connect") \
        .remote("local[*]") \
        .getOrCreate()
    
    # Note: spark.sparkContext is not available in Spark Connect
    # Log level configuration is handled server-side
    
    # Create sample data
    df = create_customer_journey_data(spark)
    
    print("📋 Original Customer Journey Data")
    print("-" * 40)
    df.orderBy("customer_id", "event_date").show(truncate=False)
    
    # Demonstrate first/last value functions
    df_first_last = demonstrate_first_last_values(df)
    
    # Analyze customer acquisition
    df_acquisition = analyze_customer_acquisition(df)
    
    # Track customer lifetime value
    df_clv = track_customer_lifetime_value(df)
    
    # Real-world application
    real_world_example(df)
    
    print("\n✅ DEMO COMPLETED!")
    print("\n💡 Key Takeaways:")
    print("• FIRST_VALUE() gets the first value in the window frame")
    print("• LAST_VALUE() gets the last value in the window frame")
    print("• Use unboundedFollowing for true 'last' values across entire partition")
    print("• Perfect for customer journey analysis and attribution modeling")
    print("• Essential for tracking acquisition channels and conversion paths")
    print("• Combine with conditional logic for advanced insights")
    
    spark.stop() 