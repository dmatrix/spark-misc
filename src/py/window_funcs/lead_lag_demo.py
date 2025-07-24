from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, desc, when, round as spark_round
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from datetime import date

def create_stock_price_data(spark):
    """Create simple stock price data for lead/lag demonstrations"""
    data = [
        # AAPL stock prices - 6 days
        ("AAPL", date(2024, 1, 1), 185.00),
        ("AAPL", date(2024, 1, 2), 187.00),
        ("AAPL", date(2024, 1, 3), 184.00),
        ("AAPL", date(2024, 1, 4), 189.00),
        ("AAPL", date(2024, 1, 5), 192.00),
        ("AAPL", date(2024, 1, 8), 195.00),
        
        # GOOGL stock prices - 6 days  
        ("GOOGL", date(2024, 1, 1), 142.00),
        ("GOOGL", date(2024, 1, 2), 145.00),
        ("GOOGL", date(2024, 1, 3), 143.00),
        ("GOOGL", date(2024, 1, 4), 148.00),
        ("GOOGL", date(2024, 1, 5), 149.00),
        ("GOOGL", date(2024, 1, 8), 151.00),
    ]
    
    schema = StructType([
        StructField("stock_symbol", StringType(), False),
        StructField("trade_date", DateType(), False),
        StructField("closing_price", DoubleType(), False)
    ])
    
    return spark.createDataFrame(data, schema)

def demonstrate_basic_lead_lag(df):
    """Show basic lead and lag operations"""
    
    print("🎯 LEAD & LAG FUNCTIONS DEMO")
    print("=" * 60)
    
    # Window specification: partition by stock, order by date
    window_spec = Window.partitionBy("stock_symbol").orderBy("trade_date")
    
    # Access adjacent row values using LAG and LEAD:
    # - previous_price: Get yesterday's closing price (LAG)
    # - next_price: Get tomorrow's closing price (LEAD)
    df_lead_lag = df.withColumn("previous_price", lag("closing_price", 1).over(window_spec)) \
                    .withColumn("next_price", lead("closing_price", 1).over(window_spec))
    
    print("📊 Stock Prices with Previous/Next Day Values")
    print("-" * 60)
    df_lead_lag.select("stock_symbol", "trade_date", "closing_price",
                       "previous_price", "next_price") \
               .orderBy("stock_symbol", "trade_date") \
               .show(truncate=False)
    
    return df_lead_lag

def calculate_price_changes(df):
    """Calculate daily price changes using LAG"""
    
    print("\n🎯 DAILY PRICE CHANGE ANALYSIS")
    print("=" * 60)
    
    window_spec = Window.partitionBy("stock_symbol").orderBy("trade_date")
    
    # Calculate daily price changes using LAG function:
    # - previous_price: Get previous day's closing price
    # - daily_change: Calculate price difference from previous day
    # - trend_direction: Determine if price went up or down
    df_changes = (df.withColumn("previous_price", lag("closing_price", 1).over(window_spec))
                    .withColumn("daily_change", col("closing_price") - col("previous_price"))
                    .withColumn("trend_direction",
                               when(col("daily_change") > 0, "📈 UP").otherwise("📉 DOWN")))
    
    print("📊 Daily Price Changes and Trends")
    print("-" * 60)
    df_changes.select("stock_symbol", "trade_date", "closing_price", 
                      "previous_price", "daily_change", "trend_direction") \
              .orderBy("stock_symbol", "trade_date") \
              .show(truncate=False)
    
    return df_changes

def detect_price_patterns(df):
    """Detect price patterns using lead/lag combinations"""
    
    print("\n🎯 PRICE PATTERN DETECTION")
    print("=" * 60)
    
    window_spec = Window.partitionBy("stock_symbol").orderBy("trade_date")
    
    # Access adjacent row values for pattern detection:
    # - prev_price: Get yesterday's closing price  
    # - next_price: Get tomorrow's closing price
    df_patterns = (df.withColumn("prev_price", lag("closing_price", 1).over(window_spec))
                     .withColumn("next_price", lead("closing_price", 1).over(window_spec)))
    
    # Add pattern detection in a separate step for clarity
    df_patterns = df_patterns.withColumn("pattern_type",                                            # Identify price pattern
                               when(col("closing_price") > col("prev_price"), "📈 UP from yesterday")
                               .otherwise("📉 DOWN from yesterday"))
    
    print("📊 Price Pattern Analysis")
    print("-" * 60)
    df_patterns.select("stock_symbol", "trade_date", "closing_price",
                       "prev_price", "next_price", "pattern_type") \
               .orderBy("stock_symbol", "trade_date") \
               .show(truncate=False)
    
    return df_patterns

def real_world_example(df):
    """Show practical application: Trading Signal Generation"""
    
    print("\n💼 REAL-WORLD APPLICATION: Simple Trading Signals")
    print("=" * 60)
    
    window_spec = Window.partitionBy("stock_symbol").orderBy("trade_date")
    
    # Generate trading signals based on price changes:
    # - price_yesterday: Get previous day's closing price
    # - daily_change: Calculate price difference from previous day
    # - trading_signal: Generate buy/sell/hold signal based on price movement
    df_signals = (df.withColumn("price_yesterday", lag("closing_price", 1).over(window_spec))
                    .withColumn("daily_change", col("closing_price") - col("price_yesterday"))
                    .withColumn("trading_signal",
                               when(col("daily_change") > 5, "🟢 BUY")
                               .when(col("daily_change") < -5, "🔴 SELL")
                               .otherwise("🟡 HOLD")))
    
    print("📊 Trading Signals Based on Price Trends")
    print("-" * 60)
    df_signals.select("stock_symbol", "trade_date", "closing_price",
                      "daily_change", "trading_signal") \
              .orderBy("stock_symbol", "trade_date") \
              .show(truncate=False)
    
    # Show signal summary
    print("\n📈 Trading Signal Summary")
    print("-" * 40)
    from pyspark.sql.functions import count
    
    signal_summary = df_signals.groupBy("stock_symbol", "trading_signal") \
                               .agg(count("*").alias("signal_count")) \
                               .orderBy("stock_symbol", "trading_signal")
    
    signal_summary.show(truncate=False)

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("LeadLagDemo") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Create sample data
    df = create_stock_price_data(spark)
    
    print("📋 Original Stock Price Data")
    print("-" * 40)
    df.orderBy("stock_symbol", "trade_date").show(truncate=False)
    
    # Demonstrate basic lead/lag
    df_lead_lag = demonstrate_basic_lead_lag(df)
    
    # Calculate price changes
    df_changes = calculate_price_changes(df)
    
    # Detect patterns
    df_patterns = detect_price_patterns(df)
    
    # Real-world application
    real_world_example(df)
    
    print("\n✅ DEMO COMPLETED!")
    print("\n💡 Key Takeaways:")
    print("• LAG function accesses previous row values (LAG(col, n) gets n rows back)")
    print("• LEAD function accesses next row values (LEAD(col, n) gets n rows ahead)")
    print("• Perfect for time series analysis and trend detection")
    print("• Essential for calculating changes, patterns, and trading signals")
    print("• Use with proper date/time ordering for meaningful results")
    
    spark.stop() 