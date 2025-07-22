from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, row_number, desc, when, lag, unix_timestamp, abs as spark_abs
from pyspark.sql.window import Window
from datetime import datetime

def create_sample_data(spark):
    """Create sample data with various duplicate patterns"""
    data = [
        # Exact duplicates
        ("TXN001", "USER123", 99.99, datetime(2024, 1, 15, 10, 30, 0), "RETAIL", "GOLD"),
        ("TXN001", "USER123", 99.99, datetime(2024, 1, 15, 10, 30, 0), "RETAIL", "GOLD"),
        
        # Same transaction_id, different details (system error)
        ("TXN002", "USER456", 150.00, datetime(2024, 1, 15, 11, 0, 0), "GROCERY", "SILVER"),
        ("TXN002", "USER789", 200.00, datetime(2024, 1, 15, 11, 5, 0), "RETAIL", "BRONZE"),
        
        # Same user + amount within 30 seconds (double-click)
        ("TXN003", "USER123", 49.99, datetime(2024, 1, 15, 12, 0, 0), "ONLINE", "GOLD"),
        ("TXN004", "USER123", 49.99, datetime(2024, 1, 15, 12, 0, 30), "ONLINE", "GOLD"),
        
        # Unique records
        ("TXN005", "USER456", 75.50, datetime(2024, 1, 15, 13, 0, 0), "RESTAURANT", "SILVER"),
        ("TXN006", "USER789", 120.00, datetime(2024, 1, 15, 14, 0, 0), "GAS", "BRONZE"),
    ]
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("transaction_amount", DoubleType(), False),
        StructField("transaction_date", TimestampType(), False),
        StructField("merchant_category", StringType(), True),
        StructField("customer_tier", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

def exact_deduplication(df):
    """Remove exact duplicate rows - fastest method"""
    return df.distinct()

def business_logic_deduplication(df):
    """Remove duplicates based on business rules"""
    
    # Rule 1: Same transaction_id = duplicate (keep first occurrence)
    window_txn = Window.partitionBy("transaction_id").orderBy("transaction_date")
    df = df.withColumn("txn_rank", row_number().over(window_txn)) \
           .filter(col("txn_rank") == 1) \
           .drop("txn_rank")
    
    # Rule 2: Same user + amount within 60 seconds = double-click (keep first)
    window_user = Window.partitionBy("user_id", "transaction_amount").orderBy("transaction_date")
    
    df = df.withColumn("prev_time", lag("transaction_date").over(window_user)) \
           .withColumn("time_diff", 
                      when(col("prev_time").isNull(), 999999)
                      .otherwise(spark_abs(unix_timestamp("transaction_date") - 
                                         unix_timestamp("prev_time")))) \
           .filter(col("time_diff") > 60) \
           .drop("prev_time", "time_diff")
    
    return df

def priority_deduplication(df):
    """Keep the best record when duplicates exist"""
    
    # Priority: GOLD > SILVER > BRONZE
    df_with_priority = df.withColumn("priority",
                                   when(col("customer_tier") == "GOLD", 3)
                                   .when(col("customer_tier") == "SILVER", 2)
                                   .when(col("customer_tier") == "BRONZE", 1)
                                   .otherwise(0))
    
    window_priority = Window.partitionBy("transaction_id") \
                           .orderBy(desc("priority"), desc("transaction_date"))
    
    return df_with_priority.withColumn("rank", row_number().over(window_priority)) \
                          .filter(col("rank") == 1) \
                          .drop("priority", "rank")

if __name__ == "__main__":
    # Initialize Spark with performance optimizations
    spark = SparkSession.builder \
        .appName("DeduplicationExample") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")  # Reduce verbose output
    
    # Create sample data
    df = create_sample_data(spark)
    print(f"Original records: {df.count()}")
    
    # Method 1: Exact deduplication (fastest for exact matches)
    exact_result = exact_deduplication(df)
    print(f"After exact deduplication: {exact_result.count()}")
    
    # Method 2: Business logic deduplication
    business_result = business_logic_deduplication(df)
    print(f"After business logic deduplication: {business_result.count()}")
    
    # Method 3: Priority-based deduplication
    priority_result = priority_deduplication(df)
    print(f"After priority deduplication: {priority_result.count()}")
    
    # Show final results
    print("\nFinal deduplicated data:")
    business_result.show(truncate=False)
    
    spark.stop() 