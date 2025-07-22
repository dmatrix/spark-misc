from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import col, row_number, desc, count, when, isnan, isnull
from pyspark.sql.window import Window
from datetime import datetime
import time

def create_sample_data(spark):
    """Create sample transaction data with intentional duplicates"""
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("transaction_amount", DoubleType(), False),
        StructField("transaction_date", TimestampType(), False),
        StructField("merchant_category", StringType(), True),
        StructField("customer_tier", StringType(), True)
    ])
    
    # Sample data with various types of duplicates
    data = [
        # Exact duplicates (rows 1-2)
        ("TXN001", "USER123", 99.99, datetime(2024, 1, 15, 10, 30, 0), "RETAIL", "GOLD"),
        ("TXN001", "USER123", 99.99, datetime(2024, 1, 15, 10, 30, 0), "RETAIL", "GOLD"),
        
        # Same transaction_id, different details (system error)
        ("TXN002", "USER456", 150.00, datetime(2024, 1, 15, 11, 0, 0), "GROCERY", "SILVER"),
        ("TXN002", "USER789", 200.00, datetime(2024, 1, 15, 11, 5, 0), "RETAIL", "BRONZE"),
        
        # Same user + amount + close time (potential double-click)
        ("TXN003", "USER123", 49.99, datetime(2024, 1, 15, 12, 0, 0), "ONLINE", "GOLD"),
        ("TXN004", "USER123", 49.99, datetime(2024, 1, 15, 12, 0, 30), "ONLINE", "GOLD"),
        
        # Unique records
        ("TXN005", "USER456", 75.50, datetime(2024, 1, 15, 13, 0, 0), "RESTAURANT", "SILVER"),
        ("TXN006", "USER789", 120.00, datetime(2024, 1, 15, 14, 0, 0), "GAS", "BRONZE"),
        
        # Near duplicates (same user, similar amount, same day)
        ("TXN007", "USER123", 25.00, datetime(2024, 1, 15, 15, 0, 0), "COFFEE", "GOLD"),
        ("TXN008", "USER123", 25.01, datetime(2024, 1, 15, 15, 30, 0), "COFFEE", "GOLD"),
    ]
    
    return spark.createDataFrame(data, schema)

def simple_exact_deduplication(df):
    """
    Simple deduplication - removes exact duplicate rows
    Performance optimized with distinct()
    """
    print("🔍 Simple Exact Deduplication")
    print("-" * 50)
    
    start_time = time.time()
    
    # Method 1: Using distinct() - most performant for exact matches
    deduplicated_df = df.distinct()
    
    end_time = time.time()
    
    print(f"Original records: {df.count()}")
    print(f"After exact deduplication: {deduplicated_df.count()}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    print(f"Performance tip: distinct() is optimized for exact row matches\n")
    
    return deduplicated_df

def business_logic_deduplication(df):
    """
    Business logic deduplication - removes duplicates based on business rules
    Performance optimized with partitioning and window functions
    """
    print("🎯 Business Logic Deduplication")
    print("-" * 50)
    
    start_time = time.time()
    
    # Business Rule 1: Same transaction_id = duplicate (keep first occurrence)
    print("Rule 1: Deduplicating by transaction_id...")
    window_txn = Window.partitionBy("transaction_id").orderBy("transaction_date")
    
    df_txn_dedup = df.withColumn("txn_rank", row_number().over(window_txn)) \
                     .filter(col("txn_rank") == 1) \
                     .drop("txn_rank")
    
    print(f"After transaction_id deduplication: {df_txn_dedup.count()} records")
    
    # Business Rule 2: Same user + same amount within 1 minute = duplicate click
    print("Rule 2: Detecting potential double-clicks (same user + amount + time)...")
    
    # Convert timestamp to seconds for time window comparison
    from pyspark.sql.functions import unix_timestamp, abs as spark_abs, lag
    
    window_user = Window.partitionBy("user_id", "transaction_amount") \
                        .orderBy("transaction_date")
    
    df_with_time_diff = df_txn_dedup.withColumn(
        "prev_transaction_time", 
        lag("transaction_date").over(window_user)
    ).withColumn(
        "time_diff_seconds",
        when(col("prev_transaction_time").isNull(), 999999)
        .otherwise(
            spark_abs(unix_timestamp("transaction_date") - unix_timestamp("prev_transaction_time"))
        )
    )
    
    # Remove records within 60 seconds of previous same-user same-amount transaction
    df_final = df_with_time_diff.filter(col("time_diff_seconds") > 60) \
                               .drop("prev_transaction_time", "time_diff_seconds")
    
    end_time = time.time()
    
    print(f"After business logic deduplication: {df_final.count()} records")
    print(f"Total execution time: {end_time - start_time:.4f} seconds")
    print(f"Performance tip: Window functions with partitioning optimize deduplication")
    
    return df_final

def advanced_deduplication_with_priority(df):
    """
    Advanced deduplication with priority rules
    When duplicates exist, keep the "best" record based on business priority
    """
    print("🏆 Advanced Priority-Based Deduplication")  
    print("-" * 50)
    
    start_time = time.time()
    
    # Priority rule: For same transaction_id, keep record with highest customer tier
    tier_priority = {"GOLD": 3, "SILVER": 2, "BRONZE": 1, None: 0}
    
    # Create priority column
    df_with_priority = df.withColumn(
        "tier_priority",
        when(col("customer_tier") == "GOLD", 3)
        .when(col("customer_tier") == "SILVER", 2)  
        .when(col("customer_tier") == "BRONZE", 1)
        .otherwise(0)
    )
    
    # Window function to rank by priority within each transaction group
    window_priority = Window.partitionBy("transaction_id") \
                           .orderBy(desc("tier_priority"), desc("transaction_date"))
    
    df_priority_dedup = df_with_priority.withColumn("priority_rank", row_number().over(window_priority)) \
                                       .filter(col("priority_rank") == 1) \
                                       .drop("tier_priority", "priority_rank")
    
    end_time = time.time()
    
    print(f"After priority-based deduplication: {df_priority_dedup.count()} records")
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    print(f"Performance tip: Combine business rules with window ranking for optimal results\n")
    
    return df_priority_dedup

def analyze_duplicates(df):
    """Analyze duplicate patterns for insights"""
    print("📊 Duplicate Analysis")
    print("-" * 50)
    
    # Find exact duplicate count
    total_records = df.count()
    unique_records = df.distinct().count()
    exact_duplicates = total_records - unique_records
    
    print(f"Total records: {total_records}")
    print(f"Unique records: {unique_records}")  
    print(f"Exact duplicates: {exact_duplicates}")
    
    # Find transaction_id duplicates
    txn_id_duplicates = df.groupBy("transaction_id").count().filter(col("count") > 1)
    print(f"Transaction IDs with duplicates: {txn_id_duplicates.count()}")
    
    if txn_id_duplicates.count() > 0:
        print("Duplicate transaction IDs:")
        txn_id_duplicates.show(10, truncate=False)
    
    print()

if __name__ == "__main__":
    # Initialize Spark with performance optimizations
    spark = SparkSession.builder \
        .appName("DeduplicationExample") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("🚀 PySpark 4.0 Deduplication Examples")
    print("="*60)
    
    # Create sample data
    df = create_sample_data(spark)
    
    print("📋 Original Data Sample:")
    df.show(20, truncate=False)
    
    # Analyze duplicate patterns
    analyze_duplicates(df)
    
    # Example 1: Simple exact deduplication
    simple_dedup_df = simple_exact_deduplication(df)
    simple_dedup_df.show(truncate=False)
    
    # Example 2: Business logic deduplication  
    business_dedup_df = business_logic_deduplication(df)
    business_dedup_df.show(truncate=False)
    
    # Example 3: Advanced priority-based deduplication
    priority_dedup_df = advanced_deduplication_with_priority(df)
    priority_dedup_df.show(truncate=False)
    
    print("✅ Deduplication examples completed!")
    print("\n💡 Key Performance Tips:")
    print("1. Use distinct() for exact row matches")
    print("2. Leverage window functions for business rule deduplication")  
    print("3. Enable adaptive query execution for better performance")
    print("4. Cache intermediate results when applying multiple deduplication rules")
    print("5. Consider partitioning strategy based on your duplicate detection keys")
    
    spark.stop() 