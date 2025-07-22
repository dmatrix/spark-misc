from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import (col, row_number, rank, dense_rank, max as spark_max, 
                                 sum as spark_sum, count, desc, asc, first, last, 
                                 when, collect_list, struct)
from pyspark.sql.window import Window
from datetime import datetime

def create_advanced_sample_data(spark):
    """Create sample data with complex duplicate scenarios"""
    data = [
        # Same customer, different transaction amounts (keep highest)
        ("CUST001", "John Smith", 50.00, datetime(2024, 1, 15, 10, 0, 0), "Premium", "ACTIVE"),
        ("CUST001", "John Smith", 150.00, datetime(2024, 1, 15, 11, 0, 0), "Premium", "ACTIVE"),
        ("CUST001", "John Smith", 75.00, datetime(2024, 1, 15, 12, 0, 0), "Premium", "ACTIVE"),
        
        # Same customer, different timestamps (keep most recent)
        ("CUST002", "Jane Doe", 100.00, datetime(2024, 1, 15, 9, 0, 0), "Standard", "ACTIVE"),
        ("CUST002", "Jane Doe", 100.00, datetime(2024, 1, 15, 10, 0, 0), "Standard", "SUSPENDED"),
        ("CUST002", "Jane Doe", 100.00, datetime(2024, 1, 15, 11, 0, 0), "Standard", "ACTIVE"),
        
        # Same customer, complex criteria (amount + status + time)
        ("CUST003", "Bob Wilson", 200.00, datetime(2024, 1, 15, 8, 0, 0), "Basic", "SUSPENDED"),
        ("CUST003", "Bob Wilson", 180.00, datetime(2024, 1, 15, 9, 0, 0), "Premium", "ACTIVE"),
        ("CUST003", "Bob Wilson", 220.00, datetime(2024, 1, 15, 7, 0, 0), "Basic", "ACTIVE"),
        
        # Different customers (no duplicates)
        ("CUST004", "Alice Brown", 300.00, datetime(2024, 1, 15, 10, 0, 0), "Premium", "ACTIVE"),
        ("CUST005", "Charlie Davis", 120.00, datetime(2024, 1, 15, 11, 0, 0), "Standard", "ACTIVE"),
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("transaction_amount", DoubleType(), False),
        StructField("last_updated", TimestampType(), False),
        StructField("tier_level", StringType(), True),
        StructField("account_status", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

def keep_highest_value(df):
    """Keep record with highest transaction amount for each customer"""
    window_spec = Window.partitionBy("customer_id").orderBy(desc("transaction_amount"))
    
    return df.withColumn("amount_rank", row_number().over(window_spec)) \
             .filter(col("amount_rank") == 1) \
             .drop("amount_rank")

def keep_most_recent(df):
    """Keep the most recent record for each customer"""
    window_spec = Window.partitionBy("customer_id").orderBy(desc("last_updated"))
    
    return df.withColumn("time_rank", row_number().over(window_spec)) \
             .filter(col("time_rank") == 1) \
             .drop("time_rank")

def keep_best_by_multiple_criteria(df):
    """Advanced: Keep best record using multiple criteria with priority"""
    
    # Create priority score: Premium=3, Standard=2, Basic=1
    df_with_priority = df.withColumn("tier_priority",
                                   when(col("tier_level") == "Premium", 3)
                                   .when(col("tier_level") == "Standard", 2)
                                   .when(col("tier_level") == "Basic", 1)
                                   .otherwise(0)) \
                        .withColumn("status_priority", 
                                  when(col("account_status") == "ACTIVE", 1)
                                  .otherwise(0))
    
    # Complex ranking: Tier priority > Status > Amount > Recency
    window_spec = Window.partitionBy("customer_id") \
                       .orderBy(desc("tier_priority"), desc("status_priority"), 
                               desc("transaction_amount"), desc("last_updated"))
    
    return df_with_priority.withColumn("overall_rank", row_number().over(window_spec)) \
                          .filter(col("overall_rank") == 1) \
                          .drop("tier_priority", "status_priority", "overall_rank")

def aggregate_duplicates(df):
    """Alternative approach: Aggregate duplicate records instead of removing them"""
    
    # Group by customer and aggregate the duplicates
    return df.groupBy("customer_id", "customer_name") \
             .agg(
                 spark_max("transaction_amount").alias("max_transaction_amount"),
                 spark_sum("transaction_amount").alias("total_transaction_amount"),
                 count("*").alias("transaction_count"),
                 spark_max("last_updated").alias("most_recent_update"),
                 first("tier_level").alias("current_tier_level"),
                 last("account_status").alias("latest_account_status"),
                 collect_list(struct("transaction_amount", "last_updated", "tier_level", "account_status")).alias("all_records")
             ) \
             .drop("all_records")  # Remove for cleaner output

def rank_based_deduplication(df):
    """Use rank() instead of row_number() to handle ties differently"""
    
    # rank() gives same rank to ties, dense_rank() doesn't skip ranks
    window_spec = Window.partitionBy("customer_id").orderBy(desc("transaction_amount"))
    
    return df.withColumn("amount_rank", rank().over(window_spec)) \
             .withColumn("amount_dense_rank", dense_rank().over(window_spec)) \
             .filter(col("amount_rank") == 1)  # Keeps all tied records

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("AdvancedDeduplication") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Create sample data
    df = create_advanced_sample_data(spark)
    print(f"Original records: {df.count()}")
    df.show(truncate=False)
    
    print("\n" + "="*60)
    print("METHOD 1: Keep Highest Transaction Amount")
    print("="*60)
    highest_result = keep_highest_value(df)
    print(f"Records after keeping highest amount: {highest_result.count()}")
    highest_result.select("customer_id", "customer_name", "transaction_amount", "last_updated").show()
    
    print("\n" + "="*60)
    print("METHOD 2: Keep Most Recent Record")
    print("="*60)
    recent_result = keep_most_recent(df)
    print(f"Records after keeping most recent: {recent_result.count()}")
    recent_result.select("customer_id", "customer_name", "transaction_amount", "last_updated", "account_status").show()
    
    print("\n" + "="*60)
    print("METHOD 3: Multi-Criteria Priority Ranking")
    print("="*60)
    best_result = keep_best_by_multiple_criteria(df)
    print(f"Records after multi-criteria selection: {best_result.count()}")
    best_result.show()
    
    print("\n" + "="*60)
    print("METHOD 4: Aggregate Instead of Remove")
    print("="*60)
    agg_result = aggregate_duplicates(df)
    print(f"Aggregated customer records: {agg_result.count()}")
    agg_result.show()
    
    print("\n" + "="*60)
    print("METHOD 5: Rank vs Row Number (Handling Ties)")
    print("="*60)
    rank_result = rank_based_deduplication(df)
    print(f"Records with rank-based deduplication: {rank_result.count()}")
    rank_result.select("customer_id", "transaction_amount", "amount_rank", "amount_dense_rank").show()
    
    spark.stop() 