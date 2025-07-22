from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, when, max as spark_max, sum as spark_sum, count
from pyspark.sql.window import Window
from datetime import datetime

def create_sample_data(spark):
    """Sample customer data with duplicates"""
    data = [
        ("CUST001", "John Smith", 50.00, datetime(2024, 1, 15, 10, 0, 0), "Basic", "ACTIVE"),
        ("CUST001", "John Smith", 150.00, datetime(2024, 1, 15, 11, 0, 0), "Premium", "ACTIVE"),
        ("CUST002", "Jane Doe", 100.00, datetime(2024, 1, 15, 9, 0, 0), "Standard", "SUSPENDED"),
        ("CUST002", "Jane Doe", 100.00, datetime(2024, 1, 15, 11, 0, 0), "Standard", "ACTIVE"),
        ("CUST003", "Bob Wilson", 220.00, datetime(2024, 1, 15, 8, 0, 0), "Basic", "ACTIVE"),
        ("CUST003", "Bob Wilson", 180.00, datetime(2024, 1, 15, 10, 0, 0), "Premium", "ACTIVE"),
    ]
    columns = ["customer_id", "name", "amount", "timestamp", "tier", "status"]
    return spark.createDataFrame(data, columns)

def keep_highest_value(df):
    """Keep record with highest amount per customer"""
    window = Window.partitionBy("customer_id").orderBy(desc("amount"))
    return df.withColumn("rank", row_number().over(window)) \
             .filter(col("rank") == 1).drop("rank")

def keep_most_recent(df):
    """Keep most recent record per customer"""
    window = Window.partitionBy("customer_id").orderBy(desc("timestamp"))
    return df.withColumn("rank", row_number().over(window)) \
             .filter(col("rank") == 1).drop("rank")

def multi_criteria_best(df):
    """Advanced: Keep best record using business priority"""
    df_priority = df.withColumn("tier_score", 
                               when(col("tier") == "Premium", 3)
                               .when(col("tier") == "Standard", 2)
                               .otherwise(1)) \
                    .withColumn("status_score", 
                               when(col("status") == "ACTIVE", 1).otherwise(0))
    
    window = Window.partitionBy("customer_id") \
                  .orderBy(desc("tier_score"), desc("status_score"), 
                          desc("amount"), desc("timestamp"))
    
    return df_priority.withColumn("rank", row_number().over(window)) \
                     .filter(col("rank") == 1) \
                     .drop("tier_score", "status_score", "rank")

def aggregate_duplicates(df):
    """Alternative: Aggregate instead of removing duplicates"""
    return df.groupBy("customer_id", "name") \
             .agg(spark_max("amount").alias("max_amount"),
                  spark_sum("amount").alias("total_amount"),
                  count("*").alias("record_count"),
                  spark_max("timestamp").alias("latest_update"))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("AdvancedDedup").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = create_sample_data(spark)
    print(f"Original: {df.count()} records")
    
    # Method 1: Keep highest value
    highest = keep_highest_value(df)
    print(f"Highest value: {highest.count()} records")
    highest.select("customer_id", "name", "amount", "tier").show()
    
    # Method 2: Keep most recent
    recent = keep_most_recent(df)
    print(f"Most recent: {recent.count()} records")
    recent.select("customer_id", "name", "timestamp", "status").show()
    
    # Method 3: Multi-criteria priority
    best = multi_criteria_best(df)
    print(f"Multi-criteria best: {best.count()} records")
    best.show()
    
    # Method 4: Aggregate instead of remove
    agg = aggregate_duplicates(df)
    print(f"Aggregated: {agg.count()} records")
    agg.show()
    
    spark.stop() 