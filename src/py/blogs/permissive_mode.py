from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col

# Define schema once at module level for efficiency
PERMISSIVE_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=True),
    StructField("transaction_amount", DoubleType(), nullable=True),
    StructField("transaction_date", TimestampType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("customer_tier", StringType(), nullable=True),
    StructField("_corrupt_record", StringType(), nullable=True)  # Captures bad records
])

def load_with_permissive_mode(spark, file_path):
    """
    Example 2: Permissive Mode with Corrupt Record Tracking
    
    Uses PERMISSIVE mode - never fails, captures bad records in _corrupt_record column.
    Perfect for messy data where you want to save what you can.
    """
    
    print(f"📋 Loading {file_path} with permissive mode...")
    
    # PERMISSIVE mode - never fails, captures corruption
    df = spark.read.option("mode", "PERMISSIVE").schema(PERMISSIVE_SCHEMA).json(file_path)
    
    total_records = df.count()
    
    # Separate good from bad records
    good_records = df.filter(col("_corrupt_record").isNull() & col("user_id").isNotNull())
    bad_records = df.filter(col("_corrupt_record").isNotNull() | col("user_id").isNull())
    
    good_count = good_records.count()
    bad_count = bad_records.count()
    success_rate = (good_count / total_records * 100) if total_records > 0 else 0
    
    print(f"📊 Results: {good_count}/{total_records} good records ({success_rate:.1f}% success)")
    
    if good_count > 0:
        print("✅ Sample good records:")
        good_records.drop("_corrupt_record").show(2, truncate=False)
    
    if bad_count > 0:
        print("❌ Sample bad records:")
        bad_records.select("_corrupt_record").show(2, truncate=False)
    
    return good_records, bad_records

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PermissiveMode").master("local[*]").getOrCreate()
    
    # Test with different data files
    test_files = [
        "data/clean_transactions.json",
        "data/mixed_transactions.json", 
        "data/invalid_transactions.json"
    ]
    
    for file_path in test_files:
        print(f"\n{'='*60}")
        print(f"🧪 TESTING: {file_path}")
        print(f"{'='*60}")
        
        good_data, bad_data = load_with_permissive_mode(spark, file_path)
        print(f"✅ PERMISSIVE always succeeds - captured {good_data.count()} good, {bad_data.count()} bad records")
    
    spark.stop() 