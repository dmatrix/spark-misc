from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col

def smart_json_load(spark, file_path):
    """
    Smart JSON Loading: Try strict first, fallback to permissive
    
    Best of both worlds:
    - Fast processing for clean data (FAILFAST)
    - Graceful recovery for messy data (PERMISSIVE)
    """
    
    # Define schema
    schema = StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("transaction_amount", DoubleType(), nullable=False),
        StructField("transaction_date", TimestampType(), nullable=True),
        StructField("merchant_category", StringType(), nullable=True),
        StructField("customer_tier", StringType(), nullable=True)
    ])
    
    print(f"📋 Smart loading {file_path}...")
    
    # Step 1: Try FAILFAST for clean data
    try:
        print("🎯 Attempting FAILFAST mode...")
        df = spark.read.option("mode", "FAILFAST").schema(schema).json(file_path)
        count = df.count()
        
        # Force full evaluation to catch parsing errors - need to process all data
        try:
            df.collect()  # This forces processing of ALL records, not just the first
            print(f"✅ FAILFAST succeeded: {count} clean records")
            return df, None
        except Exception as eval_error:
            # This catches parsing errors that occur during data access
            raise eval_error
        
    except Exception:
        print("⚠️  FAILFAST failed, switching to PERMISSIVE mode...")
        
        # Step 2: Fallback to PERMISSIVE
        schema_with_corrupt = schema.add(StructField("_corrupt_record", StringType(), True))
        df = spark.read.option("mode", "PERMISSIVE").schema(schema_with_corrupt).json(file_path)
        
        # Cache to avoid Spark 4.0 corrupt record query restrictions
        df = df.cache()
        total_records = df.count()
        
        # Separate good from bad
        good_df = df.filter(col("_corrupt_record").isNull() & col("user_id").isNotNull()).drop("_corrupt_record")
        bad_df = df.filter(col("_corrupt_record").isNotNull() | col("user_id").isNull())
        
        good_count = good_df.count()
        bad_count = bad_df.count()
        
        print(f"🔄 PERMISSIVE recovery: {good_count} good, {bad_count} bad records")
        return good_df, bad_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SmartJSONLoader").master("local[*]").getOrCreate()
    
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
        
        good_data, bad_data = smart_json_load(spark, file_path)
        
        if bad_data is None:
            print(f"🎯 Perfect! Clean data processed with FAILFAST")
        else:
            print(f"🛡️  Recovered data with PERMISSIVE fallback")
    
    spark.stop() 