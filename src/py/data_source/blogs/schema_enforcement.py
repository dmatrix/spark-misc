from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def load_with_schema_enforcement(spark, file_path):
    """
    Example 1: Strict Schema Enforcement
    
    Uses FAILFAST mode - fails immediately if data doesn't match schema.
    Perfect for clean data pipelines where you want to catch issues early.
    """
    
    # Define strict schema
    schema = StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("transaction_amount", DoubleType(), nullable=False),
        StructField("transaction_date", TimestampType(), nullable=True),
        StructField("merchant_category", StringType(), nullable=True),
        StructField("customer_tier", StringType(), nullable=True)
    ])
    
    print(f"📋 Loading {file_path} with strict schema enforcement...")
    
    try:
        # FAILFAST mode - strict enforcement
        df = spark.read.option("mode", "FAILFAST").schema(schema).json(file_path)
        
        # Force evaluation to trigger any parsing errors
        count = df.count()
        df.show(3)
        
        print(f"✅ Success: {count} records loaded with FAILFAST mode")
        return df
        
    except Exception as e:
        print(f"❌ Failed: {e}")
        return None

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SchemaEnforcement").master("local[*]").getOrCreate()
    
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
        
        result = load_with_schema_enforcement(spark, file_path)
        
        if result is not None:
            print(f"✅ FAILFAST succeeded - data is clean!")
        else:
            print(f"❌ FAILFAST failed - data has schema violations")
    
    spark.stop() 