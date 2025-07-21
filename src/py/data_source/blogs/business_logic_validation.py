from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def validate_transaction_business_rules(df):
    """
    Business Logic Validation - ensuring data makes business sense
    
    Goes beyond schema validation to check ranges, categories, and business constraints.
    """
    
    # Define business rules
    VALID_MERCHANTS = ["grocery", "restaurant", "gas_station", "retail", "entertainment", "other"]
    VALID_TIERS = ["bronze", "silver", "gold", "platinum"]
    MIN_AMOUNT = 0.01
    MAX_AMOUNT = 10000.0
    
    # Apply validation rules
    validated_df = df.withColumn(
        "validation_status",
        when(col("transaction_amount") < MIN_AMOUNT, "negative_or_zero_amount")
        .when(col("transaction_amount") > MAX_AMOUNT, "excessive_amount")
        .when(~col("merchant_category").isin(VALID_MERCHANTS), "invalid_merchant_category")
        .when(~col("customer_tier").isin(VALID_TIERS), "invalid_customer_tier")
        .otherwise("valid")
    )
    
    # Separate valid from invalid data
    valid_df = validated_df.filter(col("validation_status") == "valid").drop("validation_status")
    invalid_df = validated_df.filter(col("validation_status") != "valid")
    
    # Show validation summary
    total = validated_df.count()
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    success_rate = (valid_count / total * 100) if total > 0 else 0
    
    print(f"📊 Business Logic Validation: {valid_count}/{total} valid ({success_rate:.1f}%)")
    
    if invalid_count > 0:
        print("🔍 Validation issues:")
        validated_df.groupBy("validation_status").count().show()
    
    return valid_df, invalid_df

if __name__ == "__main__":
    # Test the validation function
    spark = SparkSession.builder.appName("BusinessLogicValidation").master("local[*]").getOrCreate()
    
    # Create test data with various business rule violations
    test_data = [
        # Valid records
        ("user_001", 45.99, "grocery", "gold"),
        ("user_002", 125.50, "restaurant", "silver"),
        ("user_003", 89.99, "retail", "bronze"),
        
        # Invalid records
        ("user_004", -10.50, "grocery", "gold"),           # Negative amount
        ("user_005", 15000.00, "restaurant", "silver"),    # Excessive amount
        ("user_006", 50.00, "cryptocurrency", "gold"),     # Invalid merchant
        ("user_007", 75.25, "grocery", "diamond"),         # Invalid tier
        ("user_008", 0.00, "gas_station", "platinum"),     # Zero amount
    ]
    
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("transaction_amount", DoubleType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("customer_tier", StringType(), False)
    ])
    
    test_df = spark.createDataFrame(test_data, schema)
    
    print("🧪 Testing Business Logic Validation")
    print("=" * 50)
    
    # Apply validation
    valid_data, invalid_data = validate_transaction_business_rules(test_df)
    
    # Verify results
    if valid_data.count() == 3 and invalid_data.count() == 5:
        print("✅ Test PASSED: 3 valid, 5 invalid records as expected")
    else:
        print(f"❌ Test FAILED: Expected 3 valid, 5 invalid. Got {valid_data.count()} valid, {invalid_data.count()} invalid")
    
    spark.stop() 