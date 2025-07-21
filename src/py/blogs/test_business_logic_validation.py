from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from business_logic_validation import validate_transaction_business_rules

def test_business_logic_validation():
    """
    Comprehensive test suite for business logic validation.
    Tests various business rule violations and edge cases.
    """
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TestBusinessLogicValidation") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        print("🧪 COMPREHENSIVE BUSINESS LOGIC VALIDATION TESTS")
        print("=" * 60)
        
        # Test 1: Standard business rule violations
        print("\n📋 Test 1: Standard Business Rule Violations")
        print("-" * 50)
        
        test_data_1 = [
            # Valid records
            ("user_001", 45.99, "grocery", "gold"),
            ("user_002", 125.50, "restaurant", "silver"),
            ("user_003", 89.99, "retail", "bronze"),
            
            # Business rule violations
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
        
        test_df_1 = spark.createDataFrame(test_data_1, schema)
        valid_data_1, invalid_data_1 = validate_transaction_business_rules(test_df_1)
        
        # Verify Test 1 results
        if valid_data_1.count() == 3 and invalid_data_1.count() == 5:
            print("✅ Test 1 PASSED: 3 valid, 5 invalid records")
        else:
            print(f"❌ Test 1 FAILED: Expected 3 valid, 5 invalid. Got {valid_data_1.count()} valid, {invalid_data_1.count()} invalid")
        
        # Test 2: Edge cases and boundary values
        print("\n📋 Test 2: Edge Cases and Boundary Values")
        print("-" * 50)
        
        test_data_2 = [
            # Boundary values
            ("user_101", 0.01, "grocery", "bronze"),           # Minimum valid amount
            ("user_102", 10000.00, "restaurant", "platinum"),  # Maximum valid amount
            ("user_103", 0.009, "retail", "silver"),           # Just below minimum
            ("user_104", 10000.01, "gas_station", "gold"),     # Just above maximum
            
            # Valid edge merchants and tiers
            ("user_105", 100.00, "other", "bronze"),           # "other" category
            ("user_106", 50.00, "entertainment", "platinum"),  # All valid combinations
        ]
        
        test_df_2 = spark.createDataFrame(test_data_2, schema)
        valid_data_2, invalid_data_2 = validate_transaction_business_rules(test_df_2)
        
        # Verify Test 2 results
        expected_valid_2 = 4  # user_101, 102, 105, 106
        expected_invalid_2 = 2  # user_103, 104
        
        if valid_data_2.count() == expected_valid_2 and invalid_data_2.count() == expected_invalid_2:
            print(f"✅ Test 2 PASSED: {expected_valid_2} valid, {expected_invalid_2} invalid records")
        else:
            print(f"❌ Test 2 FAILED: Expected {expected_valid_2} valid, {expected_invalid_2} invalid. Got {valid_data_2.count()} valid, {invalid_data_2.count()} invalid")
        
        # Test 3: All valid data
        print("\n📋 Test 3: All Valid Data (Perfect Scenario)")
        print("-" * 50)
        
        test_data_3 = [
            ("user_201", 25.99, "grocery", "bronze"),
            ("user_202", 150.00, "restaurant", "silver"),
            ("user_203", 75.50, "gas_station", "gold"),
            ("user_204", 500.00, "retail", "platinum"),
            ("user_205", 99.99, "entertainment", "silver"),
        ]
        
        test_df_3 = spark.createDataFrame(test_data_3, schema)
        valid_data_3, invalid_data_3 = validate_transaction_business_rules(test_df_3)
        
        if valid_data_3.count() == 5 and invalid_data_3.count() == 0:
            print("✅ Test 3 PASSED: All 5 records valid (100% success rate)")
        else:
            print(f"❌ Test 3 FAILED: Expected all valid. Got {valid_data_3.count()} valid, {invalid_data_3.count()} invalid")
        
        # Test 4: All invalid data
        print("\n📋 Test 4: All Invalid Data (Worst Case Scenario)")
        print("-" * 50)
        
        test_data_4 = [
            ("user_301", -50.00, "grocery", "gold"),           # Negative amount
            ("user_302", 25000.00, "restaurant", "silver"),    # Excessive amount
            ("user_303", 100.00, "bitcoin", "bronze"),         # Invalid merchant
            ("user_304", 75.00, "retail", "premium"),          # Invalid tier
            ("user_305", 0.00, "unknown", "vip"),              # Multiple violations
        ]
        
        test_df_4 = spark.createDataFrame(test_data_4, schema)
        valid_data_4, invalid_data_4 = validate_transaction_business_rules(test_df_4)
        
        if valid_data_4.count() == 0 and invalid_data_4.count() == 5:
            print("✅ Test 4 PASSED: All 5 records invalid (0% success rate)")
        else:
            print(f"❌ Test 4 FAILED: Expected all invalid. Got {valid_data_4.count()} valid, {invalid_data_4.count()} invalid")
        
        # Summary
        print("\n🎯 TEST SUMMARY")
        print("=" * 60)
        total_tests = 4
        passed_tests = sum([
            valid_data_1.count() == 3 and invalid_data_1.count() == 5,
            valid_data_2.count() == 4 and invalid_data_2.count() == 2,
            valid_data_3.count() == 5 and invalid_data_3.count() == 0,
            valid_data_4.count() == 0 and invalid_data_4.count() == 5
        ])
        
        print(f"📊 Tests passed: {passed_tests}/{total_tests}")
        
        if passed_tests == total_tests:
            print("🎉 ALL TESTS PASSED! Business logic validation is working perfectly.")
        else:
            print(f"⚠️  {total_tests - passed_tests} test(s) failed. Check validation logic.")
        
        print("\n💡 Key Validation Rules Tested:")
        print("   ✅ Amount range: $0.01 - $10,000.00")
        print("   ✅ Valid merchants: grocery, restaurant, gas_station, retail, entertainment, other")
        print("   ✅ Valid tiers: bronze, silver, gold, platinum")
        print("   ✅ Cascading validation logic")
        print("   ✅ Edge case handling")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    test_business_logic_validation() 