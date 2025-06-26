"""
Test cases for the Simple DataSource implementation
Tests the custom Python data source using Spark 4.0 DataSource API
"""

import os
import sys
import shutil
sys.path.append('.')

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from simple_ds import SimpleDataSource


def setup_spark():
    """Set up Spark session with simple data source registered."""
    spark = SparkSession.builder \
        .appName("SimpleDataSourceTest") \
        .master("local[*]") \
        .getOrCreate()
    
    # Register our custom data source
    spark.dataSource.register(SimpleDataSource)
    return spark


def cleanup_test_files(test_paths):
    """Clean up test files after tests."""
    for path in test_paths:
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)


def test_case_1_basic_functionality(spark: SparkSession):
    """
    Test Case 1: Basic functionality with synthetic data
    Tests reading synthetic data and writing/reading custom data
    """
    print("\n=== Test Case 1: Basic Functionality ===")
    
    # Test reading synthetic data
    print("Testing synthetic data reading:")
    synthetic_df = spark.read.format("simple").load()
    synthetic_data = synthetic_df.collect()
    print("Synthetic data:")
    synthetic_df.show()
    
    # Verify synthetic data
    if len(synthetic_data) == 2:
        print("✓ Synthetic data has correct row count")
    else:
        print("✗ Synthetic data has incorrect row count")
        return False
    
    expected_names = {"Alice", "Bob"}
    actual_names = {row['name'] for row in synthetic_data}
    if actual_names == expected_names:
        print("✓ Synthetic data has correct names")
    else:
        print("✗ Synthetic data has incorrect names")
        return False
    
    expected_ages = {20, 30}
    actual_ages = {row['age'] for row in synthetic_data}
    if actual_ages == expected_ages:
        print("✓ Synthetic data has correct ages")
    else:
        print("✗ Synthetic data has incorrect ages")
        return False
    
    # Test writing and reading custom data
    print("\nTesting write and read functionality:")
    test_data = [
        Row(name="Charlie", age=25),
        Row(name="Diana", age=28),
        Row(name="Eve", age=32)
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(test_data)
    print("Original DataFrame:")
    df.show()
    
    # Test file path
    test_path = "test_data/simple_basic.txt"
    
    try:
        # Write using our custom data source
        df.coalesce(1).write.format("simple").mode("overwrite").option("path", test_path).save()
        print(f"✓ Successfully wrote data to {test_path}")
        
        # Read back using our custom data source
        read_df = spark.read.format("simple").option("path", test_path).load()
        print("Read back DataFrame:")
        read_df.show()
        
        # Verify data integrity
        original_count = df.count()
        read_count = read_df.count()
        print(f"Original row count: {original_count}")
        print(f"Read back row count: {read_count}")
        
        if original_count == read_count:
            print("✓ Row count matches")
        else:
            print("✗ Row count mismatch")
            return False
        
        # Verify schema
        expected_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        
        if read_df.schema == expected_schema:
            print("✓ Schema matches expected")
        else:
            print("✗ Schema mismatch")
            print(f"Expected: {expected_schema}")
            print(f"Actual: {read_df.schema}")
            return False
        
        print("✓ Test Case 1 PASSED")
        return True
        
    except Exception as e:
        print(f"✗ Test Case 1 FAILED: {e}")
        return False
    finally:
        cleanup_test_files([test_path])


def test_case_2_file_reading(spark: SparkSession):
    """
    Test Case 2: Reading from manually created files
    Tests reading from files with different formats and edge cases
    """
    print("\n=== Test Case 2: File Reading ===")
    
    # Test reading from manually created file
    test_path = "test_data/simple_manual.txt"
    
    try:
        # Create a test file manually
        with open(test_path, "w") as f:
            f.write("Zoe,40\n")
            f.write("Yann,35\n")
            f.write("Alice,25\n")
        
        print(f"✓ Created test file: {test_path}")
        
        # Read using our custom data source
        read_df = spark.read.format("simple").option("path", test_path).load()
        print("Read DataFrame from manual file:")
        read_df.show()
        
        # Verify data
        data = read_df.collect()
        if len(data) == 3:
            print("✓ Correct number of rows read")
        else:
            print("✗ Incorrect number of rows read")
            return False
        
        expected_names = {"Zoe", "Yann", "Alice"}
        actual_names = {row['name'] for row in data}
        if actual_names == expected_names:
            print("✓ Correct names read")
        else:
            print("✗ Incorrect names read")
            return False
        
        expected_ages = {40, 35, 25}
        actual_ages = {row['age'] for row in data}
        if actual_ages == expected_ages:
            print("✓ Correct ages read")
        else:
            print("✗ Incorrect ages read")
            return False
        
        # Test reading file with empty lines and invalid data
        test_path_edge = "test_data/simple_edge_cases.txt"
        with open(test_path_edge, "w") as f:
            f.write("Valid,30\n")
            f.write("\n")  # Empty line
            f.write("Invalid,abc\n")  # Invalid age
            f.write("Another,45\n")
        
        print(f"✓ Created edge case test file: {test_path_edge}")
        
        read_df_edge = spark.read.format("simple").option("path", test_path_edge).load()
        print("Read DataFrame from edge case file:")
        read_df_edge.show()
        
        # Should skip invalid data, so expect 2 valid rows
        edge_data = read_df_edge.collect()
        if len(edge_data) == 2:
            print("✓ Correctly handled edge cases (skipped invalid data)")
        else:
            print("✗ Did not handle edge cases correctly")
            return False
        
        print("✓ Test Case 2 PASSED")
        return True
        
    except Exception as e:
        print(f"✗ Test Case 2 FAILED: {e}")
        return False
    finally:
        cleanup_test_files([test_path, test_path_edge])


def test_case_3_error_handling_and_edge_cases(spark: SparkSession):
    """
    Test Case 3: Error handling and edge cases
    Tests handling of null values, invalid data, and edge cases
    """
    print("\n=== Test Case 3: Error Handling and Edge Cases ===")
    
    # Create test data with null values and edge cases
    test_data = [
        Row(name="Valid Name", age=25),
        Row(name=None, age=30),  # Null name
        Row(name="Valid Name", age=None),  # Null age
        Row(name="", age=40),  # Empty string name
        Row(name="Valid Name", age=0),  # Zero age
        Row(name="Very Long Name That Exceeds Normal Length", age=45)
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(test_data)
    print("Original DataFrame with edge cases:")
    df.show()
    
    # Test file path
    test_path = "test_data/simple_edge_cases.txt"
    
    try:
        # Write using our custom data source
        df.coalesce(1).write.format("simple").mode("overwrite").option("path", test_path).save()
        print(f"✓ Successfully wrote data with edge cases to {test_path}")
        
        # Read back using our custom data source
        read_df = spark.read.format("simple").option("path", test_path).load()
        print("Read back DataFrame with edge cases:")
        read_df.show()
        
        # Verify data integrity
        original_count = df.count()
        read_count = read_df.count()
        print(f"Original row count: {original_count}")
        print(f"Read back row count: {read_count}")
        
        if original_count == read_count:
            print("✓ Row count matches")
        else:
            print("✗ Row count mismatch")
            return False
        
        # Test reading from non-existent file (should return empty DataFrame)
        non_existent_path = "test_data/non_existent.txt"
        empty_df = spark.read.format("simple").option("path", non_existent_path).load()
        empty_count = empty_df.count()
        
        if empty_count == 0:
            print("✓ Correctly handles non-existent file (returns empty DataFrame)")
        else:
            print("✗ Does not handle non-existent file correctly")
            return False
        
        # Test reading without path (should return synthetic data)
        synthetic_df = spark.read.format("simple").load()
        synthetic_count = synthetic_df.count()
        
        if synthetic_count == 2:
            print("✓ Correctly returns synthetic data when no path specified")
        else:
            print("✗ Does not return synthetic data correctly")
            return False
        
        print("✓ Test Case 3 PASSED")
        return True
        
    except Exception as e:
        print(f"✗ Test Case 3 FAILED: {e}")
        return False
    finally:
        cleanup_test_files([test_path])


def run_all_tests():
    """Run all test cases."""
    print("Starting Simple DataSource Tests")
    print("=" * 50)
    
    spark = None
    try:
        spark = setup_spark()
        
        test_results = []
        
        # Run all test cases
        test_results.append(test_case_1_basic_functionality(spark))
        test_results.append(test_case_2_file_reading(spark))
        test_results.append(test_case_3_error_handling_and_edge_cases(spark))
        
        # Summary
        print("\n" + "=" * 50)
        print("TEST SUMMARY")
        print("=" * 50)
        
        passed = sum(test_results)
        total = len(test_results)
        
        print(f"Tests passed: {passed}/{total}")
        
        if passed == total:
            print("🎉 ALL TESTS PASSED!")
        else:
            print("❌ SOME TESTS FAILED!")
            
        return passed == total
        
    except Exception as e:
        print(f"Test setup failed: {e}")
        return False
    finally:
        if spark:
            spark.stop()


def run_specific_test(test_name: str):
    """Run a specific test case."""
    spark = None
    try:
        spark = setup_spark()
        
        if test_name == "basic":
            return test_case_1_basic_functionality(spark)
        elif test_name == "file":
            return test_case_2_file_reading(spark)
        elif test_name == "error":
            return test_case_3_error_handling_and_edge_cases(spark)
        else:
            print(f"Unknown test: {test_name}")
            return False
            
    except Exception as e:
        print(f"Test failed: {e}")
        return False
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Run specific test
        test_name = sys.argv[1]
        success = run_specific_test(test_name)
        sys.exit(0 if success else 1)
    else:
        # Run all tests
        success = run_all_tests()
        sys.exit(0 if success else 1)
   