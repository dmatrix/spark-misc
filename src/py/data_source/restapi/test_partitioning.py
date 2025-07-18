#!/usr/bin/env python3
"""
Test script for REST API Data Source partitioning functionality
"""

import sys
import traceback
from pyspark.sql import SparkSession
from restapi import RestApiDataSource

def test_basic_functionality():
    """Test basic functionality of the data source"""
    print("=== Testing Basic Functionality ===")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("RestApiPartitionTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        print("✓ Spark session created successfully")
        
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        print("✓ Data source registered successfully")
        
        # Test 1: Single partition (default)
        print("\n--- Test 1: Single Partition ---")
        df1 = spark.read.format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts/1") \
            .option("method", "GET") \
            .load()
        
        print(f"✓ DataFrame created successfully")
        print(f"✓ Row count: {df1.count()}")
        df1.show(5, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        traceback.print_exc()
        return False
    finally:
        if 'spark' in locals():
            spark.stop()

def test_url_partitioning():
    """Test URL-based partitioning"""
    print("\n=== Testing URL-based Partitioning ===")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("RestApiURLPartitionTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        
        # Test URL-based partitioning
        print("\n--- Test 2: URL-based Partitioning ---")
        df2 = spark.read.format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", "https://jsonplaceholder.typicode.com/posts/1,https://jsonplaceholder.typicode.com/posts/2,https://jsonplaceholder.typicode.com/posts/3") \
            .option("method", "GET") \
            .load()
        
        print(f"✓ DataFrame created successfully")
        print(f"✓ Row count: {df2.count()}")
        
        # Check partition distribution using DataFrame API
        from pyspark.sql.functions import spark_partition_id
        partition_counts = [row['count'] for row in df2.select(spark_partition_id().alias("partition_id")).groupBy("partition_id").count().orderBy("partition_id").collect()]
        print(f"✓ Partition distribution: {partition_counts}")
        
        df2.show(5, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        traceback.print_exc()
        return False
    finally:
        if 'spark' in locals():
            spark.stop()

def test_page_partitioning():
    """Test page-based partitioning"""
    print("\n=== Testing Page-based Partitioning ===")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("RestApiPagePartitionTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        
        # Test page-based partitioning
        print("\n--- Test 3: Page-based Partitioning ---")
        df3 = spark.read.format("restapi") \
            .option("partitionStrategy", "pages") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("totalPages", "2") \
            .option("pageSize", "5") \
            .option("method", "GET") \
            .load()
        
        print(f"✓ DataFrame created successfully")
        print(f"✓ Row count: {df3.count()}")
        
        # Check partition distribution using DataFrame API
        from pyspark.sql.functions import spark_partition_id
        partition_counts = [row['count'] for row in df3.select(spark_partition_id().alias("partition_id")).groupBy("partition_id").count().orderBy("partition_id").collect()]
        print(f"✓ Partition distribution: {partition_counts}")
        
        df3.show(5, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        traceback.print_exc()
        return False
    finally:
        if 'spark' in locals():
            spark.stop()

def test_schema_inference():
    """Test schema inference with partitioning"""
    print("\n=== Testing Schema Inference ===")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("RestApiSchemaTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        
        # Test schema inference
        print("\n--- Test 4: Schema Inference ---")
        df4 = spark.read.format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts/1") \
            .option("method", "GET") \
            .load()
        
        print("✓ Schema inferred successfully:")
        df4.printSchema()
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        traceback.print_exc()
        return False
    finally:
        if 'spark' in locals():
            spark.stop()

def test_error_handling():
    """Test error handling"""
    print("\n=== Testing Error Handling ===")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("RestApiErrorTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        
        # Test with invalid URL
        print("\n--- Test 5: Invalid URL ---")
        try:
            df5 = spark.read.format("restapi") \
                .option("url", "https://invalid-url-that-does-not-exist.com/api/data") \
                .option("method", "GET") \
                .load()
            
            df5.count()  # This should fail
            print("✗ Expected error but got success")
            return False
        except Exception as e:
            print(f"✓ Correctly handled invalid URL error: {type(e).__name__}")
        
        # Test with missing URL
        print("\n--- Test 6: Missing URL ---")
        try:
            df6 = spark.read.format("restapi") \
                .option("method", "GET") \
                .load()
            
            df6.count()  # This should fail
            print("✗ Expected error but got success")
            return False
        except Exception as e:
            print(f"✓ Correctly handled missing URL error: {type(e).__name__}")
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        traceback.print_exc()
        return False
    finally:
        if 'spark' in locals():
            spark.stop()

def main():
    """Run all tests"""
    print("Starting REST API Data Source Partitioning Tests")
    print("=" * 60)
    
    tests = [
        ("Basic Functionality", test_basic_functionality),
        ("URL-based Partitioning", test_url_partitioning),
        ("Page-based Partitioning", test_page_partitioning),
        ("Schema Inference", test_schema_inference),
        ("Error Handling", test_error_handling)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 60)
    print("Test Results Summary:")
    print("=" * 60)
    
    passed = 0
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nPassed: {passed}/{len(tests)} tests")
    
    if passed == len(tests):
        print("✓ All tests passed!")
        return 0
    else:
        print("✗ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 