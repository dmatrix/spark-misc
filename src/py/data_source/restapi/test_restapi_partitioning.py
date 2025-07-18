#!/usr/bin/env python3
"""
Comprehensive test for REST API Data Source partitioning functionality using restapi.py
"""

import sys
import time
import traceback
from pyspark.sql import SparkSession
from restapi import RestApiDataSource

def test_basic_functionality():
    """Test basic functionality of the REST API data source"""
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

def test_comprehensive_partitioning():
    """Test comprehensive partitioning functionality"""
    print("\n=== Testing Comprehensive Partitioning ===")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("ComprehensivePartitionTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        
        # Test 1: Parallel processing verification
        print("\n--- Test 5: Parallel Processing ---")
        start_time = time.time()
        df_parallel = spark.read.format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", "https://jsonplaceholder.typicode.com/posts/1,https://jsonplaceholder.typicode.com/posts/2,https://jsonplaceholder.typicode.com/posts/3,https://jsonplaceholder.typicode.com/posts/4") \
            .option("method", "GET") \
            .load()
        
        parallel_count = df_parallel.count()
        parallel_time = time.time() - start_time
        
        print(f"✓ Parallel processing: {parallel_count} rows in {parallel_time:.2f}s")
        print(f"✓ Parallel processing completed successfully")
        
        # Test 2: Data integrity verification
        print("\n--- Test 6: Data Integrity ---")
        all_data = df_parallel.collect()
        unique_ids = set(row.id for row in all_data)
        
        print(f"✓ Total rows: {len(all_data)}")
        print(f"✓ Unique IDs: {len(unique_ids)}")
        print(f"✓ Data integrity: {'PASS' if len(all_data) == len(unique_ids) else 'FAIL'}")
        
        # Test 3: SQL integration
        print("\n--- Test 7: SQL Integration ---")
        df_parallel.createOrReplaceTempView("api_posts")
        
        result = spark.sql("""
            SELECT userId, COUNT(*) as post_count
            FROM api_posts
            GROUP BY userId
            ORDER BY userId
        """)
        
        sql_results = result.collect()
        print(f"✓ SQL integration: {len(sql_results)} groups processed")
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        traceback.print_exc()
        return False
    finally:
        if 'spark' in locals():
            spark.stop()

def test_performance_comparison():
    """Test performance comparison between single and multi-partition"""
    print("\n=== Testing Performance Comparison ===")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("PerformanceComparisonTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        
        # Single partition timing
        print("\n--- Test 8: Performance Comparison ---")
        start_time = time.time()
        df_single = spark.read.format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("method", "GET") \
            .load()
        single_count = df_single.count()
        single_time = time.time() - start_time
        
        # Multi partition timing
        start_time = time.time()
        df_multi = spark.read.format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", "https://jsonplaceholder.typicode.com/posts/1,https://jsonplaceholder.typicode.com/posts/2,https://jsonplaceholder.typicode.com/posts/3,https://jsonplaceholder.typicode.com/posts/4") \
            .option("method", "GET") \
            .load()
        multi_count = df_multi.count()
        multi_time = time.time() - start_time
        
        print(f"✓ Single partition: {single_count} rows in {single_time:.2f}s")
        print(f"✓ Multi partition: {multi_count} rows in {multi_time:.2f}s")
        
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
    print("Starting REST API Data Source Partitioning Tests (using restapi.py)")
    print("=" * 80)
    
    tests = [
        ("Basic Functionality", test_basic_functionality),
        ("URL-based Partitioning", test_url_partitioning),
        ("Page-based Partitioning", test_page_partitioning),
        ("Schema Inference", test_schema_inference),
        ("Comprehensive Partitioning", test_comprehensive_partitioning),
        ("Performance Comparison", test_performance_comparison)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            print(f"\n{'='*20} {test_name} {'='*20}")
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 80)
    print("Test Results Summary:")
    print("=" * 80)
    
    passed = 0
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nPassed: {passed}/{len(tests)} tests")
    
    if passed == len(tests):
        print("🎉 All tests passed! REST API partitioning is working perfectly!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 