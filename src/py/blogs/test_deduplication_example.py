import pytest
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime
import sys
import os

# Add current directory to path to import our module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from deduplication_example import (
    create_sample_data,
    simple_exact_deduplication,
    business_logic_deduplication,
    advanced_deduplication_with_priority,
    analyze_duplicates
)

class TestDeduplication(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests"""
        cls.spark = SparkSession.builder \
            .appName("TestDeduplication") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Reduce log level for cleaner test output
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test"""
        self.sample_df = create_sample_data(self.spark)
        
    def test_create_sample_data(self):
        """Test that sample data is created correctly"""
        df = create_sample_data(self.spark)
        
        # Test schema
        expected_columns = ["transaction_id", "user_id", "transaction_amount", 
                          "transaction_date", "merchant_category", "customer_tier"]
        self.assertEqual(df.columns, expected_columns)
        
        # Test record count
        self.assertEqual(df.count(), 10)
        
        # Test data types
        schema = df.schema
        self.assertIsInstance(schema["transaction_id"].dataType, StringType)
        self.assertIsInstance(schema["user_id"].dataType, StringType)
        self.assertIsInstance(schema["transaction_amount"].dataType, DoubleType)
        self.assertIsInstance(schema["transaction_date"].dataType, TimestampType)
        
        # Test for expected duplicates
        txn_001_count = df.filter(df.transaction_id == "TXN001").count()
        self.assertEqual(txn_001_count, 2, "Should have 2 exact duplicates for TXN001")
        
        txn_002_count = df.filter(df.transaction_id == "TXN002").count()
        self.assertEqual(txn_002_count, 2, "Should have 2 records with TXN002")
    
    def test_simple_exact_deduplication(self):
        """Test simple exact deduplication"""
        original_count = self.sample_df.count()
        deduplicated_df = simple_exact_deduplication(self.sample_df)
        
        # Should remove exactly 1 duplicate (the exact TXN001 duplicate)
        self.assertEqual(deduplicated_df.count(), original_count - 1)
        self.assertEqual(deduplicated_df.count(), 9)
        
        # Verify no exact duplicates remain
        distinct_count = deduplicated_df.distinct().count()
        self.assertEqual(deduplicated_df.count(), distinct_count)
        
        # Verify specific duplicate is removed
        txn_001_count = deduplicated_df.filter(deduplicated_df.transaction_id == "TXN001").count()
        self.assertEqual(txn_001_count, 1, "Should have only 1 TXN001 record after deduplication")
    
    def test_business_logic_deduplication(self):
        """Test business logic deduplication"""
        original_count = self.sample_df.count()
        deduplicated_df = business_logic_deduplication(self.sample_df)
        
        # Should remove more records due to business rules
        self.assertLess(deduplicated_df.count(), original_count)
        
        # Verify transaction_id uniqueness (Rule 1)
        unique_txn_ids = deduplicated_df.select("transaction_id").distinct().count()
        self.assertEqual(deduplicated_df.count(), unique_txn_ids, 
                        "Each transaction_id should appear only once")
        
        # Verify no TXN002 duplicates (should keep only first occurrence)
        txn_002_count = deduplicated_df.filter(deduplicated_df.transaction_id == "TXN002").count()
        self.assertEqual(txn_002_count, 1, "Should have only 1 TXN002 record")
        
        # Verify USER123 + 49.99 double-click detection (Rule 2)
        # TXN003 and TXN004 are 30 seconds apart, so TXN004 should be removed
        user123_4999_count = deduplicated_df.filter(
            (deduplicated_df.user_id == "USER123") & 
            (deduplicated_df.transaction_amount == 49.99)
        ).count()
        self.assertEqual(user123_4999_count, 1, 
                        "Should detect double-click and keep only 1 record")
    
    def test_advanced_deduplication_with_priority(self):
        """Test priority-based deduplication"""
        original_count = self.sample_df.count()
        deduplicated_df = advanced_deduplication_with_priority(self.sample_df)
        
        # Should remove fewer records than business logic (only exact duplicates + priority)
        self.assertLess(deduplicated_df.count(), original_count)
        
        # Verify transaction_id uniqueness
        unique_txn_ids = deduplicated_df.select("transaction_id").distinct().count()
        self.assertEqual(deduplicated_df.count(), unique_txn_ids)
        
        # Verify priority logic: TXN002 should keep SILVER over BRONZE
        txn_002_records = deduplicated_df.filter(deduplicated_df.transaction_id == "TXN002").collect()
        self.assertEqual(len(txn_002_records), 1)
        self.assertEqual(txn_002_records[0]["customer_tier"], "SILVER", 
                        "Should keep SILVER tier over BRONZE due to priority")
        self.assertEqual(txn_002_records[0]["user_id"], "USER456", 
                        "Should keep USER456 (SILVER) over USER789 (BRONZE)")
    
    def test_empty_dataframe(self):
        """Test deduplication with empty dataframe"""
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("transaction_amount", DoubleType(), False),
            StructField("transaction_date", TimestampType(), False),
            StructField("merchant_category", StringType(), True),
            StructField("customer_tier", StringType(), True)
        ])
        
        empty_df = self.spark.createDataFrame([], schema)
        
        # All deduplication methods should handle empty dataframes
        simple_result = simple_exact_deduplication(empty_df)
        self.assertEqual(simple_result.count(), 0)
        
        business_result = business_logic_deduplication(empty_df)
        self.assertEqual(business_result.count(), 0)
        
        priority_result = advanced_deduplication_with_priority(empty_df)
        self.assertEqual(priority_result.count(), 0)
    
    def test_single_record_dataframe(self):
        """Test deduplication with single record"""
        single_record_data = [
            ("TXN999", "USER999", 100.0, datetime(2024, 1, 15, 10, 0, 0), "TEST", "GOLD")
        ]
        
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("transaction_amount", DoubleType(), False),
            StructField("transaction_date", TimestampType(), False),
            StructField("merchant_category", StringType(), True),
            StructField("customer_tier", StringType(), True)
        ])
        
        single_df = self.spark.createDataFrame(single_record_data, schema)
        
        # All methods should return the same single record
        simple_result = simple_exact_deduplication(single_df)
        self.assertEqual(simple_result.count(), 1)
        
        business_result = business_logic_deduplication(single_df)
        self.assertEqual(business_result.count(), 1)
        
        priority_result = advanced_deduplication_with_priority(single_df)
        self.assertEqual(priority_result.count(), 1)
    
    def test_all_unique_records(self):
        """Test deduplication with all unique records"""
        unique_data = [
            ("TXN100", "USER100", 50.0, datetime(2024, 1, 15, 10, 0, 0), "RETAIL", "GOLD"),
            ("TXN101", "USER101", 60.0, datetime(2024, 1, 15, 11, 0, 0), "GROCERY", "SILVER"),
            ("TXN102", "USER102", 70.0, datetime(2024, 1, 15, 12, 0, 0), "ONLINE", "BRONZE"),
        ]
        
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("transaction_amount", DoubleType(), False),
            StructField("transaction_date", TimestampType(), False),
            StructField("merchant_category", StringType(), True),
            StructField("customer_tier", StringType(), True)
        ])
        
        unique_df = self.spark.createDataFrame(unique_data, schema)
        
        # All methods should return all 3 records
        simple_result = simple_exact_deduplication(unique_df)
        self.assertEqual(simple_result.count(), 3)
        
        business_result = business_logic_deduplication(unique_df)
        self.assertEqual(business_result.count(), 3)
        
        priority_result = advanced_deduplication_with_priority(unique_df)
        self.assertEqual(priority_result.count(), 3)
    
    def test_null_values_handling(self):
        """Test deduplication with null values"""
        null_data = [
            ("TXN200", "USER200", 100.0, datetime(2024, 1, 15, 10, 0, 0), None, None),
            ("TXN200", "USER200", 100.0, datetime(2024, 1, 15, 10, 0, 0), "RETAIL", "GOLD"),
            ("TXN201", "USER201", 200.0, datetime(2024, 1, 15, 11, 0, 0), None, "SILVER"),
        ]
        
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("transaction_amount", DoubleType(), False),
            StructField("transaction_date", TimestampType(), False),
            StructField("merchant_category", StringType(), True),
            StructField("customer_tier", StringType(), True)
        ])
        
        null_df = self.spark.createDataFrame(null_data, schema)
        
        # Test that null handling works - these records are NOT exact duplicates due to different null values
        simple_result = simple_exact_deduplication(null_df)
        self.assertEqual(simple_result.count(), 3)  # All records are unique when considering nulls
        
        priority_result = advanced_deduplication_with_priority(null_df)
        # Priority should keep the GOLD tier record over null
        txn_200_records = priority_result.filter(priority_result.transaction_id == "TXN200").collect()
        self.assertEqual(len(txn_200_records), 1)
        self.assertEqual(txn_200_records[0]["customer_tier"], "GOLD")
    
    def test_time_window_edge_cases(self):
        """Test business logic time window edge cases"""
        # Test 2 simple cases: one within 60 seconds, one at exactly 61 seconds
        edge_case_data = [
            ("TXN300", "USER300", 100.0, datetime(2024, 1, 15, 12, 0, 0), "RETAIL", "GOLD"),
            ("TXN301", "USER300", 100.0, datetime(2024, 1, 15, 12, 0, 59), "RETAIL", "GOLD"),  # 59 seconds later (should be removed)
            # Separate user/amount to test 61 seconds boundary
            ("TXN302", "USER300", 200.0, datetime(2024, 1, 15, 12, 0, 0), "RETAIL", "GOLD"),
            ("TXN303", "USER300", 200.0, datetime(2024, 1, 15, 12, 1, 1), "RETAIL", "GOLD"),   # 61 seconds later (should be kept)
        ]
        
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("transaction_amount", DoubleType(), False),
            StructField("transaction_date", TimestampType(), False),
            StructField("merchant_category", StringType(), True),
            StructField("customer_tier", StringType(), True)
        ])
        
        edge_df = self.spark.createDataFrame(edge_case_data, schema)
        
        business_result = business_logic_deduplication(edge_df)
        
        # Should have 3 records: TXN300, TXN302, TXN303 (TXN301 removed for being within 60 seconds)
        self.assertEqual(business_result.count(), 3)
        
        # Verify TXN300 exists (first transaction)
        txn_300_exists = business_result.filter(business_result.transaction_id == "TXN300").count()
        self.assertEqual(txn_300_exists, 1)
        
        # Verify TXN301 doesn't exist (59 seconds = duplicate)
        txn_301_exists = business_result.filter(business_result.transaction_id == "TXN301").count()
        self.assertEqual(txn_301_exists, 0)
        
        # Verify TXN302 exists (different amount)
        txn_302_exists = business_result.filter(business_result.transaction_id == "TXN302").count()
        self.assertEqual(txn_302_exists, 1)
        
        # Verify TXN303 exists (61 seconds = allowed)
        txn_303_exists = business_result.filter(business_result.transaction_id == "TXN303").count()
        self.assertEqual(txn_303_exists, 1)

    def test_performance_with_large_dataset(self):
        """Test performance doesn't degrade significantly with larger datasets"""
        import time
        
        # Create a larger dataset
        large_data = []
        for i in range(1000):
            # Add some duplicates every 100 records
            if i % 100 == 0 and i > 0:
                # Duplicate the previous record
                large_data.append(large_data[-1])
            else:
                                 large_data.append((
                     f"TXN{i:04d}",
                     f"USER{i%50:03d}",  # 50 unique users
                     float(i % 1000),
                     datetime(2024, 1, 15, 10, i % 60, i % 60),  # Spread minutes and seconds
                     "RETAIL",
                     ["GOLD", "SILVER", "BRONZE"][i % 3]
                 ))
        
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("transaction_amount", DoubleType(), False),
            StructField("transaction_date", TimestampType(), False),
            StructField("merchant_category", StringType(), True),
            StructField("customer_tier", StringType(), True)
        ])
        
        large_df = self.spark.createDataFrame(large_data, schema)
        
        # Test that operations complete in reasonable time
        start_time = time.time()
        result = simple_exact_deduplication(large_df)
        result.count()  # Force evaluation
        end_time = time.time()
        
        # Should complete within 5 seconds for 1000 records
        self.assertLess(end_time - start_time, 5.0, "Performance test failed - too slow")
        
        # Verify some duplicates were removed
        self.assertLess(result.count(), large_df.count())

def run_tests():
    """Run all tests with detailed output"""
    print("🧪 Running Deduplication Tests")
    print("=" * 50)
    
    # Create test suite
    unittest.main(argv=[''], verbosity=2, exit=False)

if __name__ == "__main__":
    run_tests() 