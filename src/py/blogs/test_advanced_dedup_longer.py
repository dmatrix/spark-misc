import unittest
from pyspark.sql import SparkSession
import sys
import os

# Add current directory to path to import our module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from advanced_deduplication_longer_snippet import (
    create_advanced_sample_data,
    keep_highest_value,
    keep_most_recent,
    keep_best_by_multiple_criteria,
    aggregate_duplicates,
    rank_based_deduplication
)

class TestAdvancedDeduplicationLonger(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests"""
        cls.spark = SparkSession.builder \
            .appName("TestAdvancedDeduplicationLonger") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test"""
        self.df = create_advanced_sample_data(self.spark)
    
    def test_create_advanced_sample_data(self):
        """Test advanced sample data creation"""
        df = create_advanced_sample_data(self.spark)
        
        # Test record count
        self.assertEqual(df.count(), 11)
        
        # Test schema
        expected_columns = ["customer_id", "customer_name", "transaction_amount", 
                           "last_updated", "tier_level", "account_status"]
        self.assertEqual(df.columns, expected_columns)
        
        # Test specific customer duplicate counts
        cust001_count = df.filter(df.customer_id == "CUST001").count()
        self.assertEqual(cust001_count, 3)
        
        cust002_count = df.filter(df.customer_id == "CUST002").count()
        self.assertEqual(cust002_count, 3)
        
        cust003_count = df.filter(df.customer_id == "CUST003").count()
        self.assertEqual(cust003_count, 3)
    
    def test_keep_highest_value(self):
        """Test keeping record with highest transaction amount"""
        result_df = keep_highest_value(self.df)
        
        # Should have 5 unique customers (3 with duplicates + 2 unique)
        self.assertEqual(result_df.count(), 5)
        
        # Test specific highest values
        cust001_record = result_df.filter(result_df.customer_id == "CUST001").collect()[0]
        self.assertEqual(cust001_record["transaction_amount"], 150.0)
        
        cust002_record = result_df.filter(result_df.customer_id == "CUST002").collect()[0]
        self.assertEqual(cust002_record["transaction_amount"], 100.0)
        
        # CUST003 should keep the $220 record (highest amount)
        cust003_record = result_df.filter(result_df.customer_id == "CUST003").collect()[0]
        self.assertEqual(cust003_record["transaction_amount"], 220.0)
        self.assertEqual(cust003_record["tier_level"], "Basic")
    
    def test_keep_most_recent(self):
        """Test keeping most recent record per customer"""
        result_df = keep_most_recent(self.df)
        
        # Should have 5 unique customers
        self.assertEqual(result_df.count(), 5)
        
        # Test CUST001: should have most recent (12:00:00) with $75
        cust001_record = result_df.filter(result_df.customer_id == "CUST001").collect()[0]
        self.assertEqual(cust001_record["transaction_amount"], 75.0)
        self.assertEqual(str(cust001_record["last_updated"]), "2024-01-15 12:00:00")
        
        # Test CUST002: should have most recent (11:00:00) with ACTIVE status  
        cust002_record = result_df.filter(result_df.customer_id == "CUST002").collect()[0]
        self.assertEqual(str(cust002_record["last_updated"]), "2024-01-15 11:00:00")
        self.assertEqual(cust002_record["account_status"], "ACTIVE")
        
        # Test CUST003: should have most recent (09:00:00) Premium/ACTIVE $180
        cust003_record = result_df.filter(result_df.customer_id == "CUST003").collect()[0]
        self.assertEqual(cust003_record["transaction_amount"], 180.0)
        self.assertEqual(cust003_record["tier_level"], "Premium")
        self.assertEqual(str(cust003_record["last_updated"]), "2024-01-15 09:00:00")
    
    def test_keep_best_by_multiple_criteria(self):
        """Test multi-criteria priority ranking - the critical test!"""
        result_df = keep_best_by_multiple_criteria(self.df)
        
        # Should have 5 unique customers
        self.assertEqual(result_df.count(), 5)
        
        # Test CUST001: Should keep Premium tier record ($150)
        cust001_record = result_df.filter(result_df.customer_id == "CUST001").collect()[0]
        self.assertEqual(cust001_record["transaction_amount"], 150.0)
        self.assertEqual(cust001_record["tier_level"], "Premium")
        self.assertEqual(cust001_record["account_status"], "ACTIVE")
        
        # Test CUST002: All same tier/amount, should keep ACTIVE over SUSPENDED
        cust002_record = result_df.filter(result_df.customer_id == "CUST002").collect()[0]
        self.assertEqual(cust002_record["account_status"], "ACTIVE")
        self.assertEqual(str(cust002_record["last_updated"]), "2024-01-15 11:00:00")
        
        # Test CUST003: CRITICAL TEST - Premium $180 should beat Basic $220
        # This is the key test for multi-criteria logic
        cust003_record = result_df.filter(result_df.customer_id == "CUST003").collect()[0]
        self.assertEqual(cust003_record["transaction_amount"], 180.0)
        self.assertEqual(cust003_record["tier_level"], "Premium")
        self.assertEqual(cust003_record["account_status"], "ACTIVE")
        
        print("✅ Multi-criteria test: Premium $180 correctly chosen over Basic $220")
    
    def test_aggregate_duplicates(self):
        """Test aggregation approach instead of deduplication"""
        result_df = aggregate_duplicates(self.df)
        
        # Should have 5 aggregated customer records
        self.assertEqual(result_df.count(), 5)
        
        # Test CUST001 aggregation: 3 records ($50, $150, $75)
        cust001_agg = result_df.filter(result_df.customer_id == "CUST001").collect()[0]
        self.assertEqual(cust001_agg["max_transaction_amount"], 150.0)
        self.assertEqual(cust001_agg["total_transaction_amount"], 275.0)  # 50+150+75
        self.assertEqual(cust001_agg["transaction_count"], 3)
        
        # Test CUST002 aggregation: 3 records ($100 each)
        cust002_agg = result_df.filter(result_df.customer_id == "CUST002").collect()[0]
        self.assertEqual(cust002_agg["max_transaction_amount"], 100.0)
        self.assertEqual(cust002_agg["total_transaction_amount"], 300.0)  # 100+100+100
        self.assertEqual(cust002_agg["transaction_count"], 3)
        
        # Test CUST003 aggregation: 3 records ($200, $180, $220)
        cust003_agg = result_df.filter(result_df.customer_id == "CUST003").collect()[0]
        self.assertEqual(cust003_agg["max_transaction_amount"], 220.0)
        self.assertEqual(cust003_agg["total_transaction_amount"], 600.0)  # 200+180+220
        self.assertEqual(cust003_agg["transaction_count"], 3)
        
        # Test single record customers (CUST004, CUST005)
        cust004_agg = result_df.filter(result_df.customer_id == "CUST004").collect()[0]
        self.assertEqual(cust004_agg["transaction_count"], 1)
        self.assertEqual(cust004_agg["max_transaction_amount"], 300.0)
    
    def test_rank_based_deduplication(self):
        """Test rank vs row_number difference for handling ties"""
        result_df = rank_based_deduplication(self.df)
        
        # Should have more records than row_number() due to ties
        # CUST002 has three $100 records that all get rank=1
        self.assertGreaterEqual(result_df.count(), 5)
        
        # Test CUST002: all three $100 records should have rank=1
        cust002_records = result_df.filter(result_df.customer_id == "CUST002").collect()
        self.assertEqual(len(cust002_records), 3)  # All three ties kept
        
        for record in cust002_records:
            self.assertEqual(record["amount_rank"], 1)
            self.assertEqual(record["amount_dense_rank"], 1)
            self.assertEqual(record["transaction_amount"], 100.0)
        
        # Test single highest value customers still have one record
        cust001_records = result_df.filter(result_df.customer_id == "CUST001").collect()
        self.assertEqual(len(cust001_records), 1)
        self.assertEqual(cust001_records[0]["transaction_amount"], 150.0)
        
        print("✅ Rank-based test: Ties correctly handled with rank() vs row_number()")
    
    def test_edge_cases(self):
        """Test edge cases with empty data and single records"""
        # Test empty dataframe
        empty_df = self.spark.createDataFrame([], self.df.schema)
        
        highest_result = keep_highest_value(empty_df)
        self.assertEqual(highest_result.count(), 0)
        
        recent_result = keep_most_recent(empty_df)
        self.assertEqual(recent_result.count(), 0)
        
        best_result = keep_best_by_multiple_criteria(empty_df)
        self.assertEqual(best_result.count(), 0)
        
        agg_result = aggregate_duplicates(empty_df)
        self.assertEqual(agg_result.count(), 0)
        
        rank_result = rank_based_deduplication(empty_df)
        self.assertEqual(rank_result.count(), 0)
    
    def test_priority_hierarchy_comprehensive(self):
        """Comprehensive test of priority hierarchy logic"""
        from datetime import datetime
        # Create test case with clear priority distinctions
        test_data = [
            ("PRIORITY_TEST", "Test User", 50.0, datetime(2024, 1, 15, 8, 0, 0), "Basic", "SUSPENDED"),     # Lowest priority
            ("PRIORITY_TEST", "Test User", 100.0, datetime(2024, 1, 15, 9, 0, 0), "Standard", "SUSPENDED"), # Mid-low priority
            ("PRIORITY_TEST", "Test User", 200.0, datetime(2024, 1, 15, 10, 0, 0), "Standard", "ACTIVE"),    # Mid priority
            ("PRIORITY_TEST", "Test User", 75.0, datetime(2024, 1, 15, 11, 0, 0), "Premium", "SUSPENDED"),   # High tier, suspended
            ("PRIORITY_TEST", "Test User", 25.0, datetime(2024, 1, 15, 12, 0, 0), "Premium", "ACTIVE"),      # Highest priority (tier + status)
        ]
        
        test_df = self.spark.createDataFrame(test_data, self.df.schema)
        result = keep_best_by_multiple_criteria(test_df)
        
        # Should pick Premium/ACTIVE $25 despite being lowest amount
        record = result.collect()[0]
        self.assertEqual(record["tier_level"], "Premium")
        self.assertEqual(record["account_status"], "ACTIVE")
        self.assertEqual(record["transaction_amount"], 25.0)
        
        print("✅ Comprehensive priority test: Premium/ACTIVE $25 beats all higher amounts")

def run_tests():
    """Run all tests with detailed output"""
    print("🧪 Running Advanced Deduplication (Longer Version) Tests")
    print("=" * 60)
    
    unittest.main(argv=[''], verbosity=2, exit=False)

if __name__ == "__main__":
    run_tests() 