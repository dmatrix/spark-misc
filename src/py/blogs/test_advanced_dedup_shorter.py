import unittest
from pyspark.sql import SparkSession
import sys
import os

# Add current directory to path to import our module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from advanced_deduplication_shorter_snippet import (
    create_sample_data,
    keep_highest_value,
    keep_most_recent,
    multi_criteria_best,
    aggregate_duplicates
)

class TestAdvancedDeduplicationShorter(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests"""
        cls.spark = SparkSession.builder \
            .appName("TestAdvancedDeduplicationShorter") \
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
        self.df = create_sample_data(self.spark)
    
    def test_create_sample_data(self):
        """Test sample data creation"""
        df = create_sample_data(self.spark)
        
        # Test record count
        self.assertEqual(df.count(), 6)
        
        # Test schema
        expected_columns = ["customer_id", "name", "amount", "timestamp", "tier", "status"]
        self.assertEqual(df.columns, expected_columns)
        
        # Test specific duplicates exist
        cust001_count = df.filter(df.customer_id == "CUST001").count()
        self.assertEqual(cust001_count, 2)
        
        cust002_count = df.filter(df.customer_id == "CUST002").count()
        self.assertEqual(cust002_count, 2)
        
        cust003_count = df.filter(df.customer_id == "CUST003").count()
        self.assertEqual(cust003_count, 2)
    
    def test_keep_highest_value(self):
        """Test keeping record with highest amount per customer"""
        result_df = keep_highest_value(self.df)
        
        # Should have 3 unique customers
        self.assertEqual(result_df.count(), 3)
        
        # Verify each customer has only one record
        unique_customers = result_df.select("customer_id").distinct().count()
        self.assertEqual(unique_customers, 3)
        
        # Test specific highest values
        cust001_record = result_df.filter(result_df.customer_id == "CUST001").collect()[0]
        self.assertEqual(cust001_record["amount"], 150.0)
        self.assertEqual(cust001_record["tier"], "Premium")
        
        cust003_record = result_df.filter(result_df.customer_id == "CUST003").collect()[0]
        self.assertEqual(cust003_record["amount"], 220.0)
        self.assertEqual(cust003_record["tier"], "Basic")
    
    def test_keep_most_recent(self):
        """Test keeping most recent record per customer"""
        result_df = keep_most_recent(self.df)
        
        # Should have 3 unique customers
        self.assertEqual(result_df.count(), 3)
        
        # Test that all customers have ACTIVE status (most recent)
        active_count = result_df.filter(result_df.status == "ACTIVE").count()
        self.assertEqual(active_count, 3)
        
        # Verify CUST002 has most recent timestamp (11:00) with ACTIVE status
        cust002_record = result_df.filter(result_df.customer_id == "CUST002").collect()[0]
        self.assertEqual(str(cust002_record["timestamp"]), "2024-01-15 11:00:00")
        self.assertEqual(cust002_record["status"], "ACTIVE")
    
    def test_multi_criteria_best(self):
        """Test multi-criteria priority ranking - the most important test!"""
        result_df = multi_criteria_best(self.df)
        
        # Should have 3 unique customers
        self.assertEqual(result_df.count(), 3)
        
        # Test CUST001: Should keep Premium $150 over Basic $50
        cust001_record = result_df.filter(result_df.customer_id == "CUST001").collect()[0]
        self.assertEqual(cust001_record["amount"], 150.0)
        self.assertEqual(cust001_record["tier"], "Premium")
        
        # Test CUST002: Both have same tier/amount, should keep ACTIVE over SUSPENDED
        cust002_record = result_df.filter(result_df.customer_id == "CUST002").collect()[0]
        self.assertEqual(cust002_record["status"], "ACTIVE")
        
        # Test CUST003: KEY TEST - should keep Premium $180 over Basic $220
        # This proves tier priority beats amount priority
        cust003_record = result_df.filter(result_df.customer_id == "CUST003").collect()[0]
        self.assertEqual(cust003_record["amount"], 180.0)
        self.assertEqual(cust003_record["tier"], "Premium")
        self.assertEqual(cust003_record["status"], "ACTIVE")
        
        # This is the money test - Premium $180 beats Basic $220!
        print("✅ Multi-criteria test: Premium $180 correctly chosen over Basic $220")
    
    def test_aggregate_duplicates(self):
        """Test aggregation instead of deduplication"""
        result_df = aggregate_duplicates(self.df)
        
        # Should have 3 aggregated customer records
        self.assertEqual(result_df.count(), 3)
        
        # Test CUST001 aggregation
        cust001_agg = result_df.filter(result_df.customer_id == "CUST001").collect()[0]
        self.assertEqual(cust001_agg["max_amount"], 150.0)
        self.assertEqual(cust001_agg["total_amount"], 200.0)  # 50 + 150
        self.assertEqual(cust001_agg["record_count"], 2)
        
        # Test CUST003 aggregation
        cust003_agg = result_df.filter(result_df.customer_id == "CUST003").collect()[0]
        self.assertEqual(cust003_agg["max_amount"], 220.0)
        self.assertEqual(cust003_agg["total_amount"], 400.0)  # 220 + 180
        self.assertEqual(cust003_agg["record_count"], 2)
    
    def test_edge_case_empty_dataframe(self):
        """Test all methods handle empty dataframes"""
        empty_df = self.spark.createDataFrame([], self.df.schema)
        
        # All methods should handle empty input gracefully
        highest_result = keep_highest_value(empty_df)
        self.assertEqual(highest_result.count(), 0)
        
        recent_result = keep_most_recent(empty_df)
        self.assertEqual(recent_result.count(), 0)
        
        best_result = multi_criteria_best(empty_df)
        self.assertEqual(best_result.count(), 0)
        
        agg_result = aggregate_duplicates(empty_df)
        self.assertEqual(agg_result.count(), 0)
    
    def test_business_logic_priority_order(self):
        """Test that priority order is correctly implemented"""
        from datetime import datetime
        # Create specific test case for priority verification
        test_data = [
            ("TEST001", "Test User", 100.0, datetime(2024, 1, 15, 10, 0, 0), "Basic", "ACTIVE"),      # Low tier, active
            ("TEST001", "Test User", 200.0, datetime(2024, 1, 15, 11, 0, 0), "Standard", "SUSPENDED"), # Mid tier, suspended  
            ("TEST001", "Test User", 50.0, datetime(2024, 1, 15, 12, 0, 0), "Premium", "ACTIVE"),     # High tier, active, low amount
        ]
        
        test_df = self.spark.createDataFrame(test_data, self.df.schema)
        result = multi_criteria_best(test_df)
        
        # Should pick Premium/ACTIVE $50 over Standard/SUSPENDED $200 or Basic/ACTIVE $100
        record = result.collect()[0]
        self.assertEqual(record["tier"], "Premium")
        self.assertEqual(record["status"], "ACTIVE")
        self.assertEqual(record["amount"], 50.0)
        
        print("✅ Priority order test: Premium tier correctly beats higher amounts")

def run_tests():
    """Run all tests with detailed output"""
    print("🧪 Running Advanced Deduplication (Shorter Version) Tests")
    print("=" * 60)
    
    unittest.main(argv=[''], verbosity=2, exit=False)

if __name__ == "__main__":
    run_tests() 