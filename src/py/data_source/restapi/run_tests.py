#!/usr/bin/env python3
"""
Test runner for REST API Data Source using PySpark Data Source API

This script runs comprehensive tests for the REST API Data Source that properly
uses the PySpark Data Source API introduced in Spark 4.0.
"""

import sys
import traceback


def run_all_tests():
    """Run all tests for the REST API Data Source"""
    print("🚀 Starting REST API Data Source Tests")
    print("=" * 80)
    print("This tests the proper PySpark Data Source API implementation:")
    print("- spark.dataSource.register(RestApiDataSource)")
    print("- spark.read.format('restapi').option(...).load()")
    print("- spark.write.format('restapi').option(...).save()")
    print("- SQL API with temporary views")
    print("- Schema inference")
    print("- Error handling")
    print("=" * 80)
    
    try:
        # Import and run the DataSource API tests
        from test_format_api import run_data_source_tests
        run_data_source_tests()
        
        print("\n" + "🎉" * 20)
        print("🎉 ALL TESTS PASSED! 🎉")
        print("🎉" * 20)
        print("\n✅ The REST API Data Source is working correctly!")
        print("✅ Users can now use:")
        print("   - spark.read.format('restapi').option('url', 'https://api.com').load()")
        print("   - spark.write.format('restapi').option('url', 'https://api.com').save()")
        print("   - SQL queries with temporary views")
        print("\n📝 Next Steps:")
        print("   1. Test with real APIs")
        print("   2. Add more authentication methods")
        print("   3. Add pagination support")
        print("   4. Add streaming support")
        
    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests() 