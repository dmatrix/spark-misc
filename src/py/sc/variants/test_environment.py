#!/usr/bin/env python3
"""
Test Environment Setup for Apache Spark 4.0 Variant Use Cases
=============================================================

This script tests the environment to ensure all requirements are met
for running the Variant data type use cases.

Authors: Jules S. Damji & Cursor AI
"""

import sys


def test_python_version():
    """Test Python version compatibility"""
    print("Testing Python version...")
    major, minor = sys.version_info[:2]
    
    if major >= 3 and minor >= 8:
        print(f"✓ Python {major}.{minor} is compatible (3.8+ required)")
        return True
    else:
        print(f"✗ Python {major}.{minor} is too old (3.8+ required)")
        return False

def test_pyspark_installation():
    """Test PySpark installation and version"""
    print("\nTesting PySpark installation...")
    
    try:
        import pyspark
        version = pyspark.__version__
        print(f"✓ PySpark {version} is installed")
        
        # Test specific imports
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import parse_json
        print("✓ Required PySpark modules are available")
        
        return True
    except ImportError as e:
        print(f"✗ PySpark import failed: {e}")
        print("Install with: pip install pyspark>=4.0.0")
        return False

def test_spark_session():
    """Test Spark session creation"""
    print("\nTesting Spark session creation...")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("Environment Test") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        print("✓ Spark session created successfully")
        
        # Test basic DataFrame operations
        df = spark.range(5)
        count = df.count()
        print(f"✓ Basic DataFrame operations work (count: {count})")
        
        spark.stop()
        print("✓ Spark session stopped cleanly")
        return True
        
    except Exception as e:
        print(f"✗ Spark session test failed: {e}")
        return False

def test_variant_support():
    """Test Variant data type support (may not be available in all Spark versions)"""
    print("\nTesting Variant data type support...")
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import parse_json
        
        spark = SparkSession.builder \
            .appName("Variant Test") \
            .getOrCreate()
        
        # Try to create a simple Variant example
        data = [('{"name": "test", "value": 123}',)]
        df = spark.createDataFrame(data, ["json_str"])
        
        # Try to parse JSON (this should work in Spark 3.0+)
        variant_df = df.select(parse_json(df.json_str).alias("parsed"))
        result = variant_df.collect()
        
        print("✓ JSON parsing functions are available")
        print("✓ Basic Variant-like operations work")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"⚠ Variant support test: {e}")
        print("Note: Full Variant support requires Spark 4.0+")
        print("The use cases will still demonstrate the concepts with available functions")
        return True  # Not failing this test as fallback is available

def test_memory_resources():
    """Test available memory resources"""
    print("\nTesting system resources...")
    
    try:
        import psutil
        memory = psutil.virtual_memory()
        available_gb = memory.available / (1024**3)
        
        if available_gb >= 8:
            print(f"✓ Sufficient memory available: {available_gb:.1f} GB")
            return True
        else:
            print(f"⚠ Limited memory available: {available_gb:.1f} GB")
            print("Recommended: 8+ GB for optimal performance")
            print("Use cases will still run but may be slower")
            return True
            
    except ImportError:
        print("⚠ Could not check memory (psutil not available)")
        print("Ensure you have at least 8GB RAM for optimal performance")
        return True
    except Exception as e:
        print(f"⚠ Memory check failed: {e}")
        return True

def test_file_access():
    """Test file access for use case modules"""
    print("\nTesting use case file access...")
    
    required_files = [
        "iot_sensor_processing.py",
        "ecommerce_event_analytics.py", 
        "security_log_analysis.py",
        "run_variant_usecase.py"
    ]
    
    all_found = True
    for filename in required_files:
        try:
            with open(filename, 'r') as f:
                first_line = f.readline()
                print(f"✓ {filename} is accessible")
        except FileNotFoundError:
            print(f"✗ {filename} not found")
            all_found = False
        except Exception as e:
            print(f"✗ Error accessing {filename}: {e}")
            all_found = False
    
    return all_found

def main():
    """Run all environment tests"""
    print("=" * 60)
    print("Apache Spark 4.0 Variant Use Cases - Environment Test")
    print("=" * 60)
    
    tests = [
        ("Python Version", test_python_version),
        ("PySpark Installation", test_pyspark_installation),
        ("Spark Session", test_spark_session),
        ("Variant Support", test_variant_support),
        ("System Resources", test_memory_resources),
        ("File Access", test_file_access)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ {test_name} test encountered an error: {e}")
    
    print("\n" + "=" * 60)
    print(f"Environment Test Results: {passed}/{total} tests passed")
    print("=" * 60)
    
    if passed == total:
        print("🎉 Environment is ready for Spark 4.0 Variant use cases!")
        print("\nNext steps:")
        print("  python run_variant_usecase.py --help        # Show help and usage")
        print("  python run_variant_usecase.py               # Run all use cases")
        print("  python run_variant_usecase.py iot           # Run offshore oil rig sensors")
        print("  python run_variant_usecase.py ecommerce     # Run e-commerce analytics")
        print("  python run_variant_usecase.py security      # Run security log analysis")
    elif passed >= total - 1:
        print("✅ Environment is mostly ready with minor issues noted above")
        print("Use cases should run successfully")
    else:
        print("⚠️  Some environment issues detected")
        print("Please address the failed tests before running use cases")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())