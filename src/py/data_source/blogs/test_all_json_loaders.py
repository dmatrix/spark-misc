from pyspark.sql import SparkSession
import sys
import traceback

# Import all JSON loading approaches
from schema_enforcement import load_with_schema_enforcement
from permissive_mode import load_with_permissive_mode
from smart_json_loader import smart_json_load

def test_all_json_loaders():
    """
    Comprehensive test suite for all JSON loading strategies.
    Tests schema enforcement, permissive mode, and smart loader approaches.
    """
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TestAllJSONLoaders") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        print("🧪 COMPREHENSIVE JSON LOADING STRATEGY TESTS")
        print("=" * 80)
        print("Testing three approaches: FAILFAST, PERMISSIVE, and SMART HYBRID")
        
        # Test files with different data quality levels
        test_files = [
            "data/clean_transactions.json",     # 100% clean data
            "data/mixed_transactions.json",     # Mixed clean/corrupt data
            "data/invalid_transactions.json"    # Mostly corrupt data
        ]
        
        test_results = {}
        
        for test_file in test_files:
            print(f"\n{'='*80}")
            print(f"🎯 TESTING FILE: {test_file}")
            print(f"{'='*80}")
            
            file_results = {}
            
            # Test 1: Schema Enforcement (FAILFAST)
            print(f"\n📋 Test 1: Schema Enforcement (FAILFAST)")
            print("-" * 50)
            
            try:
                result = load_with_schema_enforcement(spark, test_file)
                if result is not None:
                    count = result.count()
                    file_results['schema_enforcement'] = {
                        'status': 'SUCCESS',
                        'records': count,
                        'message': f'Successfully loaded {count} records'
                    }
                    print(f"✅ SCHEMA ENFORCEMENT: SUCCESS - {count} records")
                else:
                    file_results['schema_enforcement'] = {
                        'status': 'FAILED',
                        'records': 0,
                        'message': 'FAILFAST mode failed due to schema violations'
                    }
                    print("❌ SCHEMA ENFORCEMENT: FAILED - Data contains violations")
            except Exception as e:
                file_results['schema_enforcement'] = {
                    'status': 'ERROR',
                    'records': 0,
                    'message': f'Error: {str(e)[:100]}...'
                }
                print(f"❌ SCHEMA ENFORCEMENT: ERROR - {str(e)[:100]}...")
            
            # Test 2: Permissive Mode
            print(f"\n📋 Test 2: Permissive Mode (Always Succeeds)")
            print("-" * 50)
            
            try:
                good_data, bad_data = load_with_permissive_mode(spark, test_file)
                good_count = good_data.count()
                bad_count = bad_data.count()
                total_count = good_count + bad_count
                success_rate = (good_count / total_count * 100) if total_count > 0 else 0
                
                file_results['permissive_mode'] = {
                    'status': 'SUCCESS',
                    'records': good_count,
                    'total_records': total_count,
                    'corrupt_records': bad_count,
                    'success_rate': success_rate,
                    'message': f'{good_count}/{total_count} good records ({success_rate:.1f}% success)'
                }
                print(f"✅ PERMISSIVE MODE: SUCCESS - {good_count}/{total_count} good ({success_rate:.1f}%)")
            except Exception as e:
                file_results['permissive_mode'] = {
                    'status': 'ERROR',
                    'records': 0,
                    'message': f'Error: {str(e)[:100]}...'
                }
                print(f"❌ PERMISSIVE MODE: ERROR - {str(e)[:100]}...")
            
            # Test 3: Smart JSON Loader (Hybrid)
            print(f"\n📋 Test 3: Smart JSON Loader (Hybrid Approach)")
            print("-" * 50)
            
            try:
                good_data, bad_data = smart_json_load(spark, test_file)
                
                if bad_data is None:
                    # FAILFAST succeeded
                    count = good_data.count()
                    file_results['smart_loader'] = {
                        'status': 'FAILFAST_SUCCESS',
                        'records': count,
                        'strategy': 'FAILFAST',
                        'message': f'FAILFAST succeeded - {count} clean records'
                    }
                    print(f"✅ SMART LOADER: FAILFAST SUCCESS - {count} records")
                else:
                    # Fell back to PERMISSIVE
                    good_count = good_data.count()
                    bad_count = bad_data.count()
                    total_count = good_count + bad_count
                    success_rate = (good_count / total_count * 100) if total_count > 0 else 0
                    
                    file_results['smart_loader'] = {
                        'status': 'PERMISSIVE_FALLBACK',
                        'records': good_count,
                        'total_records': total_count,
                        'corrupt_records': bad_count,
                        'success_rate': success_rate,
                        'strategy': 'PERMISSIVE',
                        'message': f'Fell back to PERMISSIVE - {good_count}/{total_count} good ({success_rate:.1f}%)'
                    }
                    print(f"✅ SMART LOADER: PERMISSIVE FALLBACK - {good_count}/{total_count} good ({success_rate:.1f}%)")
            except Exception as e:
                file_results['smart_loader'] = {
                    'status': 'ERROR',
                    'records': 0,
                    'message': f'Error: {str(e)[:100]}...'
                }
                print(f"❌ SMART LOADER: ERROR - {str(e)[:100]}...")
            
            test_results[test_file] = file_results
        
        # Generate comprehensive summary report
        print(f"\n{'='*80}")
        print("🎯 COMPREHENSIVE TEST RESULTS SUMMARY")
        print(f"{'='*80}")
        
        print("\n📊 Strategy Comparison Matrix:")
        print("-" * 100)
        print(f"{'File':<25} {'Schema Enforcement':<20} {'Permissive Mode':<20} {'Smart Loader':<25}")
        print("-" * 100)
        
        for file_path, results in test_results.items():
            filename = file_path.split('/')[-1]
            
            # Schema enforcement status
            schema_status = results['schema_enforcement']['status']
            schema_display = f"{schema_status}"
            if schema_status == 'SUCCESS':
                schema_display += f" ({results['schema_enforcement']['records']} rec)"
            
            # Permissive mode status
            perm_result = results['permissive_mode']
            if perm_result['status'] == 'SUCCESS':
                perm_display = f"SUCCESS ({perm_result['success_rate']:.1f}%)"
            else:
                perm_display = perm_result['status']
            
            # Smart loader status
            smart_result = results['smart_loader']
            if smart_result['status'] == 'FAILFAST_SUCCESS':
                smart_display = f"FAILFAST ({smart_result['records']} rec)"
            elif smart_result['status'] == 'PERMISSIVE_FALLBACK':
                smart_display = f"FALLBACK ({smart_result['success_rate']:.1f}%)"
            else:
                smart_display = smart_result['status']
            
            print(f"{filename:<25} {schema_display:<20} {perm_display:<20} {smart_display:<25}")
        
        # Analysis and recommendations
        print(f"\n💡 KEY INSIGHTS:")
        print("-" * 50)
        
        for file_path, results in test_results.items():
            filename = file_path.split('/')[-1]
            print(f"\n🔹 {filename}:")
            
            # Analyze schema enforcement
            schema_result = results['schema_enforcement']
            if schema_result['status'] == 'SUCCESS':
                print(f"   ✅ Schema Enforcement: Perfect for clean data - fast processing")
            else:
                print(f"   ❌ Schema Enforcement: Fails on any violation - not resilient")
            
            # Analyze permissive mode
            perm_result = results['permissive_mode']
            if perm_result['status'] == 'SUCCESS':
                success_rate = perm_result.get('success_rate', 0)
                if success_rate == 100:
                    print(f"   ✅ Permissive Mode: Perfect success rate - no data quality issues")
                elif success_rate >= 50:
                    print(f"   ⚠️  Permissive Mode: Partial success ({success_rate:.1f}%) - some recovery possible")
                else:
                    print(f"   ❌ Permissive Mode: Low success ({success_rate:.1f}%) - major data quality issues")
            
            # Analyze smart loader
            smart_result = results['smart_loader']
            if smart_result['status'] == 'FAILFAST_SUCCESS':
                print(f"   🚀 Smart Loader: Optimal path - used FAILFAST for maximum performance")
            elif smart_result['status'] == 'PERMISSIVE_FALLBACK':
                print(f"   🛡️  Smart Loader: Intelligent fallback - recovered {smart_result.get('success_rate', 0):.1f}% of data")
        
        print(f"\n🎯 RECOMMENDATIONS:")
        print("-" * 50)
        print("• Use SCHEMA ENFORCEMENT when: Data quality is guaranteed, performance is critical")
        print("• Use PERMISSIVE MODE when: Data quality is unknown, need maximum recovery")  
        print("• Use SMART LOADER when: Want best of both worlds - performance + resilience")
        print("• Monitor data quality trends using success rate metrics")
        
        # Final test summary
        total_tests = sum(len(results) for results in test_results.values())
        successful_tests = sum(
            1 for results in test_results.values() 
            for result in results.values() 
            if result['status'] in ['SUCCESS', 'FAILFAST_SUCCESS', 'PERMISSIVE_FALLBACK']
        )
        
        print(f"\n📈 OVERALL TEST RESULTS:")
        print("-" * 30)
        print(f"Tests executed: {total_tests}")
        print(f"Tests successful: {successful_tests}")
        print(f"Success rate: {(successful_tests/total_tests*100):.1f}%")
        
        if successful_tests == total_tests:
            print("\n🎉 ALL LOADING STRATEGIES WORKING PERFECTLY!")
        else:
            print(f"\n⚠️  {total_tests - successful_tests} test(s) had issues - check logs above")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    test_all_json_loaders() 