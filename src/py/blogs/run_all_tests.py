#!/usr/bin/env python3
"""
Data Quality Strategies Test Runner
Runs all test suites and provides comprehensive reporting
"""

import subprocess
import sys
import time
from datetime import datetime

def run_test(test_file):
    """Run a single test file and capture results"""
    print(f'\n🧪 Running {test_file}')
    print('=' * 70)
    
    start_time = time.time()
    
    try:
        result = subprocess.run([sys.executable, test_file], 
                              capture_output=True, 
                              text=True, 
                              timeout=300)  # 5 minute timeout
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        if result.returncode == 0:
            print(f'✅ {test_file} PASSED ({execution_time:.2f}s)')
            return True, execution_time, None
        else:
            print(f'❌ {test_file} FAILED ({execution_time:.2f}s)')
            print(f'Error output:\n{result.stderr}')
            return False, execution_time, result.stderr
            
    except subprocess.TimeoutExpired:
        print(f'⏰ {test_file} TIMED OUT (>300s)')
        return False, 300, "Test timed out"
    except Exception as e:
        print(f'💥 {test_file} CRASHED: {str(e)}')
        return False, 0, str(e)

def main():
    """Main test runner"""
    print("🚀 PySpark 4.0 Data Quality Strategies - Test Suite Runner")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Define all test files
    tests = [
        ('Smart JSON Loading', 'test_all_json_loaders.py'),
        ('Business Logic Validation', 'test_business_logic_validation.py'),
        ('Basic Deduplication', 'test_deduplication_example.py'),
        ('Advanced Deduplication (Shorter)', 'test_advanced_dedup_shorter.py'),
        ('Advanced Deduplication (Longer)', 'test_advanced_dedup_longer.py')
    ]
    
    # Track results
    results = []
    total_start_time = time.time()
    
    # Run each test
    for test_name, test_file in tests:
        passed, exec_time, error = run_test(test_file)
        results.append({
            'name': test_name,
            'file': test_file,
            'passed': passed,
            'time': exec_time,
            'error': error
        })
    
    total_end_time = time.time()
    total_time = total_end_time - total_start_time
    
    # Generate summary report
    print('\n' + '='*70)
    print('📊 TEST SUMMARY REPORT')
    print('='*70)
    
    passed_tests = [r for r in results if r['passed']]
    failed_tests = [r for r in results if not r['passed']]
    
    print(f"Total Tests: {len(results)}")
    print(f"✅ Passed: {len(passed_tests)}")
    print(f"❌ Failed: {len(failed_tests)}")
    print(f"⏱️  Total Time: {total_time:.2f} seconds")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Detailed results
    print('\n📋 DETAILED RESULTS:')
    print('-' * 70)
    
    for result in results:
        status = "✅ PASS" if result['passed'] else "❌ FAIL"
        print(f"{status} {result['name']:<35} ({result['time']:.2f}s)")
        if not result['passed'] and result['error']:
            print(f"     Error: {result['error'][:100]}...")
    
    # Performance insights
    print('\n⚡ PERFORMANCE INSIGHTS:')
    print('-' * 70)
    
    if passed_tests:
        fastest = min(passed_tests, key=lambda x: x['time'])
        slowest = max(passed_tests, key=lambda x: x['time'])
        avg_time = sum(r['time'] for r in passed_tests) / len(passed_tests)
        
        print(f"Fastest: {fastest['name']} ({fastest['time']:.2f}s)")
        print(f"Slowest: {slowest['name']} ({slowest['time']:.2f}s)")
        print(f"Average: {avg_time:.2f}s per test suite")
    
    # Strategy recommendations
    print('\n💡 STRATEGY RECOMMENDATIONS:')
    print('-' * 70)
    
    strategy_advice = {
        'Smart JSON Loading': 'Use for mixed-quality data sources',
        'Business Logic Validation': 'Critical for domain-specific rules',
        'Basic Deduplication': 'Start here for simple duplicate removal',
        'Advanced Deduplication (Shorter)': 'Blog-friendly, developer-ready',
        'Advanced Deduplication (Longer)': 'Full-featured enterprise solution'
    }
    
    for result in results:
        if result['passed']:
            advice = strategy_advice.get(result['name'], 'General purpose')
            print(f"✅ {result['name']}: {advice}")
    
    # Exit code
    exit_code = 0 if len(failed_tests) == 0 else 1
    
    if exit_code == 0:
        print('\n🎉 ALL TESTS PASSED! Data quality strategies are developer-ready and tested.')
    else:
        print('\n⚠️  SOME TESTS FAILED. Review errors above before deploying.')
    
    print('\n📚 For usage examples and customization, see README.md')
    
    return exit_code

if __name__ == "__main__":
    sys.exit(main()) 