#!/usr/bin/env python3
"""
Apache Spark 4.0 Variant Data Type Use Cases Runner
===================================================

This script runs three comprehensive use cases demonstrating the Variant data type
in Apache Spark 4.0 for handling semi-structured data:

1. Offshore Oil Rig Sensor Data Processing (iot)
2. E-commerce Event Analytics (ecommerce)
3. Security Log Analysis (security)

Each use case generates substantial fake data and performs realistic analytics
to showcase the flexibility and performance benefits of the Variant data type.

Requirements:
- Apache Spark 4.0 or compatible version with Variant support
- Python 3.8+
- PySpark with Variant data type support

Authors: Jules S. Damji & Cursor AI

Usage:
    python run_use_case.py [use_case_name]
    
    use_case_name: iot, ecommerce, security, or all (default)
    Use --help for detailed information
"""

import sys
import time
import importlib.util
import argparse
from datetime import datetime
from typing import Tuple, List, Dict, Any

def print_banner(title: str, width: int = 70) -> None:
    """Print a formatted banner.
    
    Args:
        title (str): Title text to display in the banner
        width (int): Width of the banner in characters (default: 70)
        
    Returns:
        None
    """
    print("\n" + "=" * width)
    print(f"{title:^{width}}")
    print("=" * width + "\n")

def print_section(title: str, width: int = 50) -> None:
    """Print a section divider.
    
    Args:
        title (str): Section title to display
        width (int): Width of the section divider in characters (default: 50)
        
    Returns:
        None
    """
    print("\n" + "-" * width)
    print(f" {title}")
    print("-" * width)

def check_dependencies() -> bool:
    """Check if required dependencies are available.
    
    Verifies that PySpark and required Spark SQL functions are properly installed
    and accessible.
    
    Returns:
        bool: True if all dependencies are satisfied, False otherwise
    """
    print_section("Checking Dependencies")
    
    try:
        import pyspark
        print(f"✓ PySpark version: {pyspark.__version__}")
    except ImportError:
        print("✗ PySpark not found. Please install PySpark 4.0+")
        return False
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import parse_json
        print("✓ Spark SQL functions available")
    except ImportError:
        print("✗ Required Spark SQL functions not available")
        return False
    
    print("✓ All dependencies satisfied")
    return True

def run_use_case_module(module_name: str, description: str) -> Tuple[bool, float]:
    """Run a specific use case module.
    
    Args:
        module_name (str): Name of the Python module to import and execute
        description (str): Human-readable description of the use case
        
    Returns:
        Tuple[bool, float]: Tuple containing:
            - bool: True if execution was successful, False otherwise
            - float: Execution time in seconds
    """
    print_banner(f"Running: {description}")
    
    start_time = time.time()
    
    try:
        # Import and run the module
        spec = importlib.util.spec_from_file_location(module_name, f"{module_name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Run the main analysis function
        if module_name == "iot_sensor_processing":
            # Special case for oil rig analysis
            if hasattr(module, "run_oil_rig_analysis"):
                module.run_oil_rig_analysis()
            else:
                print(f"Warning: No oil rig analysis function found in {module_name}")
        elif hasattr(module, f"run_{module_name.split('_')[0]}_analysis"):
            getattr(module, f"run_{module_name.split('_')[0]}_analysis")()
        else:
            print(f"Warning: No main analysis function found in {module_name}")
        
        execution_time = time.time() - start_time
        print(f"\n✓ Successfully completed {description}")
        print(f"  Execution time: {execution_time:.2f} seconds")
        return True, execution_time
        
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"\n✗ Error running {description}:")
        print(f"  {str(e)}")
        print(f"  Execution time: {execution_time:.2f} seconds")
        return False, execution_time

def show_use_case_info() -> None:
    """Display information about available use cases.
    
    Prints detailed information about all available use cases including
    descriptions, file names, dataset sizes, and key features.
    
    Returns:
        None
    """
    print_banner("Apache Spark 4.0 Variant Data Type Use Cases")
    
    use_cases = [
        {
            "name": "iot",
            "full_name": "Offshore Oil Rig Sensor Data Processing",
            "file": "iot_sensor_processing.py", 
            "description": "Processing 10 critical offshore oil rig sensors: pressure, flow, gas detection, temperature, vibration, position, weather, level, current, oil spill detection",
            "data_size": "50,000 records",
            "highlights": [
                "10 critical offshore oil rig sensors",
                "Pressure, flow, gas detection monitoring",
                "Safety systems with environmental monitoring",
                "Equipment health and predictive maintenance"
            ]
        },
        {
            "name": "ecommerce",
            "full_name": "E-commerce Event Analytics",
            "file": "ecommerce_event_analytics.py",
            "description": "Analyzing user behavior events across e-commerce platform with flexible event schemas",
            "data_size": "75,000 records",
            "highlights": [
                "Product views, cart additions, purchases, searches",
                "Customer segmentation and behavior analysis",
                "Shopping patterns and category performance",
                "User activity and spending analysis"
            ]
        },
        {
            "name": "security",
            "full_name": "Security Log Analysis",
            "file": "security_log_analysis.py",
            "description": "Processing security logs from multiple sources (Antivirus, Firewall, IDS, SIEM) for threat detection",
            "data_size": "60,000 records",
            "highlights": [
                "Multi-source security event correlation",
                "Geographic threat distribution analysis",
                "Network attack pattern detection",
                "User agent analysis (Chrome, Firefox, Safari)"
            ]
        }
    ]
    
    for case in use_cases:
        print(f"Use Case '{case['name']}': {case['full_name']}")
        print(f"  File: {case['file']}")
        print(f"  Description: {case['description']}")
        print(f"  Dataset Size: {case['data_size']}")
        print("  Key Features:")
        for highlight in case['highlights']:
            print(f"    • {highlight}")
        print()
    
    print("Benefits of Variant Data Type demonstrated:")
    print("  • No predefined schemas required")
    print("  • Improved performance vs JSON strings")
    print("  • Efficient nested field access")
    print("  • Flexible evolution of data structures")
    print("  • Direct SQL querying of semi-structured data")

def run_all_use_cases() -> None:
    """Run all use cases sequentially.
    
    Executes all available use cases in order and provides a comprehensive
    summary of execution results including timing and success/failure status.
    
    Returns:
        None
    """
    print_banner("Running All Use Cases")
    
    use_cases = [
        ("iot_sensor_processing", "Offshore Oil Rig Sensor Data Processing"),
        ("ecommerce_event_analytics", "E-commerce Event Analytics"),
        ("security_log_analysis", "Security Log Analysis"),

    ]
    
    results = []
    total_start_time = time.time()
    
    for i, (module_name, description) in enumerate(use_cases, 1):
        print(f"\n[{i}/4] Starting {description}...")
        success, exec_time = run_use_case_module(module_name, description)
        results.append((description, success, exec_time))
        
        if not success:
            print(f"\n⚠️  Use case {i} failed, but continuing with remaining cases...")
    
    total_time = time.time() - total_start_time
    
    # Print summary
    print_banner("Execution Summary")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    successful = 0
    for description, success, exec_time in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"{status:10} | {exec_time:8.2f}s | {description}")
        if success:
            successful += 1
    
    print(f"\nResults: {successful}/{len(results)} use cases completed successfully")
    
    if successful == len(results):
        print("\n🎉 All use cases completed successfully!")
        print("   The Variant data type has been demonstrated across multiple domains:")
        print("   • Offshore oil rig sensors with heterogeneous schemas")
        print("   • E-commerce events with complex nested structures")
        print("   • Security logs from various sources and formats")

    else:
        print(f"\n⚠️  {len(results) - successful} use case(s) encountered errors.")
        print("   Check the output above for details.")

def create_parser() -> argparse.ArgumentParser:
    """Create argument parser.
    
    Creates and configures an ArgumentParser with all available command-line
    options for running the use cases.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(
        description="Apache Spark 4.0 Variant Data Type Use Cases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_variant_usecase.py                    # Run all use cases
  python run_variant_usecase.py iot               # Run oil rig sensor processing
  python run_variant_usecase.py ecommerce         # Run e-commerce analytics  
  python run_variant_usecase.py security          # Run security log analysis
  
Use Cases:
  iot         Offshore Oil Rig Sensor Data Processing (50K records)
  ecommerce   E-commerce Event Analytics (75K records)
  security    Security Log Analysis (60K records)

  all         Run all use cases (default)
        """
    )
    
    parser.add_argument(
        'use_case',
        nargs='?',
        default='all',
        choices=['iot', 'ecommerce', 'security', 'all'],
        help='Use case to run: iot, ecommerce, security, or all (default: all)'
    )
    
    parser.add_argument(
        '--info',
        action='store_true',
        help='Show detailed information about all use cases'
    )
    
    return parser

def main() -> None:
    """Main function to run use cases.
    
    Parses command-line arguments, checks dependencies, and executes the
    requested use case(s). Handles all user interaction and error reporting.
    
    Returns:
        None
    """
    parser = create_parser()
    args = parser.parse_args()
    
    if args.info:
        show_use_case_info()
        return
    
    # Check dependencies first
    if not check_dependencies():
        print("\nPlease install required dependencies and try again.")
        sys.exit(1)
    
    print(f"Apache Spark 4.0 Variant Data Type Demonstration")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if args.use_case == 'iot':
        run_use_case_module("iot_sensor_processing", "Offshore Oil Rig Sensor Data Processing")
    elif args.use_case == 'ecommerce':
        run_use_case_module("ecommerce_event_analytics", "E-commerce Event Analytics")
    elif args.use_case == 'security':
        run_use_case_module("security_log_analysis", "Security Log Analysis")

    elif args.use_case == 'all':
        run_all_use_cases()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)