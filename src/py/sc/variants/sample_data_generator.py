"""
Sample Data Generator for Spark 4.0 Variant Use Cases
=====================================================

This module generates sample JSON data for three use cases:
1. Oil Rig IoT Sensor Processing (Comprehensive - All 10 sensors in single record)
2. E-commerce Event Analytics  
3. Security Log Analysis

Key Features:
- Comprehensive oil rig data: All 10 sensor types in single JSON record
- Realistic data patterns using shared data_utility module
- Configurable record counts per use case
- JSON-only output for Variant data type demonstrations
- Uses shared data generation utilities

Oil Rig Data Structure:
Generates comprehensive records containing all 10 sensor types:
- pressure, flow, gas, temperature, vibration, position, weather, level, current, spill_detection
- Each sensor type contains 3 key measurements
- Demonstrates Variant's nested data capabilities

Usage:
    python sample_data_generator.py                    # Generate 1 comprehensive record each
    python sample_data_generator.py --count 100        # Generate 100 comprehensive records each
    python sample_data_generator.py --oil-rig 50 --ecommerce 75 --security 100  # Custom counts
    python sample_data_generator.py --output-dir ./data  # Custom output directory
    python sample_data_generator.py --no-output --show-samples 5  # Display only, no files

Authors: Jules S. Damji & Cursor AI
"""

import json
import argparse
import os
from data_utility import (
    generate_comprehensive_oil_rig_data,
    generate_ecommerce_data, 
    generate_security_data
)

# ============================================================================
# WRAPPER FUNCTIONS FOR ADDING USE CASE METADATA
# ============================================================================

def generate_oil_rig_data_with_metadata(num_records=1):
    """Generate comprehensive oil rig sensor data with use case metadata"""
    data = generate_comprehensive_oil_rig_data(num_records)
    # Add use case metadata to each record
    for record in data:
        record["use_case"] = "oil_rig"
    return data

def generate_ecommerce_data_with_metadata(num_records=1):
    """Generate e-commerce event data with use case metadata"""
    data = generate_ecommerce_data(num_records)
    # Add use case metadata to each record
    for record in data:
        record["use_case"] = "ecommerce"
    return data

def generate_security_data_with_metadata(num_records=1):
    """Generate security log data with use case metadata"""
    data = generate_security_data(num_records)
    # Add use case metadata to each record
    for record in data:
        record["use_case"] = "security"
    return data

# ============================================================================
# OUTPUT FUNCTIONS
# ============================================================================

def save_as_json(data, filename):
    """Save data as JSON file"""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} records to {filename}")

def print_sample_data(data, use_case_name, num_samples=3):
    """Print sample data to console"""
    print(f"\n{use_case_name.upper()} SAMPLE DATA:")
    print("=" * 50)
    
    for i, record in enumerate(data[:num_samples]):
        print(f"Record {i+1}:")
        for key, value in record.items():
            if key.endswith('_json'):
                # Pretty print JSON data
                try:
                    parsed_json = json.loads(value)
                    print(f"  {key}: {json.dumps(parsed_json, indent=4)}")
                except:
                    print(f"  {key}: {value}")
            else:
                print(f"  {key}: {value}")
        print("-" * 30)

# ============================================================================
# MAIN FUNCTION AND CLI
# ============================================================================

def main():
    """Main function with command-line interface"""
    parser = argparse.ArgumentParser(
        description="Generate sample data for Spark 4.0 Variant use cases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sample_data_generator.py                           # Generate 1 record each
  python sample_data_generator.py --count 100               # Generate 100 records each
  python sample_data_generator.py --oil-rig 50 --ecommerce 75 --security 100
  python sample_data_generator.py --output-dir ./data
  python sample_data_generator.py --no-output --show-samples 5
        """
    )
    
    # Record count arguments
    parser.add_argument('--count', type=int, default=1,
                       help='Number of records to generate for each use case (default: 1)')
    parser.add_argument('--oil-rig', type=int, 
                       help='Number of oil rig sensor records (overrides --count)')
    parser.add_argument('--ecommerce', type=int,
                       help='Number of e-commerce event records (overrides --count)')
    parser.add_argument('--security', type=int,
                       help='Number of security log records (overrides --count)')
    
    # Output options
    parser.add_argument('--output-dir', default='.',
                       help='Output directory (default: current directory)')
    parser.add_argument('--no-output', action='store_true',
                       help='Do not save files, only show samples')
    parser.add_argument('--show-samples', type=int, default=3,
                       help='Number of sample records to display (default: 3)')
    
    args = parser.parse_args()
    
    # Determine record counts
    oil_rig_count = args.oil_rig if args.oil_rig is not None else args.count
    ecommerce_count = args.ecommerce if args.ecommerce is not None else args.count
    security_count = args.security if args.security is not None else args.count
    
    print("Sample Data Generator for Spark 4.0 Variant Use Cases")
    print("=" * 60)
    print(f"Oil Rig records: {oil_rig_count}")
    print(f"E-commerce records: {ecommerce_count}")
    print(f"Security records: {security_count}")
    print(f"Output format: JSON")
    print(f"Output directory: {args.output_dir}")
    print()
    
    # Create output directory if it doesn't exist
    if not args.no_output:
        os.makedirs(args.output_dir, exist_ok=True)
    
    # Generate data for each use case
    all_data = []
    
    # Oil Rig Data
    oil_rig_data = generate_oil_rig_data_with_metadata(oil_rig_count)
    all_data.extend(oil_rig_data)
    print_sample_data(oil_rig_data, "Oil Rig IoT Sensor", args.show_samples)
    
    if not args.no_output:
        filename = os.path.join(args.output_dir, "oil_rig_data.json")
        save_as_json(oil_rig_data, filename)
    
    # E-commerce Data
    ecommerce_data = generate_ecommerce_data_with_metadata(ecommerce_count)
    all_data.extend(ecommerce_data)
    print_sample_data(ecommerce_data, "E-commerce Events", args.show_samples)
    
    if not args.no_output:
        filename = os.path.join(args.output_dir, "ecommerce_data.json")
        save_as_json(ecommerce_data, filename)
    
    # Security Data
    security_data = generate_security_data_with_metadata(security_count)
    all_data.extend(security_data)
    print_sample_data(security_data, "Security Logs", args.show_samples)
    
    if not args.no_output:
        filename = os.path.join(args.output_dir, "security_data.json")
        save_as_json(security_data, filename)
    
    # Combined data file
    if not args.no_output:
        filename = os.path.join(args.output_dir, "combined_data.json")
        save_as_json(all_data, filename)
    
    print(f"\nGeneration completed!")
    print(f"Total records generated: {len(all_data)}")
    print(f"- Oil Rig: {len(oil_rig_data)} records")
    print(f"- E-commerce: {len(ecommerce_data)} records") 
    print(f"- Security: {len(security_data)} records")
    
    if not args.no_output:
        print(f"\nFiles saved to: {args.output_dir}")
        print(f"- oil_rig_data.json")
        print(f"- ecommerce_data.json")
        print(f"- security_data.json")
        print(f"- combined_data.json")

if __name__ == "__main__":
    main()
