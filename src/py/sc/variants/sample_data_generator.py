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
    python sample_data_generator.py                    # Generate complete sample sets (default: 1 each)
    python sample_data_generator.py --count 100        # Generate 100 records each (complete sets)
    python sample_data_generator.py --minimal          # Generate minimal sets (10 records each)
    python sample_data_generator.py --oil-rig 50 --ecommerce 75 --security 100  # Custom counts
    python sample_data_generator.py --show-samples 5   # Show 5 sample records per use case
    
All data is always saved to ./sample_data/ directory.

Authors: Jules S. Damji & Cursor AI
"""

import json
import argparse
import os
from typing import List, Dict, Any
from data_utility import (
    generate_comprehensive_oil_rig_data,
    generate_complete_ecommerce_data,
    generate_complete_security_data,
    generate_ecommerce_data, 
    generate_security_data
)

# ============================================================================
# WRAPPER FUNCTIONS FOR ADDING USE CASE METADATA
# ============================================================================

def generate_oil_rig_data_with_metadata(num_records: int = 1) -> List[Dict[str, str]]:
    """Generate comprehensive oil rig sensor data with use case metadata.
    
    Args:
        num_records (int): Number of comprehensive oil rig records to generate (default: 1)
        
    Returns:
        List[Dict[str, str]]: List of dictionaries containing oil rig sensor data with
            added 'use_case' metadata field set to 'oil_rig'
    """
    data = generate_comprehensive_oil_rig_data(num_records)
    # Add use case metadata to each record
    for record in data:
        record["use_case"] = "oil_rig"
    return data

def generate_ecommerce_data_with_metadata(num_records: int = 1) -> List[Dict[str, str]]:
    """Generate e-commerce event data with use case metadata.
    
    Always generates complete sets with all event types (purchase, search, wishlist).
    
    Args:
        num_records (int): Base number of records per event type (default: 1)
        
    Returns:
        List[Dict[str, str]]: List of dictionaries containing complete e-commerce event data
            with all event types represented and 'use_case' metadata field set to 'ecommerce'
    """
    # Always generate complete set with all event types (purchase, search, wishlist)
    data = generate_complete_ecommerce_data(num_records)
    
    # Add use case metadata to each record
    for record in data:
        record["use_case"] = "ecommerce"
    return data

def generate_security_data_with_metadata(num_records: int = 1) -> List[Dict[str, str]]:
    """Generate security log data with use case metadata.
    
    Always generates complete sets with all system types (firewall, antivirus, IDS).
    
    Args:
        num_records (int): Base number of records per system type (default: 1)
        
    Returns:
        List[Dict[str, str]]: List of dictionaries containing complete security log data
            with all system types represented and 'use_case' metadata field set to 'security'
    """
    # Always generate complete set with all system types (firewall, antivirus, IDS)
    data = generate_complete_security_data(num_records)
    
    # Add use case metadata to each record
    for record in data:
        record["use_case"] = "security"
    return data

# ============================================================================
# OUTPUT FUNCTIONS
# ============================================================================

def save_as_json(data: List[Dict[str, Any]], filename: str) -> None:
    """Save data as JSON file.
    
    Args:
        data (List[Dict[str, Any]]): List of dictionaries to save as JSON
        filename (str): Path to the output JSON file
        
    Returns:
        None
    """
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} records to {filename}")

def print_sample_data(data: List[Dict[str, Any]], use_case_name: str, num_samples: int = 3) -> None:
    """Print sample data to console.
    
    Args:
        data (List[Dict[str, Any]]): List of data records to display
        use_case_name (str): Name of the use case for display headers
        num_samples (int): Number of sample records to display (default: 3)
        
    Returns:
        None
    """
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

def main() -> None:
    """Main function with command-line interface.
    
    Parses command-line arguments and generates sample data for all three use cases:
    - Oil Rig IoT Sensor Processing (comprehensive records)
    - E-commerce Event Analytics
    - Security Log Analysis
    
    Supports various output options including custom record counts, output directory,
    and display-only mode.
    
    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description="Generate sample data for Spark 4.0 Variant use cases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sample_data_generator.py                           # Generate complete sets (1 per event type)
  python sample_data_generator.py --count 100               # Generate 100 per event type (complete sets)
  python sample_data_generator.py --minimal                 # Generate minimal sets (10 per event type)
  python sample_data_generator.py --oil-rig 50 --ecommerce 75 --security 100
  python sample_data_generator.py --show-samples 5          # Show 5 sample records per use case
        """
    )
    
    # Record count arguments
    parser.add_argument('--count', type=int, default=1,
                       help='Base number of records per event type for each use case (default: 1)')
    parser.add_argument('--minimal', action='store_true',
                       help='Generate minimal sample sets (10 records per event type)')
    parser.add_argument('--oil-rig', type=int, 
                       help='Number of oil rig sensor records (overrides --count and --minimal)')
    parser.add_argument('--ecommerce', type=int,
                       help='Base number per event type for e-commerce (overrides --count and --minimal)')
    parser.add_argument('--security', type=int,
                       help='Base number per system type for security (overrides --count and --minimal)')
    
    # Output options
    parser.add_argument('--show-samples', type=int, default=3,
                       help='Number of sample records to display (default: 3)')
    
    args = parser.parse_args()
    
    # Determine record counts
    base_count = 10 if args.minimal else args.count
    oil_rig_count = args.oil_rig if args.oil_rig is not None else base_count
    ecommerce_count = args.ecommerce if args.ecommerce is not None else base_count
    security_count = args.security if args.security is not None else base_count
    
    # Fixed output directory
    output_dir = "sample_data"
    
    print("Sample Data Generator for Spark 4.0 Variant Use Cases")
    print("=" * 60)
    print(f"Oil Rig records: {oil_rig_count}")
    print(f"E-commerce base records per event type: {ecommerce_count}")
    print(f"Security base records per system type: {security_count}")
    print(f"Event generation: Complete sets (all event types)")
    print(f"Output format: JSON")
    print(f"Output directory: {output_dir}")
    print()
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate data for each use case
    
    # Oil Rig Data
    oil_rig_data = generate_oil_rig_data_with_metadata(oil_rig_count)
    print_sample_data(oil_rig_data, "Oil Rig IoT Sensor", args.show_samples)
    
    filename = os.path.join(output_dir, "oil_rig_data.json")
    save_as_json(oil_rig_data, filename)
    
    # E-commerce Data
    ecommerce_data = generate_ecommerce_data_with_metadata(ecommerce_count)
    print_sample_data(ecommerce_data, "E-commerce Events", args.show_samples)
    
    filename = os.path.join(output_dir, "ecommerce_data.json")
    save_as_json(ecommerce_data, filename)
    
    # Security Data
    security_data = generate_security_data_with_metadata(security_count)
    print_sample_data(security_data, "Security Logs", args.show_samples)
    
    filename = os.path.join(output_dir, "security_data.json")
    save_as_json(security_data, filename)
    

    
    print(f"\nGeneration completed!")
    total_records = len(oil_rig_data) + len(ecommerce_data) + len(security_data)
    print(f"Total records generated: {total_records}")
    print(f"- Oil Rig: {len(oil_rig_data)} records")
    print(f"- E-commerce: {len(ecommerce_data)} records") 
    print(f"- Security: {len(security_data)} records")
    
    print(f"\nFiles saved to: {output_dir}")
    print(f"- oil_rig_data.json")
    print(f"- ecommerce_data.json")
    print(f"- security_data.json")

if __name__ == "__main__":
    main()
