#!/usr/bin/env python3
"""
Logical Data Pipelines (LDP) Examples

This module provides a command-line interface to run the LDP example pipelines:
- Oil Rigs: Industrial IoT sensor monitoring and analysis

Usage:
    python main.py --help
    python main.py oil-rigs
"""

import argparse
import sys
import subprocess
import os
from pathlib import Path


def run_oil_rigs_pipeline():
    """Run the Oil Rigs industrial monitoring pipeline."""
    print("🛢️  Running Oil Rigs Industrial Monitoring Pipeline...")
    print("=" * 50)
    
    oil_rigs_dir = Path("oil_rigs")
    if not oil_rigs_dir.exists():
        print(f"Error: {oil_rigs_dir} directory not found!")
        return 1
    
    try:
        # Change to oil_rigs directory and run pipeline
        os.chdir(oil_rigs_dir)
        
        print("1. Executing LDP pipeline...")
        try:
            subprocess.run(["./run_pipeline.sh"], check=True)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print("❌ ERROR: LDP pipeline command failed!")
            print("   Logical Data Pipelines CLI is missing files.")
            print("   Please check the LDP CLI before running this pipeline.")
            return 1
        
        print("\n2. Querying sensor data...")
        subprocess.run(["python", "query_oil_rigs_tables.py"], check=True)
        
        print("\n3. Generating temperature visualizations...")
        subprocess.run(["python", "plot_temperatures.py"], check=True)
        
        print("\n✅ Oil Rigs pipeline completed successfully!")
        return 0
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    finally:
        os.chdir("..")


def main():
    """Main entry point for LDP examples."""
    parser = argparse.ArgumentParser(
        description="Logical Data Pipelines (LDP) Examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py oil-rigs     # Run Oil Rigs sensor monitoring pipeline
  
Requirements:
  - Logical Data Pipelines CLI must be installed
  - Use 'logical-pipelines --help' to verify CLI availability
  
For more information, see LDP_README.md
        """
    )
    
    parser.add_argument(
        "pipeline",
        choices=["oil-rigs"],
        help="Pipeline to run"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="LDP Examples v0.1.0"
    )
    
    if len(sys.argv) == 1:
        parser.print_help()
        return 0
    
    args = parser.parse_args()
    
    print("🚀 Logical Data Pipelines (LDP) Examples")
    print("=" * 50)
    
    if args.pipeline == "oil-rigs":
        return run_oil_rigs_pipeline()
    else:
        print(f"❌ Unknown pipeline: {args.pipeline}")
        return 1


if __name__ == "__main__":
    sys.exit(main())