#!/usr/bin/env python3
"""
PySpark Window Functions Demo Runner - CLI Interface for All Window Function Examples

OVERVIEW:
This CLI tool provides a convenient interface to run individual Window function demos
by specifying use case labels. Each demo showcases different Window function patterns
with comprehensive real-world business scenarios and code examples.
All demos are built using Spark Connect.

KEY FEATURES:
• Simple command-line interface for quick demo execution
• Comprehensive help system with detailed descriptions
• Error handling and user-friendly feedback
• Individual demo execution with progress tracking
• List all available demos with business context
• Spark Connect architecture

AVAILABLE DEMOS:
• ranking - Performance rankings and bonus calculations using ROW_NUMBER, RANK, DENSE_RANK
• aggregation - Running totals, YTD tracking, and cumulative calculations
• lead_lag - Time series analysis and trend detection with LAG/LEAD functions
• moving_averages - Trend smoothing and performance monitoring with rolling windows
• percentile - Statistical analysis and salary distributions using NTILE, PERCENT_RANK
• first_last_value - Customer journey analysis and marketing attribution modeling

BENEFITS:
• Educational tool for learning Window functions with real examples
• Code snippets with Spark Connect for immediate implementation
• Examples with clear explanations and modern architecture
• Quick iteration and testing of different Window function patterns
• Professional development reference and training material
• Spark Connect configuration for Spark 4.0+ environments

Usage:
    python run_demo.py <use_case>
    python run_demo.py --help
    python run_demo.py --list

Examples:
    python run_demo.py ranking          # Run ranking operations demo
    python run_demo.py aggregation      # Run running aggregations demo
    python run_demo.py lead_lag          # Run time series analysis demo
    python run_demo.py --list           # Show all available demos
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path

# Demo mappings: label -> (filename, description)
DEMOS = {
    'ranking': (
        'ranking_operations_demo.py',
        'Ranking Operations: ROW_NUMBER, RANK, DENSE_RANK for performance analysis and bonus calculations (Spark Connect)'
    ),
    'aggregation': (
        'aggregation_window_demo.py',
        'Running Aggregations: Cumulative totals, YTD tracking, and contribution analysis (Spark Connect)'
    ),
    'lead_lag': (
        'lead_lag_demo.py',
        'Lead/Lag Operations: Time series analysis, trend detection, and trading signals (Spark Connect)'
    ),
    'moving_averages': (
        'moving_averages_demo.py',
        'Moving Averages: Trend smoothing, performance monitoring, and noise reduction (Spark Connect)'
    ),
    'percentile': (
        'percentile_analysis_demo.py',
        'Percentile Analysis: Salary distributions, benchmarking, and statistical analysis (Spark Connect)'
    ),
    'first_last_value': (
        'first_last_value_demo.py',
        'First/Last Value: Customer journey analysis, attribution modeling, and lifecycle tracking (Spark Connect)'
    )
}

def show_strategy_guide():
    """Display information about the Window Functions Strategy Selection Guide"""
    print("🎯 PySpark Window Functions: Strategy Selection Guide")
    print("=" * 70)
    print("\n📋 COMPREHENSIVE DECISION FRAMEWORK:")
    print("   This guide helps you choose the optimal window function approach")
    print("   for your specific business scenario with practical examples.")
    print("\n🚀 WHAT'S INCLUDED:")
    print("   • Decision framework with key questions to ask")
    print("   • Six detailed scenarios matching window function patterns")
    print("   • Strategy selection matrix for different business contexts")
    print("   • Performance optimization strategies for various data volumes")
    print("   • Implementation patterns and debugging tips")
    print("   • Advanced combinations for complex analytical needs")
    
    print(f"\n📁 LOCATION: WINDOW_FUNCTIONS_STRATEGY_GUIDE.md")
    print("   Located in the same directory as your demo files")
    
    print(f"\n💡 QUICK START:")
    print("   1. Read the Decision Framework section")
    print("   2. Find your scenario in the Quick Strategy Selector")
    print("   3. Run the recommended demo file to see it in action")
    print("   4. Adapt the pattern to your specific use case")
    
    print(f"\n🎯 EXAMPLE SCENARIOS:")
    print("   • 'I need to rank top performers' → ranking_operations_demo.py")
    print("   • 'I need running totals' → aggregation_window_demo.py")
    print("   • 'I need trend analysis' → lead_lag_demo.py")
    print("   • 'I need to smooth volatility' → moving_averages_demo.py")
    print("   • 'I need relative positions' → percentile_analysis_demo.py")
    print("   • 'I need customer journeys' → first_last_value_demo.py")
    
    # Try to open the file if possible
    strategy_file = "WINDOW_FUNCTIONS_STRATEGY_GUIDE.md"
    if Path(strategy_file).exists():
        print(f"\n✅ Strategy guide found: {strategy_file}")
        try:
            if sys.platform.startswith('darwin'):  # macOS
                subprocess.run(['open', strategy_file], check=True)
                print("   📖 Opening guide in your default markdown viewer...")
            elif sys.platform.startswith('linux'):  # Linux
                subprocess.run(['xdg-open', strategy_file], check=True)
                print("   📖 Opening guide in your default markdown viewer...")
            elif sys.platform.startswith('win'):  # Windows
                subprocess.run(['start', strategy_file], shell=True, check=True)
                print("   📖 Opening guide in your default markdown viewer...")
            else:
                print("   📖 Please open the file manually in your preferred markdown viewer")
        except:
            print("   📖 Please open the file manually in your preferred markdown viewer")
    else:
        print(f"\n❌ Strategy guide not found: {strategy_file}")
        print("   Please ensure the file is in the same directory as this script")

def list_demos():
    """Display all available demos with descriptions"""
    print("🎯 Available PySpark Window Function Demos (Spark Connect):")
    print("=" * 60)
    print("\n⚡ All demos use Spark Connect!")
    print("\n💡 NEW: Check out WINDOW_FUNCTIONS_STRATEGY_GUIDE.md for help choosing the right approach!")
    print("   This comprehensive guide helps you select the optimal window function strategy")
    print("   for your specific business scenario with decision frameworks and examples.")
    print("=" * 60)
    
    for label, (filename, description) in DEMOS.items():
        print(f"\n📊 {label.upper().replace('_', ' ')}")
        print(f"   Command: python run_demo.py {label}")
        print(f"   File: {filename}")
        print(f"   Description: {description}")
    
    print(f"\n💡 Total demos available: {len(DEMOS)}")
    print("\n🚀 Usage Examples:")
    print("   python run_demo.py ranking          # Run ranking operations demo")
    print("   python run_demo.py aggregation      # Run running aggregations demo")
    print("   python run_demo.py --list           # Show this list")

def run_demo(use_case):
    """Execute the specified demo"""
    if use_case not in DEMOS:
        print(f"❌ Error: Unknown use case '{use_case}'")
        print(f"\n✅ Available options: {', '.join(DEMOS.keys())}")
        print("\n🔍 Use 'python run_demo.py --list' to see all demos with descriptions")
        return False
    
    filename, description = DEMOS[use_case]
    
    # Check if file exists
    if not Path(filename).exists():
        print(f"❌ Error: Demo file '{filename}' not found")
        return False
    
    print(f"🚀 Running: {use_case.upper().replace('_', ' ')} Demo")
    print(f"📁 File: {filename}")
    print(f"📝 Description: {description}")
    print("=" * 80)
    
    try:
        # Execute the demo file
        result = subprocess.run([sys.executable, filename], 
                              cwd=os.getcwd(),
                              check=True)
        
        print("\n" + "=" * 80)
        print(f"✅ {use_case.upper().replace('_', ' ')} Demo completed successfully!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Demo failed with exit code {e.returncode}")
        return False
    except KeyboardInterrupt:
        print(f"\n⚠️ Demo interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description='PySpark Window Functions Demo Runner (Spark Connect)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available Demo Use Cases (All using Spark Connect):
  ranking          - Ranking operations (ROW_NUMBER, RANK, DENSE_RANK)
  aggregation      - Running totals and cumulative calculations  
  lead_lag         - Time series analysis with LAG/LEAD functions
  moving_averages  - Trend analysis with moving window calculations
  percentile       - Statistical analysis and salary distributions
  first_last_value - Customer journey and attribution analysis

Examples:
  python run_demo.py ranking
  python run_demo.py aggregation
  python run_demo.py --list
  python run_demo.py --strategy
        """
    )
    
    parser.add_argument(
        'use_case',
        nargs='?',
        choices=list(DEMOS.keys()),
        help='Window function use case to demonstrate'
    )
    
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all available demos with detailed descriptions'
    )
    
    parser.add_argument(
        '--strategy', '-s',
        action='store_true',
        help='Open the comprehensive Window Functions Strategy Selection Guide'
    )
    
    args = parser.parse_args()
    
    # Show strategy guide
    if args.strategy:
        show_strategy_guide()
        return
    
    # Show list of demos
    if args.list:
        list_demos()
        return
    
    # Show help if no use case provided
    if not args.use_case:
        parser.print_help()
        print(f"\n🔍 Use --list to see detailed descriptions of all {len(DEMOS)} available demos")
        print(f"📋 Use --strategy to access the comprehensive strategy selection guide")
        return
    
    # Run the specified demo
    success = run_demo(args.use_case)
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main() 