"""
Ultra-Simple Learning Experience for transformWithState on Databricks

Just one demo, minimal options, maximum learning.
Perfect for absolute beginners on Databricks cloud environment.

Features:
- RocksDB state store with multi-column family support
- DBFS checkpointing for fault tolerance
- Production-grade streaming infrastructure
- Auto-scaling clusters

Usage:
    python learn_main.py           # Run the demo on Databricks
    python learn_main.py --explain # Just explain concepts

Author: Jules S. Damji
"""

import argparse
from typing import Optional
from learn_util import create_spark, run_learning_demo, explain_basics


def main() -> None:
    """
    Simple main function - no complex menus or options.
    
    Args:
        None
        
    Returns:
        None
    """
    
    parser = argparse.ArgumentParser(description="Learn transformWithState the simple way")
    parser.add_argument("--explain", action="store_true", help="Just explain the concepts")
    args = parser.parse_args()
    
    # Just explain concepts
    if args.explain:
        explain_basics()
        return
    
    # Run the learning demo
    print("🎓 Welcome to transformWithState Learning on Databricks!")
    print("   This is the simplest possible example with production infrastructure.")
    print("   Watch how flight states persist between batches with RocksDB.\n")
    
    # Create Spark and run demo
    spark = create_spark()
    
    try:
        run_learning_demo(spark)
    finally:
        spark.stop()
        print("\n👋 Thanks for learning transformWithState on Databricks!")


if __name__ == "__main__":
    main()
