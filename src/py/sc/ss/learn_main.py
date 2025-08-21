"""
Ultra-Simple Learning Experience for transformWithState

Just one demo, minimal options, maximum learning.
Perfect for absolute beginners learning Spark Structured Streaming.

Features:
- RocksDB state store with JSON serialization
- Local checkpointing for fault tolerance
- Clean streaming infrastructure for learning
- Auto-advancing state transitions

Usage:
    python learn_main.py           # Run the demo

Author: Jules S. Damji & Cursor AI
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
    print("🎓 Welcome to transformWithState Learning!")
    print("   This is the simplest possible example with clean infrastructure.")
    print("   Watch how flight states persist between batches with RocksDB.\n")
    
    # Create Spark and run demo
    spark = create_spark()
    
    try:
        run_learning_demo(spark)
    finally:
        spark.stop()
        print("\n👋 Thanks for learning transformWithState!")


if __name__ == "__main__":
    main()
