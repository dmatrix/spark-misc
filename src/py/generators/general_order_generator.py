#!/usr/bin/env python3
"""
General Purpose Order Generator Utility

A standalone utility that generates batches of orders in JSON format at regular intervals.
"""

import argparse
import sys
import logging
import importlib.util
import os

# Import reusable utilities
spec = importlib.util.spec_from_file_location("generator_utils", 
                                               os.path.join(os.path.dirname(__file__), "generator_utils.py"))
generator_utils = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator_utils)

# Import classes from the dynamically loaded module
BatchManager = generator_utils.BatchManager
ScheduledRunner = generator_utils.ScheduledRunner
setup_logging = generator_utils.setup_logging
validate_generator_args = generator_utils.validate_generator_args
OrderGenerator = generator_utils.OrderGenerator










def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate batches of orders in JSON format at regular intervals"
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=5,
        help='Interval between batch generations in minutes (default: 5)'
    )
    
    parser.add_argument(
        '--dir',
        type=str,
        default='order_batches',
        help='Output directory for batch files (default: order_batches)'
    )
    
    parser.add_argument(
        '--end',
        type=int,
        help='Total runtime in minutes (default: interval * 3). Must be evenly divisible by interval'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    setup_logging("order_generator.log")
    args = parse_arguments()
    
    # Validate arguments and get end time
    end_time = validate_generator_args(args.interval, args.end)
    
    try:
        # Create components
        order_generator = OrderGenerator()
        batch_manager = BatchManager(args.dir, "order_batch")
        
        # Create and run scheduler
        runner = ScheduledRunner(
            data_generator=order_generator,
            batch_manager=batch_manager,
            interval_minutes=args.interval,
            end_minutes=end_time,
            batch_size=100
        )
        runner.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()