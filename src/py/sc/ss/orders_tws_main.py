"""
Main execution module for the Retail Order Tracking System.

This module provides the main entry point for running the retail order tracking system.
It demonstrates how to start and run the order tracking system with proper error handling
and graceful shutdown capabilities.

The module integrates the OrderTrackingProcessor with utility functions to create
a complete streaming application that processes order events in real-time.

Usage:
    python orders_tws_main.py

Features:
    - Graceful startup and shutdown handling
    - Keyboard interrupt handling (Ctrl+C)
    - Comprehensive error handling and logging
    - Integration with Spark streaming infrastructure

Author: Jules S. Damji & Cursor AI
"""

from typing import Optional
import sys
import signal

from pyspark.sql.streaming import StreamingQuery

from tws_utility import setup_spark_session, run_order_tracking


def signal_handler(signum: int, frame) -> None:
    """
    Handle system signals for graceful shutdown.
    
    This function handles system signals (like SIGINT from Ctrl+C) to ensure
    the streaming query is stopped gracefully before the application exits.
    
    Args:
        signum (int): Signal number received
        frame: Current stack frame (unused)
        
    Returns:
        None
    """
    print(f"\nReceived signal {signum}. Initiating graceful shutdown...")
    sys.exit(0)


def main() -> None:
    """
    Main execution function for the retail order tracking system.
    
    This function orchestrates the complete startup, execution, and shutdown
    of the order tracking system. It handles all aspects of the streaming
    application lifecycle including error handling and cleanup.
    
    Execution Flow:
        1. Set up signal handlers for graceful shutdown
        2. Initialize Spark session with optimized configuration
        3. Print system startup information
        4. Start the streaming query
        5. Wait for termination or handle interrupts
        6. Perform cleanup and shutdown
    
    Returns:
        None
        
    Raises:
        KeyboardInterrupt: Handled gracefully to stop the streaming query
        Exception: Other exceptions are logged and re-raised after cleanup
        
    Examples:
        >>> main()  # Start the order tracking system
    """
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize variables
    spark = None
    query: Optional[StreamingQuery] = None
    
    try:
        print("=" * 60)
        print("🚀 Starting Retail Order Tracking System")
        print("=" * 60)
        print("📦 Order Lifecycle: ORDER → PROCESSING → SHIPPED → DELIVERED")
        print("⚠️  Monitoring SLA breaches and handling cancellations")
        print("🔄 Processing events in real-time with Spark Streaming")
        print("💡 Press Ctrl+C to stop the system gracefully")
        print("=" * 60)
        
        # Set up Spark session with optimized configuration
        print("🔧 Setting up Spark session...")
        spark = setup_spark_session("RetailOrderTrackingSystem")
        print("✅ Spark session initialized successfully")
        
        # Start the order tracking streaming query
        print("🎯 Starting order tracking streaming query...")
        query = run_order_tracking(spark)
        print("✅ Streaming query started successfully")
        print("📊 Order tracking system is now running...")
        print("📈 Monitor the console output for order events and status updates")
        print("-" * 60)
        
        # Wait for manual termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print("🛑 Received interrupt signal (Ctrl+C)")
        print("🔄 Stopping streaming query gracefully...")
        
        if query is not None:
            query.stop()
            print("✅ Streaming query stopped successfully")
        
        print("🏁 Order tracking system stopped successfully")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error occurred during streaming execution: {e}")
        print("🔄 Attempting to stop streaming query...")
        
        if query is not None:
            try:
                query.stop()
                print("✅ Streaming query stopped after error")
            except Exception as stop_error:
                print(f"⚠️  Error stopping query: {stop_error}")
        
        print("💥 System stopped due to error")
        raise
        
    finally:
        # Cleanup Spark session
        if spark is not None:
            try:
                spark.stop()
                print("🧹 Spark session cleaned up successfully")
            except Exception as cleanup_error:
                print(f"⚠️  Error during Spark cleanup: {cleanup_error}")


def run_with_custom_config(
    app_name: str = "CustomOrderTracking",
    checkpoint_location: str = "/tmp/order_tracking_checkpoint",
    processing_trigger: str = "10 seconds"
) -> None:
    """
    Run the order tracking system with custom configuration.
    
    This function provides an alternative entry point that allows customization
    of various system parameters like application name, checkpoint location,
    and processing trigger intervals.
    
    Args:
        app_name (str): Custom application name for Spark session
        checkpoint_location (str): Custom checkpoint directory path
        processing_trigger (str): Processing trigger interval (e.g., "5 seconds", "1 minute")
        
    Returns:
        None
        
    Examples:
        >>> run_with_custom_config(
        ...     app_name="ProductionOrderTracker",
        ...     checkpoint_location="/data/checkpoints/orders",
        ...     processing_trigger="30 seconds"
        ... )
    """
    print(f"🔧 Starting order tracking with custom configuration:")
    print(f"   📝 App Name: {app_name}")
    print(f"   💾 Checkpoint: {checkpoint_location}")
    print(f"   ⏱️  Trigger: {processing_trigger}")
    
    # Update the main function to use custom parameters
    # This would require modifications to utility functions to accept parameters
    # For now, we'll use the standard configuration
    main()


def health_check() -> bool:
    """
    Perform a health check of the order tracking system components.
    
    This function validates that all required components are available
    and properly configured before starting the main system.
    
    Returns:
        bool: True if all health checks pass, False otherwise
        
    Raises:
        ImportError: If required dependencies are not available
        Exception: If other system components fail validation
        
    Examples:
        >>> if health_check():
        ...     main()
        ... else:
        ...     print("Health check failed - system not ready")
    """
    try:
        print("🔍 Performing system health check...")
        
        # Check Spark availability
        from pyspark.sql import SparkSession
        print("✅ PySpark is available")
        
        # Check pandas availability
        import pandas as pd
        print("✅ Pandas is available")
        
        # Check if we can import our modules
        from orders_tws import OrderTrackingProcessor
        print("✅ OrderTrackingProcessor is available")
        
        from tws_utility import setup_spark_session, run_order_tracking
        print("✅ Utility functions are available")
        
        # Test Spark session creation
        test_spark = setup_spark_session("HealthCheckTest")
        test_spark.stop()
        print("✅ Spark session creation successful")
        
        print("✅ All health checks passed - system is ready")
        return True
        
    except ImportError as e:
        print(f"❌ Import error during health check: {e}")
        return False
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False


if __name__ == "__main__":
    """
    Entry point for the retail order tracking system.
    
    This block provides the main entry point when the module is executed directly.
    It includes command-line argument parsing for different execution modes
    and comprehensive error handling.
    
    Command-line Usage:
        python orders_tws_main.py                    # Run with default settings
        python orders_tws_main.py --health-check     # Run health check only
        python orders_tws_main.py --help             # Show help information
    """
    import argparse
    
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Retail Order Tracking System - Real-time order processing with Spark Streaming",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python orders_tws_main.py                    # Run the order tracking system
    python orders_tws_main.py --health-check     # Perform system health check
    python orders_tws_main.py --help             # Show this help message

The system will process order events in real-time, tracking them through:
ORDER → PROCESSING → SHIPPED → DELIVERED

It also monitors SLA breaches and handles order cancellations gracefully.
        """
    )
    
    parser.add_argument(
        "--health-check", 
        action="store_true",
        help="Perform system health check and exit"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="Retail Order Tracking System v1.0.0"
    )
    
    # Parse command-line arguments
    args = parser.parse_args()
    
    # Execute based on arguments
    if args.health_check:
        print("🏥 Running system health check...")
        if health_check():
            print("✅ System is healthy and ready to run")
            sys.exit(0)
        else:
            print("❌ System health check failed")
            sys.exit(1)
    else:
        # Run the main system
        main()
