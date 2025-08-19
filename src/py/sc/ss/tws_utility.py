"""
Utility functions for the Retail Order Tracking System.

This module contains utility functions for creating order streams, running streaming queries,
and managing the order tracking system. These functions support both demonstration/testing
scenarios and production-ready implementations.

The utilities include:
- Sample order stream generation for testing
- Realistic order stream creation with proper sequencing
- Streaming query execution and management
- Integration examples for real-world data sources

Author: Jules S. Damji
"""

from typing import List, Tuple, Any
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    IntegerType, DoubleType, BooleanType
)

# Import OrderTrackingProcessor when needed to avoid circular imports


def create_sample_order_stream(spark: SparkSession) -> DataFrame:
    """
    Create a sample order event stream using Spark's rate source for demonstration.
    
    This function generates a synthetic stream of order events by transforming
    Spark's built-in rate source into realistic order events. The generated
    events include various order types, items, and event types for testing
    the order tracking system.
    
    Args:
        spark (SparkSession): Active Spark session for creating the streaming DataFrame
    
    Returns:
        DataFrame: A Spark streaming DataFrame containing order events with schema:
            - order_id (str): Unique order identifier (ORD-XXXXXX format)
            - customer_id (str): Customer identifier (CUST-XX format)
            - event_type (str): Type of event (ORDER_PLACED, PROCESSING_STARTED, etc.)
            - item_name (str): Name of the ordered item
            - item_category (str): Category of the item (Pants, Shirts, Shoes, etc.)
            - quantity (int): Number of items ordered
            - price (float): Price of the item
            - timestamp (timestamp): Event timestamp
    
    Note:
        This function uses Spark's rate source which generates 2 rows per second.
        The generated data follows patterns based on the value field to create
        realistic order sequences and variety in products.
        
    Examples:
        >>> order_stream = create_sample_order_stream(spark)
        >>> # Use in streaming query for testing
    """
    # Define input schema for order events
    input_schema: StructType = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_type", StringType(), True),  # ORDER_PLACED, PROCESSING_STARTED, ITEM_SHIPPED, ITEM_DELIVERED, ORDER_CANCELLED
        StructField("item_name", StringType(), True),
        StructField("item_category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    # For demonstration, create a rate source and transform it to order events
    df: DataFrame = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 2) \
        .load()
    
    # Transform rate stream to order events
    order_events: DataFrame = df.selectExpr(
        "CONCAT('ORD-', LPAD(CAST(value AS STRING), 6, '0')) as order_id",
        "CONCAT('CUST-', CAST((value % 100) AS STRING)) as customer_id",
        """CASE 
            WHEN value % 10 = 0 THEN 'ORDER_PLACED'
            WHEN value % 10 = 2 THEN 'PROCESSING_STARTED' 
            WHEN value % 10 = 4 THEN 'ITEM_SHIPPED'
            WHEN value % 10 = 6 THEN 'ITEM_DELIVERED'
            WHEN value % 10 = 8 THEN 'ORDER_CANCELLED'
            ELSE 'ORDER_PLACED'
        END as event_type""",
        """CASE (value % 5)
            WHEN 0 THEN 'Blue Jeans'
            WHEN 1 THEN 'Cotton T-Shirt' 
            WHEN 2 THEN 'Running Shoes'
            WHEN 3 THEN 'Winter Jacket'
            ELSE 'Summer Dress'
        END as item_name""",
        """CASE (value % 5)
            WHEN 0 THEN 'Pants'
            WHEN 1 THEN 'Shirts'
            WHEN 2 THEN 'Shoes' 
            WHEN 3 THEN 'Jackets'
            ELSE 'Dresses'
        END as item_category""",
        "CAST((value % 3) + 1 AS INT) as quantity",
        "CAST(((value % 10) + 1) * 25.99 AS DOUBLE) as price",
        "timestamp"
    )
    
    return order_events


def run_order_tracking(spark: SparkSession) -> StreamingQuery:
    """
    Run the order tracking streaming query with the OrderTrackingProcessor.
    
    This function creates and starts a Spark Structured Streaming query that
    processes order events using the stateful OrderTrackingProcessor. It sets up
    the complete pipeline from input stream to console output with proper
    configuration for real-time processing.
    
    Args:
        spark (SparkSession): Active Spark session for creating the streaming query
    
    Returns:
        StreamingQuery: The running Spark streaming query object that can be
            used to monitor, stop, or wait for termination of the stream processing.
    
    Raises:
        Exception: May raise various Spark-related exceptions during query setup
            or execution, such as configuration errors or resource issues.
    
    Note:
        The query is configured with:
        - Update output mode for stateful processing
        - Console sink for debugging and monitoring
        - 10-second processing trigger interval
        - Checkpointing enabled for fault tolerance
        
    Examples:
        >>> query = run_order_tracking(spark)
        >>> query.awaitTermination()  # Wait for manual termination
        >>> # Or
        >>> query.stop()  # Stop the query programmatically
    """
    # Define output schema for processed events
    output_schema: StructType = StructType([
        StructField("order_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("message", StringType(), True),
        StructField("sla_breach", BooleanType(), True)
    ])
    
    # Create order event stream
    order_stream: DataFrame = create_sample_order_stream(spark)
    
    # Import OrderTrackingProcessor locally to avoid circular imports
    from orders_tws import OrderTrackingProcessor
    
    # Apply order tracking processor with transformWithStateInPandas
    tracking_query: StreamingQuery = order_stream \
        .groupBy("order_id") \
        .transformWithStateInPandas(
            statefulProcessor=OrderTrackingProcessor(),
            outputStructType=output_schema,
            outputMode="Update",
            timeMode="ProcessingTime"
        ) \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    return tracking_query


def create_realistic_order_stream(spark: SparkSession) -> DataFrame:
    """
    Create a more realistic order stream with proper event sequencing.
    
    This function demonstrates how to create a more realistic order event stream
    with proper chronological sequencing of events. It includes examples of
    complete order flows, cancellations, and scenarios that would trigger SLA
    breaches for comprehensive testing.
    
    Args:
        spark (SparkSession): Active Spark session for creating the streaming DataFrame
    
    Returns:
        DataFrame: A Spark streaming DataFrame with realistic order event sequences.
            For demonstration purposes, this currently returns the sample stream,
            but shows how to structure realistic order data.
    
    Note:
        In a production environment, this would typically read from:
        - Apache Kafka topics
        - Database change streams
        - File-based sources with proper ordering
        - Message queues with order event data
        
        The function includes commented examples showing how to integrate
        with Kafka and other real-world data sources.
        
    Order Scenarios Demonstrated:
        1. Complete order flow: ORDER -> PROCESSING -> SHIPPED -> DELIVERED
        2. Cancelled orders: ORDER -> PROCESSING -> CANCELLED
        3. SLA breach scenarios: ORDER with no subsequent processing events
        
    Examples:
        >>> realistic_stream = create_realistic_order_stream(spark)
        >>> # Use in production streaming pipeline
    """
    # This would typically come from Kafka, files, or a database
    # For demo purposes, we'll simulate realistic order flow
    
    sample_orders: List[Tuple[str, str, str, str, str, int, float, str]] = [
        # Order 1: Complete flow
        ("ORD-001", "CUST-101", "ORDER_PLACED", "Blue Jeans", "Pants", 2, 79.99, "2024-01-01 10:00:00"),
        ("ORD-001", "CUST-101", "PROCESSING_STARTED", "Blue Jeans", "Pants", 2, 79.99, "2024-01-01 14:00:00"),
        ("ORD-001", "CUST-101", "ITEM_SHIPPED", "Blue Jeans", "Pants", 2, 79.99, "2024-01-02 09:00:00"),
        ("ORD-001", "CUST-101", "ITEM_DELIVERED", "Blue Jeans", "Pants", 2, 79.99, "2024-01-04 16:00:00"),
        
        # Order 2: Cancelled after processing
        ("ORD-002", "CUST-102", "ORDER_PLACED", "Summer Dress", "Dresses", 1, 129.99, "2024-01-01 11:00:00"),
        ("ORD-002", "CUST-102", "PROCESSING_STARTED", "Summer Dress", "Dresses", 1, 129.99, "2024-01-01 15:30:00"),
        ("ORD-002", "CUST-102", "ORDER_CANCELLED", "Summer Dress", "Dresses", 1, 129.99, "2024-01-02 10:00:00"),
        
        # Order 3: Will trigger SLA breach (processing takes too long)
        ("ORD-003", "CUST-103", "ORDER_PLACED", "Running Shoes", "Shoes", 1, 159.99, "2024-01-01 12:00:00"),
        # No processing event - will trigger SLA breach
    ]
    
    # In a real scenario, you'd read from Kafka like this:
    # kafka_stream = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "order-events") \
    #     .load() \
    #     .select(
    #         col("key").cast("string").alias("order_id"),
    #         from_json(col("value").cast("string"), input_schema).alias("data")
    #     ).select("data.*")
    
    # For demonstration, return the sample stream
    return create_sample_order_stream(spark)


def get_order_input_schema() -> StructType:
    """
    Get the standard input schema for order events.
    
    This function returns the standardized schema used for order event streams
    throughout the system. It ensures consistency across different data sources
    and processing components.
    
    Returns:
        StructType: Spark SQL schema for order events containing:
            - order_id (str): Unique order identifier
            - customer_id (str): Customer identifier
            - event_type (str): Type of order event
            - item_name (str): Name of the ordered item
            - item_category (str): Category/type of the item
            - quantity (int): Number of items ordered
            - price (float): Price per item
            - timestamp (timestamp): Event occurrence timestamp
    
    Examples:
        >>> schema = get_order_input_schema()
        >>> df = spark.readStream.schema(schema).json("path/to/json/files")
    """
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("item_category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])


def get_output_schema() -> StructType:
    """
    Get the standard output schema for processed order events.
    
    This function returns the standardized schema used for output events
    from the OrderTrackingProcessor. It defines the structure of events
    emitted by the stateful processor.
    
    Returns:
        StructType: Spark SQL schema for processed order events containing:
            - order_id (str): Unique order identifier
            - event_type (str): Type of processed event
            - status (str): Current order status
            - message (str): Human-readable status message
            - sla_breach (bool): Whether an SLA breach occurred
    
    Examples:
        >>> schema = get_output_schema()
        >>> # Use in transformWithStateInPandas outputStructType parameter
    """
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("message", StringType(), True),
        StructField("sla_breach", BooleanType(), True)
    ])


# Future Extension: Kafka Integration
# 
# def create_kafka_order_stream(
#     spark: SparkSession, 
#     bootstrap_servers: str = "localhost:9092",
#     topic: str = "order-events"
# ) -> DataFrame:
#     """
#     Create an order stream from Apache Kafka for production use.
#     
#     This function would create a streaming DataFrame that reads order events from
#     a Kafka topic. It's designed for production environments where order events
#     are published to Kafka by upstream systems.
#     
#     Note: This is a future extension and not currently implemented.
#           See README.md for implementation examples.
#     """
#     # Implementation would go here
#     pass


def setup_spark_session(app_name: str = "RetailOrderTracking") -> SparkSession:
    """
    Set up an optimized Spark session for order tracking streaming.
    
    This function creates a Spark session with optimized configuration
    for streaming workloads, including checkpointing, adaptive query
    execution, and other performance optimizations.
    
    Args:
        app_name (str): Name for the Spark application (default: "RetailOrderTracking")
    
    Returns:
        SparkSession: Configured Spark session optimized for streaming
    
    Note:
        The session is configured with:
        - Adaptive query execution enabled
        - Checkpointing directory set to /tmp/checkpoint
        - Optimized settings for streaming workloads
        
    Examples:
        >>> spark = setup_spark_session("MyOrderTracker")
        >>> # Use for streaming operations
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .getOrCreate()
