"""
E-commerce Event Analytics with Spark 4.0 Variant Data Type
===========================================================

This use case demonstrates processing heterogeneous e-commerce user events
with different schemas using the Variant data type for flexible analytics.

Key Features:
- 3 event types: purchase (with nested payment data), search, wishlist
- Realistic user behavior patterns with temporal weighting
- CTE-optimized queries eliminating window function warnings
- Cross-event user behavior analysis
- Uses shared data_utility module

Performance Optimizations:
- CTE-based percentage calculations instead of window functions
- Maintains distributed processing across Spark partitions
- Optimized aggregation patterns for event data

Data Structure:
- Purchase events: total_amount, customer_type, nested payment info (method, processor, card_type)
- Search events: search_query, results_count, results_clicked
- Wishlist events: product_id, action, product_price

Demonstrates Variant's ability to handle mixed event schemas in a single table
while maintaining query performance and schema flexibility.

Authors: Jules S. Damji & Cursor AI
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, parse_json
from data_utility import generate_ecommerce_data

def create_spark_session() -> SparkSession:
    """Create Spark session with Variant support.
    
    Returns:
        SparkSession: Configured Spark session with adaptive query execution
            and partition coalescing enabled for optimal e-commerce analytics
    """
    return SparkSession.builder \
        .appName("E-commerce Event Analytics with Variant") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()



def run_ecommerce_analysis() -> None:
    """Run the e-commerce event analytics.
    
    Generates 75,000 e-commerce event records, converts JSON to Variant using DataFrame API,
    and performs comprehensive analytics including:
    - Event distribution overview with CTE-optimized percentage calculations
    - Purchase behavior analysis (customer types, payment methods, spending patterns)
    - Search behavior analysis (query patterns, result interactions)
    - Wishlist behavior analysis (product actions, price ranges)
    - Cross-event user behavior correlation
    
    All queries use CTE patterns to eliminate Spark window function warnings
    and maintain distributed processing across partitions.
    
    Returns:
        None
    """
    print("=" * 60)
    print("E-commerce Event Analytics with Spark 4.0 Variant")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Generate fake data
        start_time = time.time()
        fake_data = generate_ecommerce_data(75000)
        print(f"Data generation completed in {time.time() - start_time:.2f} seconds")
        
        # Create DataFrame
        print("\nCreating DataFrame...")
        df = spark.createDataFrame(fake_data)
        
        # Convert JSON strings to Variant data type using DataFrame API
        print("Converting JSON strings to Variant data type...")
        events_df = df.select(
            col('event_id'),
            col('user_id'),
            col('event_type'),
            col('timestamp').cast('timestamp'),
            # Using DataFrame API: parse_json() is available as a native function
            parse_json(col('event_data_json')).alias('event_data')
        )
        
        events_df.createOrReplaceTempView("user_events")
        print(f"Created events dataset with {events_df.count()} records")
        
        # Analysis 1: Event distribution and basic metrics
        print("\n" + "="*50)
        print("ANALYSIS 1: Event Distribution Overview")
        print("="*50)
        
        event_overview = spark.sql("""
            WITH event_totals AS (
                SELECT 
                    event_type,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT user_id) as unique_users
                FROM user_events
                GROUP BY event_type
            ),
            total_events AS (
                SELECT SUM(event_count) as total_count FROM event_totals
            )
            SELECT 
                et.event_type,
                et.event_count,
                et.unique_users,
                ROUND(et.event_count * 100.0 / te.total_count, 2) as percentage
            FROM event_totals et
            CROSS JOIN total_events te
            ORDER BY et.event_count DESC
        """)
        
        print("Event Distribution:")
        event_overview.show()
        
        # Analysis 2: Product and purchase analytics
        print("\n" + "="*50)
        print("ANALYSIS 2: Purchase Behavior Analytics")
        print("="*50)
        
        # Using SQL for VARIANT_GET: No native DataFrame API equivalent exists
        # This demonstrates mixed API usage - DataFrame for parse_json(), SQL for VARIANT_GET()
        purchase_analysis = spark.sql("""
            SELECT 
                VARIANT_GET(event_data, '$.customer_type', 'string') as customer_type,
                COUNT(*) as purchase_count,
                AVG(VARIANT_GET(event_data, '$.total_amount', 'double')) as avg_order_value
            FROM user_events 
            WHERE event_type = 'purchase'
            GROUP BY VARIANT_GET(event_data, '$.customer_type', 'string')
            ORDER BY avg_order_value DESC
        """)
        
        print("Purchase Analysis by Customer Type:")
        purchase_analysis.show(15, truncate=False)
        
        # Analysis 3: Event counts
        print("\n" + "="*50)
        print("ANALYSIS 3: Event Counts by Type")
        print("="*50)
        
        event_counts = spark.sql("""
            SELECT 
                event_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users
            FROM user_events
            GROUP BY event_type
            ORDER BY event_count DESC
        """)
        
        print("Event Counts by Type:")
        event_counts.show(truncate=False)
        
        # Analysis 4: Search behavior and effectiveness
        print("\n" + "="*50)
        print("ANALYSIS 4: Search Behavior Analysis")
        print("="*50)
        
        search_analysis = spark.sql("""
            SELECT 
                VARIANT_GET(event_data, '$.search_query', 'string') as search_query,
                COUNT(*) as search_count,
                AVG(VARIANT_GET(event_data, '$.results_count', 'int')) as avg_results
            FROM user_events 
            WHERE event_type = 'search'
            GROUP BY VARIANT_GET(event_data, '$.search_query', 'string')
            ORDER BY search_count DESC
            LIMIT 10
        """)
        
        print("Top Search Queries:")
        search_analysis.show(15, truncate=False)
        
        # Analysis 5: Wishlist behavior analysis
        print("\n" + "="*50)
        print("ANALYSIS 5: Wishlist Behavior Analysis")
        print("="*50)
        
        wishlist_analysis = spark.sql("""
            SELECT 
                VARIANT_GET(event_data, '$.action', 'string') as wishlist_action,
                COUNT(*) as action_count,
                AVG(VARIANT_GET(event_data, '$.product_price', 'double')) as avg_product_price
            FROM user_events 
            WHERE event_type = 'wishlist'
            GROUP BY VARIANT_GET(event_data, '$.action', 'string')
            ORDER BY action_count DESC
        """)
        
        print("Wishlist Actions and Prices:")
        wishlist_analysis.show(truncate=False)
        
        # Analysis 6: User behavior patterns
        print("\n" + "="*50)
        print("ANALYSIS 6: User Behavior Patterns")
        print("="*50)
        
        user_activity = spark.sql("""
            SELECT 
                user_id,
                COUNT(*) as total_events,
                SUM(CASE WHEN event_type = 'purchase' THEN VARIANT_GET(event_data, '$.total_amount', 'double') ELSE 0 END) as total_spent
            FROM user_events
            GROUP BY user_id
            ORDER BY total_spent DESC
            LIMIT 10
        """)
        
        print("Top Users by Spending:")
        user_activity.show(truncate=False)
        
        # Analysis 7: Payment method analysis (demonstrates nested structure)
        print("\n" + "="*50)
        print("ANALYSIS 7: Payment Method Analysis (Nested Data)")
        print("="*50)
        
        payment_analysis = spark.sql("""
            SELECT 
                VARIANT_GET(event_data, '$.payment.method', 'string') as payment_method,
                VARIANT_GET(event_data, '$.payment.processor', 'string') as payment_processor,
                VARIANT_GET(event_data, '$.payment.card_type', 'string') as card_type,
                COUNT(*) as transaction_count,
                AVG(VARIANT_GET(event_data, '$.total_amount', 'double')) as avg_transaction_amount
            FROM user_events 
            WHERE event_type = 'purchase'
            GROUP BY VARIANT_GET(event_data, '$.payment.method', 'string'), 
                     VARIANT_GET(event_data, '$.payment.processor', 'string'),
                     VARIANT_GET(event_data, '$.payment.card_type', 'string')
            ORDER BY transaction_count DESC
        """)
        
        print("Payment Methods with Nested Structure:")
        payment_analysis.show(truncate=False)
        
        # Performance demonstration
        print("\n" + "="*50)
        print("PERFORMANCE DEMONSTRATION")
        print("="*50)
        
        # Complex query using Variant (efficient)
        start_time = time.time()
        complex_query_result = spark.sql("""
            SELECT COUNT(*) as high_value_events
            FROM user_events 
            WHERE event_type = 'purchase' 
              AND VARIANT_GET(event_data, '$.total_amount', 'double') > 1000
        """).collect()[0]['high_value_events']
        variant_time = time.time() - start_time
        
        print(f"Variant query time: {variant_time:.3f} seconds")
        print(f"High-value purchases (>$1000): {complex_query_result}")
        
        # Show schema
        print("\n" + "="*50)
        print("DATASET SCHEMA")
        print("="*50)
        events_df.printSchema()
        
        print(f"\nE-commerce event analysis completed successfully!")
        print(f"Dataset size: {events_df.count()} records")
        print(f"Simplified event structure with key insights:")
        print(f"- Purchase events: total_amount, customer_type, payment (nested)")
        print(f"- Search events: search_query, results_count, results_clicked")
        print(f"- Wishlist events: product_id, action, product_price")
        print(f"- Nested payment structure demonstrates Variant flexibility")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_ecommerce_analysis()