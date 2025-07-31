"""
E-commerce Event Analytics with Spark 4.0 Variant Data Type
===========================================================

This use case demonstrates processing heterogeneous e-commerce user events
with different schemas using the Variant data type for flexible analytics.
"""

import json
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

import time
import uuid

def create_spark_session():
    """Create Spark session with Variant support"""
    return SparkSession.builder \
        .appName("E-commerce Event Analytics with Variant") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

# Product catalogs for realistic data
CATEGORIES = ["electronics", "clothing", "books", "home", "sports", "toys", "beauty", "automotive"]
BRANDS = ["TechCorp", "StyleBrand", "ReadMore", "HomeComfort", "SportMax", "FunToys", "GlowBeauty", "AutoPro"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
SHIPPING_METHODS = ["standard", "express", "overnight", "pickup"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"]
STATES = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA"]
DISCOUNT_CODES = ["SAVE10", "WELCOME20", "FLASH15", "MEMBER5", "NEWUSER25", "LOYAL30"]



def generate_purchase_event(user_id, timestamp):
    """Generate purchase event data (simplified with nested payment)"""
    # Use timestamp for deterministic amount based on time of day (higher during peak hours)
    hour = timestamp.hour
    peak_multiplier = 1.5 if 10 <= hour <= 22 else 1.0  # Higher amounts during business hours
    base_amount = random.uniform(50.0, 1500.0) * peak_multiplier
    total_amount = round(base_amount, 2)
    
    # Use user_id for consistent customer type (same user tends to have same type)
    user_hash = hash(user_id) % 4
    customer_types = ["new", "returning", "vip", "premium"]
    
    return {
        "total_amount": total_amount,
        "customer_type": customer_types[user_hash],
        "payment": {
            "method": random.choice(PAYMENT_METHODS),
            "processor": random.choice(["stripe", "paypal", "square", "braintree"]),
            "card_type": random.choice(["visa", "mastercard", "amex", "discover"])
        }
    }

def generate_search_event(user_id, timestamp):
    """Generate search event data (simplified)"""
    search_terms = [
        "wireless headphones", "running shoes", "laptop", "coffee maker", "yoga mat",
        "smartphone case", "bluetooth speaker", "winter jacket", "desk chair", "water bottle"
    ]
    
    # Use user_id for consistent search behavior (same user tends to search similar terms)
    user_hash = hash(user_id) % len(search_terms)
    base_term = search_terms[user_hash]
    search_query = random.choice([base_term] * 3 + search_terms)  # 75% chance of user's preferred term
    
    # Use timestamp for realistic search patterns (more results during peak hours)
    hour = timestamp.hour
    peak_multiplier = 2.0 if 9 <= hour <= 17 else 1.0  # More results during business hours
    results_count = int(random.randint(50, 500) * peak_multiplier)
    
    return {
        "search_query": search_query,
        "results_count": results_count,
        "results_clicked": random.randint(0, min(10, results_count // 50))
    }

def generate_wishlist_event(user_id, timestamp):
    """Generate wishlist event data (simplified)"""
    # Use user_id for consistent product preferences (same user interacts with similar price ranges)
    user_hash = hash(user_id) % 3
    price_ranges = [(25.99, 150.0), (100.0, 400.0), (300.0, 799.99)]  # budget, mid, premium users
    min_price, max_price = price_ranges[user_hash]
    
    # Use timestamp for realistic action patterns (more 'add' during evenings)
    hour = timestamp.hour
    if 18 <= hour <= 23:  # Evening hours - more wishlist additions
        actions = ["add"] * 4 + ["remove", "move_to_cart"]
    elif 9 <= hour <= 17:  # Business hours - more conversions
        actions = ["add", "remove"] + ["move_to_cart"] * 3
    else:  # Off hours - more removals/cleanup
        actions = ["add", "move_to_cart"] + ["remove"] * 3
    
    return {
        "product_id": f"p{random.randint(1000, 9999)}",
        "action": random.choice(actions),
        "product_price": round(random.uniform(min_price, max_price), 2)
    }

def generate_fake_ecommerce_data(num_records=75000):
    """Generate large dataset of e-commerce events"""
    print(f"Generating {num_records} e-commerce event records...")
    
    event_types = [
        ("purchase", generate_purchase_event, 0.4),   # 40% of events
        ("search", generate_search_event, 0.35),      # 35% of events
        ("wishlist", generate_wishlist_event, 0.25)   # 25% of events
    ]
    
    # Create weighted list for realistic distribution
    weighted_events = []
    for event_type, generator, weight in event_types:
        weighted_events.extend([(event_type, generator)] * int(weight * 100))
    
    data = []
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    
    # Generate users for consistency
    users = [f"user_{i:06d}" for i in range(1, 10001)]  # 10,000 users
    
    for i in range(num_records):
        # Generate timestamp with realistic patterns (more activity during day)
        hour_weight = random.choices(
            range(24), 
            weights=[2, 1, 1, 1, 2, 3, 5, 8, 10, 12, 15, 16, 16, 15, 14, 16, 18, 16, 12, 8, 6, 4, 3, 2]
        )[0]
        
        timestamp = start_time + timedelta(
            days=random.randint(0, 30),
            hours=hour_weight,
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # Select event type and user
        event_type, generator_func = random.choice(weighted_events)
        user_id = random.choice(users)
        
        # Generate event data
        event_data = generator_func(user_id, timestamp)
        
        data.append({
            "event_id": f"evt_{uuid.uuid4().hex[:12]}",
            "user_id": user_id,
            "event_type": event_type,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "event_data_json": json.dumps(event_data)
        })
        
        if (i + 1) % 15000 == 0:
            print(f"Generated {i + 1} records...")
    
    return data

def run_ecommerce_analysis():
    """Run the e-commerce event analytics"""
    print("=" * 60)
    print("E-commerce Event Analytics with Spark 4.0 Variant")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Generate fake data
        start_time = time.time()
        fake_data = generate_fake_ecommerce_data(75000)
        print(f"Data generation completed in {time.time() - start_time:.2f} seconds")
        
        # Create DataFrame
        print("\nCreating DataFrame...")
        df = spark.createDataFrame(fake_data)
        
        # Create temp table and convert JSON to Variant
        df.createOrReplaceTempView("events_raw")
        
        print("Converting JSON strings to Variant data type...")
        events_df = spark.sql("""
            SELECT 
                event_id,
                user_id,
                event_type,
                CAST(timestamp AS TIMESTAMP) as timestamp,
                PARSE_JSON(event_data_json) as event_data
            FROM events_raw
        """)
        
        events_df.createOrReplaceTempView("user_events")
        print(f"Created events dataset with {events_df.count()} records")
        
        # Analysis 1: Event distribution and basic metrics
        print("\n" + "="*50)
        print("ANALYSIS 1: Event Distribution Overview")
        print("="*50)
        
        event_overview = spark.sql("""
            SELECT 
                event_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM user_events
            GROUP BY event_type
            ORDER BY event_count DESC
        """)
        
        print("Event Distribution:")
        event_overview.show()
        
        # Analysis 2: Product and purchase analytics
        print("\n" + "="*50)
        print("ANALYSIS 2: Purchase Behavior Analytics")
        print("="*50)
        
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