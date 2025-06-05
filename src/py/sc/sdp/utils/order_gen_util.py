from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import random
from datetime import datetime, timedelta
import uuid

def create_random_order_items() -> 'pyspark.sql.DataFrame':
    """
    Generates a DataFrame with random order items.
    
    Returns:
        pyspark.sql.DataFrame: DataFrame containing random order items.
    """
    # Initialize Spark session
    spark = SparkSession.active()

    # Define schema
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_item", StringType(), False),
        StructField("price", FloatType(), False),
        StructField("items_ordered", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("date_ordered", DateType(), False)
    ])

    # Possible order items (toys, sports, electronics, etc.)
    items = [
        "Toy Car", "Basketball", "Laptop", "Action Figure", "Tennis Racket",
        "Smartphone", "Board Game", "Football", "Headphones", "Drone",
        "Puzzle", "Tablet", "Skateboard", "Camera", "Video Game",
        "Scooter", "Smartwatch", "Baseball Bat", "VR Headset", "Electric Guitar"
    ]

    # Possible statuses
    statuses = ["approved", "fulfilled", "pending"]

    # Generate 100 random rows
    data = []
    for _ in range(100):
        order_id = str(uuid.uuid4())
        order_item = random.choice(items)
        price = round(random.uniform(10.0, 1000.0), 2)  # price between $10 and $1000
        items_ordered = random.randint(1, 10)
        status = random.choice(statuses)
        date_ordered = (datetime.now() - timedelta(days=random.randint(0, 30))).date()
        data.append((order_id, order_item, price, items_ordered, status, date_ordered))

    # Create DataFrame
    orders_df = spark.createDataFrame(data, schema)
    return orders_df

def main():
    # Create random order items DataFrame
    orders_df = create_random_order_items()
    
    # Show the DataFrame
    orders_df.show()

if __name__ == "__main__":
    main()
    # Uncomment the line below to run the main function when this script is executed
