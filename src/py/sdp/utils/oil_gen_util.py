from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid

# Define oil rig configurations
OIL_RIGS = {
    'permian_rig': {
        'location': 'Midland, Texas',
        'region': 'Permian Basin',
        'lat': 31.9973,
        'lon': -102.0779
    },
    'eagle_ford_rig': {
        'location': 'Karnes City, Texas',
        'region': 'Eagle Ford Shale',
        'lat': 28.8851,
        'lon': -97.9006
    }
}

def generate_sensor_data(rig_name: str, start_date: datetime, num_events: int = 100) -> list:
    """
    Generates sensor data for oil rig monitoring.
    
    Args:
        rig_name (str): Name of the oil rig (must be a key in OIL_RIGS)
        start_date (datetime): Starting timestamp for the sensor data
        num_events (int): Number of events to generate. Defaults to 100.
    
    Returns:
        list: List of dictionaries containing sensor data
    """
    data = []
    
    # Sensor ranges
    SENSOR_RANGES = {
        'temperature': (150, 350),  # Fahrenheit
        'pressure': (2000, 5000),   # PSI
        'water_level': (100, 500)   # Feet
    }
    
    current_time = start_date
    
    for _ in range(num_events):
        for sensor_type, (min_val, max_val) in SENSOR_RANGES.items():
            # Add some random variation but maintain a general trend
            base_value = random.uniform(min_val, max_val)
            # Add some noise
            value = base_value + random.uniform(-base_value * 0.05, base_value * 0.05)
            
            data.append({
                'event_id': str(uuid.uuid4()),
                'rig_name': rig_name,
                'location': OIL_RIGS[rig_name]['location'],
                'region': OIL_RIGS[rig_name]['region'],
                'sensor_type': sensor_type,
                'sensor_value': round(value, 2),
                'timestamp': current_time,
                'latitude': OIL_RIGS[rig_name]['lat'],
                'longitude': OIL_RIGS[rig_name]['lon']
            })
        
        # Increment time by 15 minutes
        current_time += timedelta(minutes=15)
    
    return data

def create_oil_rig_events_dataframe(rig_name: str, start_date: datetime = None, num_events: int = 100) -> DataFrame:
    """
    Creates a Spark DataFrame with oil rig sensor events.
    
    Args:
        rig_name (str): Name of the oil rig (must be a key in OIL_RIGS)
        start_date (datetime): Starting timestamp for the sensor data. Defaults to 2 days ago.
        num_events (int): Number of events to generate. Defaults to 100.
    
    Returns:
        DataFrame: Spark DataFrame containing oil rig sensor events
    """
    # Initialize Spark session
    spark = SparkSession.active()
    
    # Define schema
    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("rig_name", StringType(), False),
        StructField("location", StringType(), False),
        StructField("region", StringType(), False),
        StructField("sensor_type", StringType(), False),
        StructField("sensor_value", FloatType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False)
    ])
    
    # Default start date if not provided
    if start_date is None:
        start_date = datetime.now() - timedelta(days=2)
    
    # Generate sensor data
    data = generate_sensor_data(rig_name, start_date, num_events)
    
    # Create and return DataFrame
    return spark.createDataFrame(data, schema)

def get_available_rigs() -> list:
    """
    Returns a list of available oil rig names.
    
    Returns:
        list: List of available oil rig names
    """
    return list(OIL_RIGS.keys())

def get_rig_info(rig_name: str) -> dict:
    """
    Returns information about a specific oil rig.
    
    Args:
        rig_name (str): Name of the oil rig
    
    Returns:
        dict: Dictionary containing rig information
    """
    return OIL_RIGS.get(rig_name, {})

def main():
    """Test function to demonstrate the oil rig data generation utilities."""
    print("Testing oil rig sensor data generation:")
    
    # Test with permian rig - 50 events
    print("\n1. Testing Permian Rig with 50 events:")
    permian_df = create_oil_rig_events_dataframe('permian_rig', num_events=50)
    print(f"Generated {permian_df.count()} rows for Permian Rig")
    permian_df.show(5)  # Show first 5 rows
    
    # Test with eagle ford rig - 30 events
    print("\n2. Testing Eagle Ford Rig with 30 events:")
    eagle_ford_df = create_oil_rig_events_dataframe('eagle_ford_rig', num_events=30)
    print(f"Generated {eagle_ford_df.count()} rows for Eagle Ford Rig")
    eagle_ford_df.show(5)  # Show first 5 rows
    
    # Show available rigs
    print(f"\n3. Available rigs: {get_available_rigs()}")
    
    # Show rig info
    print(f"\n4. Permian rig info: {get_rig_info('permian_rig')}")

if __name__ == "__main__":
    main()
