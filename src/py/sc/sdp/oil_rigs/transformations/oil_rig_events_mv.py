from pyspark import pipelines as sdp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid

spark = SparkSession.active()
fake = Faker()

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

@sdp.materialized_view
def permian_rig_mv() -> DataFrame:
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
    
    start_date = datetime.now() - timedelta(days=2)
    data = generate_sensor_data('permian_rig', start_date)
    return spark.createDataFrame(data, schema)

@sdp.materialized_view
def eagle_ford_rig_mv() -> DataFrame:
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
    
    start_date = datetime.now() - timedelta(days=2)
    data = generate_sensor_data('eagle_ford_rig', start_date, num_events=10000)
    return spark.createDataFrame(data, schema) 