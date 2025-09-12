"""
General Purpose Generator Utilities

Reusable components for creating scheduled data generators.
Can be used for orders, oil rigs, or any other batch data generation tasks.
"""

import json
import logging
import signal
import sys
import time
import uuid
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
from abc import ABC, abstractmethod


class DataGenerator(ABC):
    """Abstract base class for data generators."""
    
    @abstractmethod
    def generate_item(self) -> Dict[str, Any]:
        """Generate a single data item."""
        pass
    
    def generate_batch(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """Generate a batch of items."""
        return [self.generate_item() for _ in range(batch_size)]


class OrderGenerator(DataGenerator):
    """Generates random order events for both JSON and Spark DataFrame output."""
    
    def __init__(self):
        self.items = [
            "Toy Car", "Basketball", "Laptop", "Action Figure", "Tennis Racket",
            "Smartphone", "Board Game", "Football", "Headphones", "Drone",
            "Puzzle", "Tablet", "Skateboard", "Camera", "Video Game",
            "Scooter", "Smartwatch", "Baseball Bat", "VR Headset", "Electric Guitar",
            "Coffee Maker", "Bluetooth Speaker", "Fitness Tracker", "Gaming Chair",
            "Wireless Mouse", "Keyboard", "Monitor", "Backpack", "Water Bottle",
            "Running Shoes", "Jacket", "Sunglasses", "Watch", "Earbuds", "Power Bank"
        ]
        
        self.statuses = ["pending", "approved", "fulfilled"]
    
    def generate_item(self) -> Dict[str, Any]:
        """Generate a single random order event (for JSON output)."""
        return {
            "order_id": str(uuid.uuid4()),
            "order_item": random.choice(self.items),
            "price": round(random.uniform(10.0, 1000.0), 2),
            "items_ordered": random.randint(1, 10),
            "status": random.choice(self.statuses),
            "date_ordered": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
            "customer_id": str(uuid.uuid4()),
            "order_timestamp": datetime.now().isoformat() + "Z"
        }
    
    def create_spark_dataframe(self, num_events: int = 100) -> 'pyspark.sql.DataFrame':
        """
        Generates a Spark DataFrame with random order events.
        
        Args:
            num_events (int): Number of random order events to generate. Defaults to 100.
        
        Returns:
            pyspark.sql.DataFrame: DataFrame containing random order events.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
        
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

        # Generate random rows based on num_events parameter
        data = []
        for _ in range(num_events):
            order_id = str(uuid.uuid4())
            order_item = random.choice(self.items)
            price = round(random.uniform(10.0, 1000.0), 2)
            items_ordered = random.randint(1, 10)
            status = random.choice(self.statuses)
            date_ordered = (datetime.now() - timedelta(days=random.randint(0, 30))).date()
            data.append((order_id, order_item, price, items_ordered, status, date_ordered))

        # Create DataFrame
        orders_df = spark.createDataFrame(data, schema)
        return orders_df


class OilRigGenerator(DataGenerator):
    """Generates random oil rig events for both JSON and Spark DataFrame output."""
    
    def __init__(self):
        self.rig_types = [
            "Jack-up", "Semi-submersible", "Drillship", "Platform", "Barge",
            "Land Rig", "Offshore Platform", "Floating Production", "Tension Leg"
        ]
        
        self.locations = [
            "Gulf of Mexico", "North Sea", "Persian Gulf", "Caspian Sea",
            "South China Sea", "Mediterranean", "Black Sea", "Red Sea",
            "Norwegian Sea", "Barents Sea", "Bass Strait", "Campos Basin"
        ]
        
        self.operators = [
            "ExxonMobil", "Shell", "BP", "Chevron", "Total", "ConocoPhillips",
            "Eni", "Equinor", "Petrobras", "Saudi Aramco", "Gazprom", "Rosneft"
        ]
        
        # For JSON entities
        self.statuses = ["active", "maintenance", "standby", "decommissioned"]
        
        # Sensor ranges
        SENSOR_RANGES = {
            'temperature': (150, 350),  # Fahrenheit
            'pressure': (2000, 5000),   # PSI
            'water_level': (100, 500)   # Feet
        }
        self.sensor_ranges = SENSOR_RANGES
        
        # Predefined rigs for sensor data
        self.oil_rigs = {
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
    
    def generate_item(self) -> Dict[str, Any]:
        """Generate a single random oil rig entity event (for JSON output)."""
        # Pick a random sensor type
        sensor_type = random.choice(list(self.sensor_ranges.keys()))
        min_val, max_val = self.sensor_ranges[sensor_type]
        base_value = random.uniform(min_val, max_val)
        # Add some noise
        sensor_value = base_value + random.uniform(-base_value * 0.05, base_value * 0.05)
        
        return {
            "rig_id": str(uuid.uuid4()),
            "rig_name": f"RIG-{random.randint(1000, 9999)}",
            "rig_type": random.choice(self.rig_types),
            "location": random.choice(self.locations),
            "operator": random.choice(self.operators),
            "depth_meters": random.randint(50, 3000),
            "daily_production_barrels": random.randint(1000, 50000),
            "crew_size": random.randint(50, 200),
            "status": random.choice(self.statuses),
            "commissioning_date": (datetime.now() - timedelta(days=random.randint(365, 7300))).strftime("%Y-%m-%d"),
            "last_inspection": (datetime.now() - timedelta(days=random.randint(1, 180))).strftime("%Y-%m-%d"),
            "coordinates": {
                "latitude": round(random.uniform(-60.0, 70.0), 6),
                "longitude": round(random.uniform(-180.0, 180.0), 6)
            },
            "sensor_type": sensor_type,
            "sensor_value": round(sensor_value, 2),
            "timestamp": datetime.now().isoformat() + "Z"
        }
    
    def create_spark_dataframe(self, rig_name: str = 'permian_rig', start_date: datetime = None, num_events: int = 100) -> 'pyspark.sql.DataFrame':
        """
        Creates a Spark DataFrame with oil rig sensor events.
        
        Args:
            rig_name (str): Name of the oil rig (must be a key in oil_rigs)
            start_date (datetime): Starting timestamp for the sensor data. Defaults to 2 days ago.
            num_events (int): Number of events to generate. Defaults to 100.
        
        Returns:
            pyspark.sql.DataFrame: DataFrame containing oil rig sensor events
        """
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
        
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
        
        # Generate sensor events using the helper method
        data = self._generate_sensor_events(rig_name, start_date, num_events)
        
        # Create and return DataFrame
        return spark.createDataFrame(data, schema)
    
    def get_available_rigs(self) -> List[str]:
        """Returns a list of available oil rig names for sensor data."""
        return list(self.oil_rigs.keys())
    
    def get_rig_info(self, rig_name: str) -> Dict[str, Any]:
        """Returns information about a specific oil rig."""
        return self.oil_rigs.get(rig_name, {})
    
    def _generate_sensor_events(self, rig_name: str, start_date: datetime, num_events: int = 100) -> List[Dict[str, Any]]:
        """
        Generate raw sensor event data as a list of dictionaries.
        
        Args:
            rig_name (str): Name of the oil rig
            start_date (datetime): Starting timestamp for the sensor data
            num_events (int): Number of events to generate
        
        Returns:
            List[Dict[str, Any]]: List of sensor event dictionaries
        """
        data = []
        current_time = start_date
        
        for _ in range(num_events):
            for sensor_type, (min_val, max_val) in self.sensor_ranges.items():
                # Add some random variation but maintain a general trend
                base_value = random.uniform(min_val, max_val)
                # Add some noise
                value = base_value + random.uniform(-base_value * 0.05, base_value * 0.05)
                
                data.append({
                    'event_id': str(uuid.uuid4()),
                    'rig_name': rig_name,
                    'location': self.oil_rigs[rig_name]['location'],
                    'region': self.oil_rigs[rig_name]['region'],
                    'sensor_type': sensor_type,
                    'sensor_value': round(value, 2),
                    'timestamp': current_time,
                    'latitude': self.oil_rigs[rig_name]['lat'],
                    'longitude': self.oil_rigs[rig_name]['lon']
                })
            
            # Increment time by 15 minutes
            current_time += timedelta(minutes=15)
        
        return data


class BatchManager:
    """Manages batch file creation and batch numbering."""
    
    def __init__(self, output_dir: Path, file_prefix: str = "batch"):
        self.output_dir = Path(output_dir)
        self.file_prefix = file_prefix
        self.counter_file = self.output_dir / f".{file_prefix}_counter"
        self._ensure_directory()
        self.current_batch = self._load_batch_counter()
    
    def _ensure_directory(self):
        """Create output directory if it doesn't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_batch_counter(self) -> int:
        """Load the current batch counter from file."""
        if self.counter_file.exists():
            try:
                with open(self.counter_file, 'r') as f:
                    return int(f.read().strip())
            except (ValueError, IOError):
                logging.warning("Could not read batch counter, starting from 1")
        return 1
    
    def _save_batch_counter(self):
        """Save the current batch counter to file."""
        try:
            with open(self.counter_file, 'w') as f:
                f.write(str(self.current_batch))
        except IOError as e:
            logging.error(f"Could not save batch counter: {e}")
    
    def save_batch(self, items: List[Dict[str, Any]]) -> str:
        """Save a batch of items to a JSON file."""
        filename = f"{self.file_prefix}_{self.current_batch}.json"
        filepath = self.output_dir / filename
        
        try:
            with open(filepath, 'w') as f:
                json.dump(items, f, indent=2)
            
            self.current_batch += 1
            self._save_batch_counter()
            
            logging.info(f"Created batch file: {filepath} with {len(items)} items")
            return str(filepath)
            
        except IOError as e:
            logging.error(f"Failed to save batch {filename}: {e}")
            raise


class ScheduledRunner:
    """Handles the scheduled execution of data generation."""
    
    def __init__(self, 
                 data_generator: DataGenerator, 
                 batch_manager: BatchManager,
                 interval_minutes: int, 
                 end_minutes: int = None,
                 batch_size: int = 100):
        self.data_generator = data_generator
        self.batch_manager = batch_manager
        self.interval_seconds = interval_minutes * 60
        self.interval_minutes = interval_minutes
        self.end_seconds = end_minutes * 60 if end_minutes else None
        self.batch_size = batch_size
        self.running = True
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, _):
        """Handle shutdown signals gracefully."""
        logging.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def run(self):
        """Main execution loop."""
        logging.info(f"Starting data generator (interval: {self.interval_minutes} minutes)")
        logging.info(f"Output directory: {self.batch_manager.output_dir}")
        logging.info(f"Starting from batch number: {self.batch_manager.current_batch}")
        
        if self.end_seconds:
            expected_batches = self.end_seconds // self.interval_seconds
            logging.info(f"Will run for {self.end_seconds//60} minutes, generating {expected_batches} batches")
        
        start_time = time.time()
        
        try:
            while self.running:
                batch_start_time = time.time()
                
                # Check if we've exceeded the end time
                if self.end_seconds and (batch_start_time - start_time) >= self.end_seconds:
                    logging.info("Reached end time, stopping generator")
                    break
                
                try:
                    # Generate and save batch
                    items = self.data_generator.generate_batch(self.batch_size)
                    self.batch_manager.save_batch(items)
                    
                    generation_time = time.time() - batch_start_time
                    logging.info(f"Generated batch in {generation_time:.2f} seconds")
                    
                except Exception as e:
                    logging.error(f"Error generating batch: {e}")
                
                # Wait for the next interval
                if self.running:
                    # Check if we have time for another batch
                    elapsed_time = time.time() - start_time
                    if self.end_seconds and (elapsed_time + self.interval_seconds) > self.end_seconds:
                        logging.info("Not enough time for another batch, stopping generator")
                        break
                    
                    logging.info(f"Waiting {self.interval_minutes} minutes until next batch...")
                    time.sleep(self.interval_seconds)
                    
        except KeyboardInterrupt:
            logging.info("Received KeyboardInterrupt, shutting down...")
        
        total_runtime = time.time() - start_time
        logging.info(f"Data generator stopped after {total_runtime/60:.1f} minutes")


def setup_logging(log_filename: str = "generator.log"):
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_filename)
        ]
    )


def validate_generator_args(interval: int, end: int = None) -> int:
    """
    Validate generator arguments and return the end time.
    
    Args:
        interval: Interval in minutes
        end: End time in minutes (optional)
    
    Returns:
        Validated end time in minutes
        
    Raises:
        SystemExit: If validation fails
    """
    # Validate interval
    if interval <= 0:
        logging.error("Interval must be a positive number")
        sys.exit(1)
    
    # Set default end time if not provided
    if end is None:
        end = interval * 3
        logging.info(f"No end time specified, using default: {end} minutes")
    
    # Validate that end time is evenly divisible by interval
    if end % interval != 0:
        logging.error(f"End time ({end} minutes) must be evenly divisible by interval ({interval} minutes)")
        sys.exit(1)
    
    # Validate that end time is positive
    if end <= 0:
        logging.error("End time must be a positive number")
        sys.exit(1)
    
    return end