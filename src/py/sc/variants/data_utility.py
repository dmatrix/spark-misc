"""
Data Utility Module for Spark 4.0 Variant Use Cases
===================================================

This module contains shared data generation functions and constants used across
all use cases for consistency and maintainability.

Key Features:
- Centralized data generation logic for consistency
- Realistic data patterns with temporal and behavioral weighting
- Support for both individual and comprehensive data structures

Contains:
- Oil Rig IoT sensor data generators (10 sensor types, 3 measurements each)
- E-commerce event data generators (purchase, search, wishlist with nested structures)
- Security log data generators (firewall, antivirus, IDS with geo-location data)
- Common constants and helper functions
- Comprehensive oil rig data generation (all sensors in single record)

Oil Rig Sensor Types:
1. Pressure: wellhead_pressure, drilling_pressure, mud_pump_pressure
2. Flow: mud_flow_rate, oil_flow_rate, gas_flow_rate
3. Gas: h2s_concentration, methane_concentration, oxygen_level
4. Temperature: equipment_temperature, ambient_temperature, sea_water_temperature
5. Vibration: overall_vibration, x_axis, y_axis
6. Position: drill_bit_depth, hook_load, rotary_position
7. Weather: wind_speed, wave_height, barometric_pressure
8. Level: fluid_level, volume, tank_type
9. Current: current_speed, current_direction, water_temperature
10. Spill Detection: oil_detected, spill_thickness, detection_confidence

Data Generation Modes:
- Individual sensor records: generate_oil_rig_data() - for main use case analytics
- Comprehensive records: generate_comprehensive_oil_rig_data() - all sensors in single JSON
- Realistic patterns: timestamp weighting, user behavior consistency, threat patterns

Usage:
    from data_utility import generate_oil_rig_data, generate_ecommerce_data, generate_security_data
    
    # Generate individual sensor records
    oil_data = generate_oil_rig_data(1000)
    
    # Generate comprehensive sensor records  
    comprehensive_data = generate_comprehensive_oil_rig_data(100)
    
    # Generate e-commerce events
    ecommerce_data = generate_ecommerce_data(5000)

Authors: Jules S. Damji & Cursor AI
"""

import json
import random
import hashlib
import ipaddress
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional, Callable, Union
from pyspark.sql import SparkSession

# ============================================================================
# SPARK SESSION CREATION
# ============================================================================

def create_spark_session(app_name: str = "Spark Variant Data Processing") -> SparkSession:
    """Create Spark session with Variant support.
    
    Args:
        app_name (str): Name of the Spark application (default: "Spark Variant Data Processing")
    
    Returns:
        SparkSession: Configured Spark session with adaptive query execution
            and partition coalescing enabled for optimal performance
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

# ============================================================================
# COMMON CONSTANTS
# ============================================================================

# E-commerce constants
CATEGORIES = ["electronics", "clothing", "books", "home", "sports", "toys", "beauty", "automotive"]
BRANDS = ["TechCorp", "StyleBrand", "ReadMore", "HomeComfort", "SportMax", "FunToys", "GlowBeauty", "AutoPro"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
SHIPPING_METHODS = ["standard", "express", "overnight", "pickup"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"]
STATES = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA"]
DISCOUNT_CODES = ["SAVE10", "WELCOME20", "FLASH15", "MEMBER5", "NEWUSER25", "LOYAL30"]

# Security constants
THREAT_TYPES = ["malware", "ransomware", "trojan", "virus", "adware", "spyware", "rootkit", "worm"]
FILE_EXTENSIONS = [".exe", ".dll", ".bat", ".ps1", ".vbs", ".js", ".jar", ".scr", ".com"]
ATTACK_TYPES = ["sql_injection", "xss", "csrf", "buffer_overflow", "directory_traversal", "command_injection", "ldap_injection"]
COUNTRIES = ["US", "CN", "RU", "BR", "IN", "DE", "FR", "GB", "CA", "AU", "Unknown"]
USER_AGENTS = [
    "Chrome/91.0.4472.124",
    "Firefox/89.0",
    "Safari/14.1.1"
]
PROTOCOLS = ["TCP", "UDP", "ICMP", "HTTP", "HTTPS", "SSH", "FTP", "SMTP", "DNS"]
ACTION_TAKEN = ["quarantined", "deleted", "blocked", "allowed", "monitored", "flagged"]

# ============================================================================
# COMMON HELPER FUNCTIONS
# ============================================================================

def generate_random_ip() -> str:
    """Generate random IP address"""
    return str(ipaddress.IPv4Address(random.randint(1, 4294967294)))

def generate_file_hash() -> Dict[str, str]:
    """Generate realistic file hash"""
    random_string = f"file_{random.randint(100000, 999999)}"
    return {
        "md5": hashlib.md5(random_string.encode()).hexdigest(),
        "sha256": hashlib.sha256(random_string.encode()).hexdigest(),
        "sha1": hashlib.sha1(random_string.encode()).hexdigest()[:40]
    }

def generate_timestamp_with_pattern(start_time: datetime, days_range: int = 30, hour_weights: Optional[List[int]] = None) -> datetime:
    """Generate timestamp with realistic patterns"""
    if hour_weights is None:
        # Default business hours pattern
        hour_weights = [2, 1, 1, 1, 2, 3, 5, 8, 10, 12, 15, 16, 16, 15, 14, 16, 18, 16, 12, 8, 6, 4, 3, 2]
    
    hour_weight = random.choices(range(24), weights=hour_weights)[0]
    
    return start_time + timedelta(
        days=random.randint(0, days_range),
        hours=hour_weight,
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

# ============================================================================
# OIL RIG IoT SENSOR DATA GENERATORS
# ============================================================================

def generate_pressure_sensor_data(sensor_id: str, timestamp: datetime) -> Dict[str, float]:
    """Generate pressure sensor data for drilling and wellhead operations"""
    return {
        "wellhead_pressure": round(random.uniform(500.0, 4000.0), 0),  # PSI
        "drilling_pressure": round(random.uniform(2000.0, 15000.0), 0),  # PSI
        "mud_pump_pressure": round(random.uniform(1000.0, 5000.0), 0)  # PSI
    }

def generate_flow_sensor_data(sensor_id: str, timestamp: datetime) -> Dict[str, float]:
    """Generate flow meter sensor data for drilling mud and production fluids"""
    return {
        "mud_flow_rate": round(random.uniform(300.0, 800.0), 1),  # gallons per minute
        "oil_flow_rate": round(random.uniform(50.0, 500.0), 1),  # barrels per hour
        "gas_flow_rate": round(random.uniform(1000.0, 50000.0), 0)  # cubic feet per hour
    }

def generate_gas_sensor_data(sensor_id: str, timestamp: datetime) -> Dict[str, float]:
    """Generate gas detection sensor data for safety monitoring"""
    return {
        "h2s_concentration": round(random.uniform(0.0, 20.0), 2),  # ppm (dangerous above 10)
        "methane_concentration": round(random.uniform(0.0, 5000.0), 0),  # ppm
        "oxygen_level": round(random.uniform(19.5, 21.0), 1)  # percentage
    }

def generate_temperature_sensor_data(sensor_id: str, timestamp: datetime) -> Dict[str, float]:
    """Generate temperature sensor data for equipment and environmental monitoring"""
    return {
        "equipment_temperature": round(random.uniform(40.0, 120.0), 1),  # Celsius
        "ambient_temperature": round(random.uniform(10.0, 45.0), 1),  # Celsius
        "sea_water_temperature": round(random.uniform(5.0, 30.0), 1)  # Celsius
    }

def generate_vibration_sensor_data(sensor_id: str, timestamp: datetime) -> Dict[str, float]:
    """Generate vibration sensor data for equipment monitoring"""
    return {
        "overall_vibration": round(random.uniform(0.5, 15.0), 2),  # mm/s RMS
        "x_axis": round(random.uniform(0.1, 5.0), 2),  # mm/s RMS
        "y_axis": round(random.uniform(0.1, 5.0), 2)  # mm/s RMS
    }

def generate_position_sensor_data(sensor_id: str, timestamp: datetime) -> Dict[str, float]:
    """Generate position sensor data for drilling equipment"""
    return {
        "drill_bit_depth": round(random.uniform(1000.0, 8000.0), 1),  # meters
        "hook_load": round(random.uniform(100.0, 500.0), 1),  # tons
        "rotary_position": round(random.uniform(0.0, 360.0), 1)  # degrees
    }

def generate_weather_sensor_data(sensor_id: str, timestamp: datetime) -> Dict[str, float]:
    """Generate weather sensor data for offshore conditions"""
    return {
        "wind_speed": round(random.uniform(0.0, 50.0), 1),  # knots
        "wave_height": round(random.uniform(0.5, 10.0), 1),  # meters
        "barometric_pressure": round(random.uniform(980.0, 1050.0), 1)  # mbar
    }

def generate_level_sensor_data(sensor_id: str, timestamp: datetime) -> Dict[str, Union[float, str]]:
    """Generate level sensor data for tanks and vessels"""
    return {
        "fluid_level": round(random.uniform(10.0, 95.0), 1),  # percentage
        "volume": round(random.uniform(1000.0, 50000.0), 0),  # liters
        "tank_type": random.choice(["fuel", "fresh_water", "drilling_mud"])  # type of tank
    }

def generate_current_sensor_data(sensor_id: str, timestamp: datetime) -> Dict[str, Union[float, int]]:
    """Generate current sensor data for marine environment"""
    return {
        "current_speed": round(random.uniform(0.1, 3.0), 2),  # knots
        "current_direction": random.randint(0, 359),  # degrees
        "water_temperature": round(random.uniform(5.0, 30.0), 1)  # Celsius
    }

def generate_spill_detection_data(sensor_id: str, timestamp: datetime) -> Dict[str, Union[bool, float]]:
    """Generate oil spill detection sensor data"""
    oil_detected = random.choice([True, False, False, False])  # 25% detection rate
    return {
        "oil_detected": oil_detected,
        "spill_thickness": round(random.uniform(0.0, 5.0), 2) if oil_detected else 0.0,  # mm
        "detection_confidence": round(random.uniform(0.7, 1.0), 2)  # confidence level
    }

def get_oil_rig_sensor_types() -> List[Tuple[str, Callable[[str, datetime], Dict[str, Any]]]]:
    """Get list of oil rig sensor types and their generators"""
    return [
        ("pressure", generate_pressure_sensor_data),
        ("flow", generate_flow_sensor_data),
        ("gas", generate_gas_sensor_data),
        ("temperature", generate_temperature_sensor_data),
        ("vibration", generate_vibration_sensor_data),
        ("position", generate_position_sensor_data),
        ("weather", generate_weather_sensor_data),
        ("level", generate_level_sensor_data),
        ("current", generate_current_sensor_data),
        ("spill_detection", generate_spill_detection_data)
    ]

# ============================================================================
# E-COMMERCE EVENT DATA GENERATORS
# ============================================================================

def generate_purchase_event(user_id: str, timestamp: datetime) -> Dict[str, Any]:
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

def generate_search_event(user_id: str, timestamp: datetime) -> Dict[str, Union[str, int]]:
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

def generate_wishlist_event(user_id: str, timestamp: datetime) -> Dict[str, Union[str, float]]:
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

def get_ecommerce_event_types() -> List[Tuple[str, Callable[[str, datetime], Dict[str, Any]], float]]:
    """Get list of e-commerce event types and their generators with weights"""
    return [
        ("purchase", generate_purchase_event, 0.4),   # 40% of events
        ("search", generate_search_event, 0.35),      # 35% of events
        ("wishlist", generate_wishlist_event, 0.25)   # 25% of events
    ]

# ============================================================================
# SECURITY LOG DATA GENERATORS
# ============================================================================

def generate_antivirus_event(event_id: str, timestamp: datetime) -> Dict[str, Union[str, float]]:
    """Generate antivirus/EDR security event (simplified)"""
    return {
        "threat_type": random.choice(THREAT_TYPES),
        "action_taken": random.choice(ACTION_TAKEN),
        "detection_score": round(random.uniform(0.1, 1.0), 3)
    }

def generate_firewall_event(event_id: str, timestamp: datetime) -> Dict[str, Any]:
    """Generate firewall security event (simplified with nested geo_location)"""
    return {
        "source_ip": generate_random_ip(),
        "action": random.choice(["blocked", "allowed", "dropped"]),
        "geo_location": {
            "source_country": random.choice(COUNTRIES),
            "dest_country": "US",
            "confidence": round(random.uniform(0.6, 1.0), 2)
        }
    }

def generate_ids_event(event_id: str, timestamp: datetime) -> Dict[str, str]:
    """Generate IDS/IPS security event (simplified)"""
    attack_type = random.choice(ATTACK_TYPES)
    return {
        "attack_type": attack_type,
        "source_ip": generate_random_ip(),
        "user_agent": random.choice(USER_AGENTS)
    }

def assign_severity(event_type: str, event_data: Dict[str, Any]) -> str:
    """Assign severity based on event type and data"""
    if event_type == "antivirus":
        if event_data.get("threat_type") in ["ransomware", "rootkit"]:
            return "critical"
        elif event_data.get("detection_score", 0) > 0.8:
            return "high"
        else:
            return "medium"
    elif event_type == "firewall":
        if event_data.get("geo_location", {}).get("source_country") in ["CN", "RU"]:
            return "high"
        elif event_data.get("action") == "blocked":
            return "medium"
        else:
            return "low"
    elif event_type == "ids":
        if event_data.get("attack_type") in ["sql_injection", "command_injection"]:
            return "critical"
        else:
            return "medium"
    
    return "medium"

def get_security_event_types() -> List[Tuple[str, Callable[[str, datetime], Dict[str, Any]], float]]:
    """Get list of security event types and their generators with weights"""
    return [
        ("firewall", generate_firewall_event, 0.4),     # 40% of events
        ("antivirus", generate_antivirus_event, 0.35),  # 35% of events
        ("ids", generate_ids_event, 0.25)               # 25% of events
    ]

# ============================================================================
# BULK DATA GENERATION FUNCTIONS
# ============================================================================

def generate_comprehensive_oil_rig_data(num_records: int = 1) -> List[Dict[str, str]]:
    """Generate comprehensive oil rig sensor data with ALL 10 sensor types in each record"""
    print(f"Generating {num_records} comprehensive oil rig sensor records...")
    
    sensor_types = get_oil_rig_sensor_types()
    data = []
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    
    for i in range(num_records):
        # Generate timestamp with some randomness
        timestamp = start_time + timedelta(
            hours=random.randint(0, 24*30),  # 30 days range
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # Generate data for ALL sensor types and combine into single JSON
        comprehensive_sensor_data = {}
        sensor_id = f"RIG_{random.randint(1, 10):02d}_COMPREHENSIVE_{i:06d}"
        
        # Generate data for each sensor type
        for sensor_type, generator_func in sensor_types:
            individual_sensor_id = f"{sensor_type.upper()}_{random.randint(1, 100):03d}"
            sensor_readings = generator_func(individual_sensor_id, timestamp)
            comprehensive_sensor_data[sensor_type] = sensor_readings
        
        data.append({
            "sensor_id": sensor_id,
            "sensor_type": "comprehensive",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "sensor_data_json": json.dumps(comprehensive_sensor_data)
        })
        
        if (i + 1) % 10000 == 0:
            print(f"Generated {i + 1} records...")
    
    return data

def generate_oil_rig_data(num_records: int = 1) -> List[Dict[str, str]]:
    """Generate oil rig sensor data (individual sensor records for main use cases)"""
    print(f"Generating {num_records} oil rig sensor records...")
    
    sensor_types = get_oil_rig_sensor_types()
    data = []
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    
    for i in range(num_records):
        # Generate timestamp with some randomness
        timestamp = start_time + timedelta(
            hours=random.randint(0, 24*30),  # 30 days range
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # Select sensor type and generate data
        sensor_type, generator_func = random.choice(sensor_types)
        sensor_id = f"{sensor_type.upper()}_{random.randint(1, 100):03d}"
        
        sensor_data = generator_func(sensor_id, timestamp)
        
        data.append({
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "sensor_data_json": json.dumps(sensor_data)
        })
        
        if (i + 1) % 10000 == 0:
            print(f"Generated {i + 1} records...")
    
    return data

def generate_complete_ecommerce_data(num_records_per_type: int = 100) -> List[Dict[str, str]]:
    """Generate complete e-commerce dataset with all event types.
    
    Creates a comprehensive dataset containing all three event types:
    - purchase events (40% of total)
    - search events (35% of total) 
    - wishlist events (25% of total)
    
    Args:
        num_records_per_type (int): Base number of records per event type (default: 100)
        
    Returns:
        List[Dict[str, str]]: List of dictionaries containing complete e-commerce event data
            with all event types represented
    """
    print(f"Generating complete e-commerce dataset with all event types...")
    
    event_types = get_ecommerce_event_types()
    all_events = []
    
    for event_type, generator_func, weight in event_types:
        # Calculate records for this event type based on weight
        records_for_type = int(num_records_per_type * weight / 0.25)  # Normalize to smallest weight
        print(f"Generating {records_for_type} {event_type} events...")
        
        for i in range(records_for_type):
            event_id = f"evt_{random.randint(100000000, 999999999):08x}"
            user_id = f"user_{random.randint(1, 10000):06d}"
            # Generate timestamp with some randomness
            timestamp = datetime(2024, 1, 1, 0, 0, 0) + timedelta(
                hours=random.randint(0, 24*30),  # 30 days range
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            event_data = generator_func(user_id, timestamp)
            
            all_events.append({
                "event_id": event_id,
                "user_id": user_id,
                "event_type": event_type,
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "event_data_json": json.dumps(event_data)
            })
    
    # Shuffle to mix event types
    random.shuffle(all_events)
    print(f"Generated {len(all_events)} total e-commerce events with all event types")
    return all_events

def generate_ecommerce_data(num_records: int = 1) -> List[Dict[str, str]]:
    """Generate e-commerce event data"""
    print(f"Generating {num_records} e-commerce event records...")
    
    event_types = get_ecommerce_event_types()
    
    # Create weighted list for realistic distribution
    weighted_events = []
    for event_type, generator, weight in event_types:
        weighted_events.extend([(event_type, generator)] * int(weight * 100))
    
    data = []
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    
    # Generate users for consistency
    users = [f"user_{i:06d}" for i in range(1, min(10001, num_records + 1))]
    
    for i in range(num_records):
        # Generate timestamp with realistic patterns (more activity during day)
        timestamp = generate_timestamp_with_pattern(start_time)
        
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

def generate_complete_security_data(num_records_per_type: int = 100) -> List[Dict[str, str]]:
    """Generate complete security dataset with all system types.
    
    Creates a comprehensive dataset containing all three security system types:
    - firewall events (40% of total)
    - antivirus events (35% of total)
    - IDS events (25% of total)
    
    Args:
        num_records_per_type (int): Base number of records per system type (default: 100)
        
    Returns:
        List[Dict[str, str]]: List of dictionaries containing complete security event data
            with all system types represented
    """
    print(f"Generating complete security dataset with all system types...")
    
    event_types = get_security_event_types()
    all_events = []
    
    for event_type, generator_func, weight in event_types:
        # Calculate records for this event type based on weight
        records_for_type = int(num_records_per_type * weight / 0.25)  # Normalize to smallest weight
        print(f"Generating {records_for_type} {event_type} events...")
        
        for i in range(records_for_type):
            event_id = f"sec_{i:08d}"
            # Generate timestamp with some randomness
            timestamp = datetime(2024, 1, 1, 0, 0, 0) + timedelta(
                hours=random.randint(0, 24*30),  # 30 days range
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            event_data = generator_func(event_id, timestamp)
            severity = assign_severity(event_type, event_data)
            
            all_events.append({
                "event_id": event_id,
                "source_system": event_type,
                "severity": severity,
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "event_details_json": json.dumps(event_data)
            })
    
    # Shuffle to mix event types
    random.shuffle(all_events)
    print(f"Generated {len(all_events)} total security events with all system types")
    return all_events

def generate_security_data(num_records: int = 1) -> List[Dict[str, str]]:
    """Generate security log data"""
    print(f"Generating {num_records} security event records...")
    
    event_types = get_security_event_types()
    
    # Create weighted list for realistic distribution
    weighted_events = []
    for event_type, generator, weight in event_types:
        weighted_events.extend([(event_type, generator)] * int(weight * 100))
    
    data = []
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    
    for i in range(num_records):
        # Generate timestamp with realistic patterns (more attacks during certain hours)
        security_hour_weights = [3, 2, 1, 1, 2, 3, 5, 8, 10, 12, 15, 16, 16, 15, 14, 16, 18, 16, 12, 8, 6, 4, 3, 2]
        timestamp = generate_timestamp_with_pattern(start_time, hour_weights=security_hour_weights)
        
        # Select event type
        event_type, generator_func = random.choice(weighted_events)
        
        # Generate event data
        event_data = generator_func(f"evt_{i:06d}", timestamp)
        
        # Assign severity based on event type and content
        severity = assign_severity(event_type, event_data)
        
        data.append({
            "event_id": f"sec_{i:08d}",
            "source_system": event_type,
            "severity": severity,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "event_details_json": json.dumps(event_data)
        })
        
        if (i + 1) % 12000 == 0:
            print(f"Generated {i + 1} records...")
    
    return data
