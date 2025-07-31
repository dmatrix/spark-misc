"""
Offshore Oil Rig Sensor Data Processing with Spark 4.0 Variant Data Type
=========================================================================

This use case demonstrates processing offshore oil rig sensor data from various 
critical monitoring systems with different schemas using the Variant data type 
for flexibility and performance in oil & gas operations.
"""

import json
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

import time

def create_spark_session():
    """Create Spark session with Variant support"""
    return SparkSession.builder \
        .appName("Offshore Oil Rig Sensor Processing with Variant") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def generate_pressure_sensor_data(sensor_id, timestamp):
    """Generate pressure sensor data for drilling and wellhead operations"""
    return {
        "wellhead_pressure": round(random.uniform(500.0, 4000.0), 0),  # PSI
        "drilling_pressure": round(random.uniform(2000.0, 15000.0), 0),  # PSI
        "mud_pump_pressure": round(random.uniform(1000.0, 5000.0), 0)  # PSI
    }

def generate_flow_sensor_data(sensor_id, timestamp):
    """Generate flow meter sensor data for drilling mud and production fluids"""
    return {
        "mud_flow_rate": round(random.uniform(300.0, 800.0), 1),  # gallons per minute
        "oil_flow_rate": round(random.uniform(50.0, 500.0), 1),  # barrels per hour
        "gas_flow_rate": round(random.uniform(1000.0, 50000.0), 0)  # cubic feet per hour
    }

def generate_gas_sensor_data(sensor_id, timestamp):
    """Generate gas detection sensor data for safety monitoring"""
    return {
        "h2s_concentration": round(random.uniform(0.0, 20.0), 2),  # ppm (dangerous above 10)
        "methane_concentration": round(random.uniform(0.0, 5000.0), 0),  # ppm
        "oxygen_level": round(random.uniform(19.5, 21.0), 1)  # percentage
    }

def generate_temperature_sensor_data(sensor_id, timestamp):
    """Generate temperature sensor data for equipment and environmental monitoring"""
    return {
        "equipment_temperature": round(random.uniform(40.0, 120.0), 1),  # Celsius
        "ambient_temperature": round(random.uniform(10.0, 45.0), 1),  # Celsius
        "sea_water_temperature": round(random.uniform(5.0, 30.0), 1)  # Celsius
    }

def generate_vibration_sensor_data(sensor_id, timestamp):
    """Generate vibration sensor data for equipment monitoring"""
    return {
        "overall_vibration": round(random.uniform(0.5, 15.0), 2),  # mm/s RMS
        "x_axis": round(random.uniform(0.1, 5.0), 2),  # mm/s RMS
        "y_axis": round(random.uniform(0.1, 5.0), 2)  # mm/s RMS
    }

def generate_position_sensor_data(sensor_id, timestamp):
    """Generate position sensor data for drilling equipment"""
    return {
        "drill_bit_depth": round(random.uniform(1000.0, 8000.0), 1),  # meters
        "hook_load": round(random.uniform(100.0, 500.0), 1),  # tons
        "rotary_position": round(random.uniform(0.0, 360.0), 1)  # degrees
    }

def generate_weather_sensor_data(sensor_id, timestamp):
    """Generate weather sensor data for offshore conditions"""
    return {
        "wind_speed": round(random.uniform(0.0, 50.0), 1),  # knots
        "wave_height": round(random.uniform(0.5, 10.0), 1),  # meters
        "barometric_pressure": round(random.uniform(980.0, 1050.0), 1)  # mbar
    }

def generate_level_sensor_data(sensor_id, timestamp):
    """Generate level sensor data for tanks and vessels"""
    return {
        "fluid_level": round(random.uniform(10.0, 95.0), 1),  # percentage
        "volume": round(random.uniform(1000.0, 50000.0), 0),  # liters
        "tank_type": random.choice(["fuel", "fresh_water", "drilling_mud"])  # type of tank
    }

def generate_current_sensor_data(sensor_id, timestamp):
    """Generate current sensor data for marine environment"""
    return {
        "current_speed": round(random.uniform(0.1, 3.0), 2),  # knots
        "current_direction": random.randint(0, 359),  # degrees
        "water_temperature": round(random.uniform(5.0, 30.0), 1)  # Celsius
    }

def generate_spill_detection_data(sensor_id, timestamp):
    """Generate oil spill detection sensor data"""
    oil_detected = random.choice([True, False, False, False])  # 25% detection rate
    return {
        "oil_detected": oil_detected,
        "spill_thickness": round(random.uniform(0.0, 5.0), 2) if oil_detected else 0.0,  # mm
        "detection_confidence": round(random.uniform(0.7, 1.0), 2)  # confidence level
    }

def generate_fake_oil_rig_data(num_records=50000):
    """Generate large dataset of offshore oil rig sensor readings"""
    print(f"Generating {num_records} offshore oil rig sensor records...")
    
    sensor_types = [
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

def run_oil_rig_analysis():
    """Run the offshore oil rig sensor data analysis"""
    print("=" * 60)
    print("Offshore Oil Rig Sensor Data Processing with Spark 4.0 Variant")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Generate fake data
        start_time = time.time()
        fake_data = generate_fake_oil_rig_data(50000)
        print(f"Data generation completed in {time.time() - start_time:.2f} seconds")
        
        # Create DataFrame
        print("\nCreating DataFrame...")
        df = spark.createDataFrame(fake_data)
        
        # Create temp table and convert JSON to Variant
        df.createOrReplaceTempView("oil_rig_raw")
        
        print("Converting JSON strings to Variant data type...")
        sensor_df = spark.sql("""
            SELECT 
                sensor_id,
                sensor_type,
                CAST(timestamp AS TIMESTAMP) as timestamp,
                PARSE_JSON(sensor_data_json) as sensor_data
            FROM oil_rig_raw
        """)
        
        sensor_df.createOrReplaceTempView("oil_rig_sensors")
        print(f"Created oil rig sensor dataset with {sensor_df.count()} records")
        
        # Analysis 1: Pressure monitoring analysis
        print("\n" + "="*50)
        print("ANALYSIS 1: Pressure Monitoring Analytics")
        print("="*50)
        
        pressure_analysis = spark.sql("""
            SELECT 
                AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double')) as avg_wellhead_pressure,
                AVG(VARIANT_GET(sensor_data, '$.drilling_pressure', 'double')) as avg_drilling_pressure,
                AVG(VARIANT_GET(sensor_data, '$.mud_pump_pressure', 'double')) as avg_mud_pump_pressure,
                COUNT(*) as reading_count
            FROM oil_rig_sensors 
            WHERE sensor_type = 'pressure'
        """)
        
        print("Average Pressure Analysis Across All Sensors:")
        pressure_analysis.show(truncate=False)
        
        # Analysis 2: Flow measurement analysis
        print("\n" + "="*50)
        print("ANALYSIS 2: Flow Measurement Analytics")  
        print("="*50)
        
        flow_analysis = spark.sql("""
            SELECT 
                AVG(VARIANT_GET(sensor_data, '$.mud_flow_rate', 'double')) as avg_mud_flow,
                AVG(VARIANT_GET(sensor_data, '$.oil_flow_rate', 'double')) as avg_oil_flow,
                AVG(VARIANT_GET(sensor_data, '$.gas_flow_rate', 'double')) as avg_gas_flow,
                COUNT(*) as reading_count
            FROM oil_rig_sensors 
            WHERE sensor_type = 'flow'
        """)
        
        print("Average Flow Rate Analysis Across All Sensors:")
        flow_analysis.show(truncate=False)
        
        # Analysis 3: Gas detection safety analysis
        print("\n" + "="*50)
        print("ANALYSIS 3: Gas Detection Safety Analytics")
        print("="*50)
        
        gas_analysis = spark.sql("""
            SELECT 
                AVG(VARIANT_GET(sensor_data, '$.h2s_concentration', 'double')) as avg_h2s,
                AVG(VARIANT_GET(sensor_data, '$.methane_concentration', 'double')) as avg_methane,
                AVG(VARIANT_GET(sensor_data, '$.oxygen_level', 'double')) as avg_oxygen,
                COUNT(*) as reading_count
            FROM oil_rig_sensors 
            WHERE sensor_type = 'gas'
        """)
        
        print("Average Gas Concentration Safety Analysis:")
        gas_analysis.show(truncate=False)
        
        # Analysis 4: Weather and environmental monitoring
        print("\n" + "="*50)
        print("ANALYSIS 4: Weather and Environmental Monitoring")
        print("="*50)
        
        weather_analysis = spark.sql("""
            SELECT 
                AVG(VARIANT_GET(sensor_data, '$.wind_speed', 'double')) as avg_wind_speed,
                AVG(VARIANT_GET(sensor_data, '$.wave_height', 'double')) as avg_wave_height,
                AVG(VARIANT_GET(sensor_data, '$.barometric_pressure', 'double')) as avg_barometric_pressure,
                COUNT(*) as reading_count
            FROM oil_rig_sensors 
            WHERE sensor_type = 'weather'
        """)
        
        print("Average Weather and Environmental Conditions:")
        weather_analysis.show(truncate=False)
        
        # Analysis 5: Multi-sensor operational summary
        print("\n" + "="*50)
        print("ANALYSIS 5: Multi-Sensor Operational Summary")
        print("="*50)
        
        summary_analysis = spark.sql("""
            SELECT 
                sensor_type,
                COUNT(*) as total_readings,
                COUNT(DISTINCT sensor_id) as unique_sensors,
                -- High pressure readings (>2500 PSI)
                SUM(CASE WHEN sensor_type = 'pressure' AND VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double') > 2500 THEN 1 ELSE 0 END) as high_pressure_readings,
                -- High H2S readings (>10 ppm - dangerous level)
                SUM(CASE WHEN sensor_type = 'gas' AND VARIANT_GET(sensor_data, '$.h2s_concentration', 'double') > 10 THEN 1 ELSE 0 END) as dangerous_h2s_readings,
                -- High wind conditions (>30 knots)
                SUM(CASE WHEN sensor_type = 'weather' AND VARIANT_GET(sensor_data, '$.wind_speed', 'double') > 30 THEN 1 ELSE 0 END) as high_wind_readings,
                -- Oil spill detections
                SUM(CASE WHEN sensor_type = 'spill_detection' AND VARIANT_GET(sensor_data, '$.oil_detected', 'boolean') = true THEN 1 ELSE 0 END) as spill_detections
            FROM oil_rig_sensors
            GROUP BY sensor_type
            ORDER BY total_readings DESC
        """)
        
        print("Operational Summary by Sensor Type:")
        summary_analysis.show(truncate=False)
        
        # Performance comparison demonstration
        print("\n" + "="*50)
        print("PERFORMANCE COMPARISON")
        print("="*50)
        
        # Query using Variant (efficient)
        start_time = time.time()
        variant_count = spark.sql("""
            SELECT COUNT(*) as total_readings
            FROM oil_rig_sensors 
            WHERE VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double') > 3000 
               OR VARIANT_GET(sensor_data, '$.h2s_concentration', 'double') > 5
               OR VARIANT_GET(sensor_data, '$.wind_speed', 'double') > 30
               OR VARIANT_GET(sensor_data, '$.oil_detected', 'boolean') = true
               OR VARIANT_GET(sensor_data, '$.overall_vibration', 'double') > 10
        """).collect()[0]['total_readings']
        variant_time = time.time() - start_time
        
        print(f"Variant query time: {variant_time:.3f} seconds")
        print(f"Oil rig records matching critical operational criteria: {variant_count}")
        
        # Show schema for reference
        print("\n" + "="*50)
        print("DATASET SCHEMA")
        print("="*50)
        sensor_df.printSchema()
        
        print(f"\nOffshore oil rig analysis completed successfully!")
        print(f"Dataset size: {sensor_df.count()} records")
        print("Key operational insights from 10 critical sensor types:")
        print("- Pressure monitoring for drilling and wellhead operations")
        print("- Flow measurement for drilling mud and production fluids")  
        print("- Gas detection for H2S, methane, and oxygen safety monitoring")
        print("- Temperature monitoring for equipment and environmental conditions")
        print("- Vibration analysis for predictive equipment maintenance")
        print("- Position tracking for drilling equipment operations")
        print("- Weather monitoring for offshore safety conditions")
        print("- Level monitoring for fuel, water, and drilling mud tanks")
        print("- Current measurement for marine environmental conditions")
        print("- Oil spill detection for environmental protection")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_oil_rig_analysis()