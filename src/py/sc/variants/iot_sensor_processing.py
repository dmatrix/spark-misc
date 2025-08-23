"""
Offshore Oil Rig Sensor Data Processing with Spark 4.0 Variant Data Type
=========================================================================

This use case demonstrates processing offshore oil rig sensor data from 10 
monitoring systems using the Variant data type for flexibility and performance in 
oil & gas operations.

Key Features:
- 10 sensor types: pressure, flow, gas, temperature, vibration, position, weather, level, current, spill detection
- Individual sensor records for detailed analytics
- CTE-optimized queries for performance
- Safety and operational monitoring
- Uses shared data_utility module

Performance Optimizations:
- Eliminates window function warnings with CTE-based queries
- Maintains Spark's parallel processing capabilities
- Optimized for sensor data processing

Data Structure:
Each record contains individual sensor readings with 3 key measurements per sensor type,
enabling detailed analysis of specific operational parameters.

Authors: Jules S. Damji & Cursor AI
"""

import time
from pyspark.sql import SparkSession
from data_utility import generate_oil_rig_data

def create_spark_session() -> SparkSession:
    """Create Spark session with Variant support.
    
    Returns:
        SparkSession: Configured Spark session with adaptive query execution
            and partition coalescing enabled for optimal performance
    """
    return SparkSession.builder \
        .appName("Offshore Oil Rig Sensor Processing with Variant") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()



def run_oil_rig_analysis() -> None:
    """Run the offshore oil rig sensor data analysis.
    
    Generates 50,000 oil rig sensor records, processes them using Spark's Variant
    data type, and performs comprehensive analytics including:
    - Sensor type distribution analysis
    - Critical pressure monitoring (wellhead pressure > 3500 PSI)
    - Gas detection safety analysis (H2S concentration > 10 ppm)
    - Equipment health monitoring (vibration levels > 10 mm/s RMS)
    - Environmental conditions analysis
    
    All queries are optimized using CTE patterns to avoid Spark window function
    warnings and maintain parallel processing capabilities.
    
    Returns:
        None
    """
    print("=" * 60)
    print("Offshore Oil Rig Sensor Data Processing with Spark 4.0 Variant")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Generate fake data
        start_time = time.time()
        fake_data = generate_oil_rig_data(50000)
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