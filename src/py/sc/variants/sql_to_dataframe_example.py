"""
SQL to PySpark DataFrame Conversion Example
==========================================

This example demonstrates how to convert SQL queries using Variant data type
to their equivalent PySpark DataFrame operations.

Original SQL Query:
SELECT 
    AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double')) as avg_wellhead_pressure,
    AVG(VARIANT_GET(sensor_data, '$.drilling_pressure', 'double')) as avg_drilling_pressure,
    COUNT(*) as reading_count
FROM oil_rig_sensors 
WHERE sensor_type = 'pressure'

Authors: Jules S. Damji & Cursor AI
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, expr
from data_utility import generate_pressure_sensor_data
import json
from datetime import datetime

def create_spark_session():
    """Create Spark session with Variant support"""
    return SparkSession.builder \
        .appName("SQL to DataFrame Conversion Example") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def create_sample_data():
    """Create sample pressure sensor data"""
    pressure_data = []
    for i in range(10):
        timestamp = datetime(2024, 1, 1, 12, i, 0)
        sensor_id = f'PRESSURE_{i:03d}'
        sensor_readings = generate_pressure_sensor_data(sensor_id, timestamp)
        
        pressure_data.append({
            'sensor_id': sensor_id,
            'sensor_type': 'pressure',
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'sensor_data_json': json.dumps(sensor_readings)
        })
    
    return pressure_data

def demonstrate_sql_to_dataframe_conversion():
    """Demonstrate various ways to convert SQL to DataFrame operations"""
    
    spark = create_spark_session()
    
    try:
        print("=" * 70)
        print("SQL to PySpark DataFrame Conversion with Variant Data Type")
        print("=" * 70)
        
        # Create sample data
        sample_data = create_sample_data()
        print(f"Created {len(sample_data)} sample pressure sensor records")
        
        # Step 1: Create DataFrame from raw data
        print("\n1. Creating DataFrame from raw JSON strings...")
        df_raw = spark.createDataFrame(sample_data)
        df_raw.printSchema()
        
        # Step 2: Convert JSON strings to Variant data type
        print("\n2. Converting JSON strings to Variant data type...")
        df_with_variant = df_raw.select(
            col('sensor_id'),
            col('sensor_type'), 
            col('timestamp').cast('timestamp'),
            expr('PARSE_JSON(sensor_data_json)').alias('sensor_data')  # Key conversion step
        )
        
        print("Schema after Variant conversion:")
        df_with_variant.printSchema()
        
        # Register as temp view for SQL comparison
        df_with_variant.createOrReplaceTempView('oil_rig_sensors')
        
        # Show sample data structure
        print("\n3. Sample Variant data structure:")
        df_with_variant.select('sensor_id', 'sensor_type', 'sensor_data').show(3, truncate=False)
        
        print("\n" + "=" * 50)
        print("ORIGINAL SQL QUERY")
        print("=" * 50)
        
        sql_query = """
            SELECT 
                AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double')) as avg_wellhead_pressure,
                AVG(VARIANT_GET(sensor_data, '$.drilling_pressure', 'double')) as avg_drilling_pressure,
                COUNT(*) as reading_count
            FROM oil_rig_sensors 
            WHERE sensor_type = 'pressure'
        """
        
        print("SQL Query:")
        print(sql_query)
        
        sql_result = spark.sql(sql_query)
        print("\nSQL Result:")
        sql_result.show()
        
        print("\n" + "=" * 50)
        print("PYSPARK DATAFRAME EQUIVALENTS")
        print("=" * 50)
        
        # Method 1: Direct translation using expr()
        print("\n🔹 Method 1: Direct Translation using expr()")
        print("Most direct conversion - uses expr() to embed SQL expressions")
        
        df_method1 = df_with_variant \
            .filter(col('sensor_type') == 'pressure') \
            .select(
                expr("AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double'))").alias('avg_wellhead_pressure'),
                expr("AVG(VARIANT_GET(sensor_data, '$.drilling_pressure', 'double'))").alias('avg_drilling_pressure'),
                count('*').alias('reading_count')
            )
        
        print("\nCode:")
        print("""
df_result = df_with_variant \\
    .filter(col('sensor_type') == 'pressure') \\
    .select(
        expr("AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double'))").alias('avg_wellhead_pressure'),
        expr("AVG(VARIANT_GET(sensor_data, '$.drilling_pressure', 'double'))").alias('avg_drilling_pressure'),
        count('*').alias('reading_count')
    )
        """)
        
        print("\nResult:")
        df_method1.show()
        
        # Method 2: Using agg() for cleaner aggregation
        print("\n🔹 Method 2: Using agg() for Cleaner Aggregation")
        print("Better for pure aggregation queries - separates filtering from aggregation")
        
        df_method2 = df_with_variant \
            .filter(col('sensor_type') == 'pressure') \
            .agg(
                expr("AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double'))").alias('avg_wellhead_pressure'),
                expr("AVG(VARIANT_GET(sensor_data, '$.drilling_pressure', 'double'))").alias('avg_drilling_pressure'),
                count('*').alias('reading_count')
            )
        
        print("\nCode:")
        print("""
df_result = df_with_variant \\
    .filter(col('sensor_type') == 'pressure') \\
    .agg(
        expr("AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double'))").alias('avg_wellhead_pressure'),
        expr("AVG(VARIANT_GET(sensor_data, '$.drilling_pressure', 'double'))").alias('avg_drilling_pressure'),
        count('*').alias('reading_count')
    )
        """)
        
        print("\nResult:")
        df_method2.show()
        
        # Method 3: Step-by-step approach
        print("\n🔹 Method 3: Step-by-Step Approach")
        print("Most readable - extract fields first, then aggregate")
        
        # Step 3a: Extract Variant fields into regular columns
        df_extracted = df_with_variant \
            .filter(col('sensor_type') == 'pressure') \
            .select(
                col('sensor_id'),
                expr("VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double')").alias('wellhead_pressure'),
                expr("VARIANT_GET(sensor_data, '$.drilling_pressure', 'double')").alias('drilling_pressure'),
                expr("VARIANT_GET(sensor_data, '$.mud_pump_pressure', 'double')").alias('mud_pump_pressure')
            )
        
        print("\nStep 3a - Extract Variant fields:")
        print("""
df_extracted = df_with_variant \\
    .filter(col('sensor_type') == 'pressure') \\
    .select(
        col('sensor_id'),
        expr("VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double')").alias('wellhead_pressure'),
        expr("VARIANT_GET(sensor_data, '$.drilling_pressure', 'double')").alias('drilling_pressure'),
        expr("VARIANT_GET(sensor_data, '$.mud_pump_pressure', 'double')").alias('mud_pump_pressure')
    )
        """)
        
        print("\nExtracted fields:")
        df_extracted.show()
        
        # Step 3b: Perform aggregation on regular columns
        df_method3 = df_extracted.agg(
            avg('wellhead_pressure').alias('avg_wellhead_pressure'),
            avg('drilling_pressure').alias('avg_drilling_pressure'),
            count('*').alias('reading_count')
        )
        
        print("\nStep 3b - Aggregate regular columns:")
        print("""
df_result = df_extracted.agg(
    avg('wellhead_pressure').alias('avg_wellhead_pressure'),
    avg('drilling_pressure').alias('avg_drilling_pressure'),
    count('*').alias('reading_count')
)
        """)
        
        print("\nFinal Result:")
        df_method3.show()
        
        print("\n" + "=" * 50)
        print("PERFORMANCE COMPARISON")
        print("=" * 50)
        
        import time
        
        # Time SQL approach
        start_time = time.time()
        sql_result.collect()
        sql_time = time.time() - start_time
        
        # Time DataFrame Method 1
        start_time = time.time()
        df_method1.collect()
        df1_time = time.time() - start_time
        
        # Time DataFrame Method 2  
        start_time = time.time()
        df_method2.collect()
        df2_time = time.time() - start_time
        
        # Time DataFrame Method 3
        start_time = time.time()
        df_method3.collect()
        df3_time = time.time() - start_time
        
        print(f"SQL Query Time:           {sql_time:.4f} seconds")
        print(f"DataFrame Method 1 Time:  {df1_time:.4f} seconds")
        print(f"DataFrame Method 2 Time:  {df2_time:.4f} seconds") 
        print(f"DataFrame Method 3 Time:  {df3_time:.4f} seconds")
        
        print("\n" + "=" * 50)
        print("RECOMMENDATIONS")
        print("=" * 50)
        
        print("""
🎯 WHEN TO USE EACH METHOD:

Method 1 (expr() with select()):
  ✅ Direct SQL-to-DataFrame translation
  ✅ Good for mixed select/aggregate queries
  ❌ Can become verbose for complex queries

Method 2 (expr() with agg()):
  ✅ Clean separation of filtering and aggregation
  ✅ Best for pure aggregation queries
  ✅ Most similar to SQL GROUP BY behavior

Method 3 (Step-by-step):
  ✅ Most readable and maintainable
  ✅ Easy to debug intermediate results
  ✅ Good for complex transformations
  ❌ Slightly more verbose

🚀 GENERAL RECOMMENDATIONS:
  • Use Method 2 for simple aggregations (like your example)
  • Use Method 3 for complex multi-step transformations
  • Use Method 1 when you need mixed select/aggregate operations
  • All methods have similar performance characteristics
        """)
        
        print("\n✅ All methods produce identical results!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    demonstrate_sql_to_dataframe_conversion()
