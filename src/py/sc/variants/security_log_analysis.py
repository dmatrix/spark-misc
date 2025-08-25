"""
Security Log Analysis with Spark 4.0 Variant Data Type
======================================================

This use case demonstrates processing heterogeneous security logs from various
security tools (Firewall, Antivirus/EDR, IDS) using the Variant data type for 
flexible threat detection and analysis.

Key Features:
- 3 security systems: firewall (with nested geo-location), antivirus, IDS
- Realistic threat patterns with severity assignment
- CTE-optimized queries for performance and clarity
- Cross-system threat correlation analysis
- Uses shared data_utility module

Performance Optimizations:
- CTE-based event distribution calculations
- Eliminates window function warnings and single-partition processing
- Optimized for security event processing

Data Structure:
- Firewall events: source_ip, action, nested geo_location (source_country, dest_country, confidence)
- Antivirus events: threat_type, action_taken, detection_score
- IDS events: attack_type, source_ip, user_agent

Security Analytics:
- Geographic threat distribution analysis
- Multi-system threat correlation
- Severity-based event classification
- Cross-system IP intelligence gathering

Demonstrates Variant's capability to unify diverse security log formats
while enabling threat detection and analysis workflows.

Authors: Jules S. Damji & Cursor AI
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, parse_json
from data_utility import generate_security_data, create_spark_session

def run_security_analysis(spark: SparkSession) -> None:
    """Run the security log analysis.
    
    Args:
        spark (SparkSession): Pre-configured Spark session to use for analysis
    
    Generates 60,000 security event records from multiple sources (firewall, antivirus, IDS),
    converts JSON to Variant using DataFrame API, and performs comprehensive threat analysis including:
    - Security event distribution overview with CTE-optimized calculations
    - Geographic threat distribution analysis (source countries)
    - Multi-system threat correlation (IPs triggering multiple systems)
    - Critical threat detection (high-severity events)
    - Cross-system IP intelligence gathering
    
    All queries use CTE patterns to eliminate Spark window function warnings
    and maintain distributed processing for security event correlation.
    
    Returns:
        None
    """
    print("=" * 60)
    print("Security Log Analysis with Spark 4.0 Variant")
    print("=" * 60)
    
    try:
        # Generate fake data
        start_time = time.time()
        fake_data = generate_security_data(60000)
        print(f"Data generation completed in {time.time() - start_time:.2f} seconds")
        
        # Create DataFrame
        print("\nCreating DataFrame...")
        df = spark.createDataFrame(fake_data)
        
        # Convert JSON strings to Variant data type using DataFrame API
        print("Converting JSON strings to Variant data type...")
        security_df = df.select(
            col('event_id'),
            col('source_system'),
            col('severity'),
            col('timestamp').cast('timestamp'),
            # Using DataFrame API: parse_json() is available as a native function
            parse_json(col('event_details_json')).alias('event_details')
        )
        
        security_df.createOrReplaceTempView("security_events")
        print(f"Created security dataset with {security_df.count()} records")
        
        # Analysis 1: Security event overview
        print("\n" + "="*50)
        print("ANALYSIS 1: Security Event Overview")
        print("="*50)
        
        event_overview = spark.sql("""
            WITH event_totals AS (
                SELECT 
                    source_system,
                    severity,
                    COUNT(*) as event_count
                FROM security_events
                GROUP BY source_system, severity
            ),
            total_events AS (
                SELECT SUM(event_count) as total_count FROM event_totals
            )
            SELECT 
                et.source_system,
                et.severity,
                et.event_count,
                ROUND(et.event_count * 100.0 / te.total_count, 2) as percentage
            FROM event_totals et
            CROSS JOIN total_events te
            ORDER BY et.source_system, 
                     CASE et.severity 
                         WHEN 'critical' THEN 1 
                         WHEN 'high' THEN 2 
                         WHEN 'medium' THEN 3 
                         ELSE 4 
                     END
        """)
        
        print("Security Event Distribution by System and Severity:")
        event_overview.show(20, truncate=False)
        
        # Analysis 2: Threat Analysis (Antivirus Events)
        print("\n" + "="*50)
        print("ANALYSIS 2: Threat Analysis (Antivirus Events)")
        print("="*50)
        
        # Using SQL for VARIANT_GET: No native DataFrame API equivalent exists
        # This demonstrates mixed API usage - DataFrame for parse_json(), SQL for VARIANT_GET()
        threat_analysis = spark.sql("""
            SELECT 
                VARIANT_GET(event_details, '$.threat_type', 'string') as threat_type,
                VARIANT_GET(event_details, '$.action_taken', 'string') as action_taken,
                AVG(VARIANT_GET(event_details, '$.detection_score', 'double')) as avg_detection_score,
                COUNT(*) as incident_count
            FROM security_events
            WHERE source_system = 'antivirus'
            GROUP BY VARIANT_GET(event_details, '$.threat_type', 'string'), VARIANT_GET(event_details, '$.action_taken', 'string')
            ORDER BY incident_count DESC
        """)
        
        print("Antivirus Threats by Type and Action:")
        threat_analysis.show(truncate=False)
        
        # Analysis 3: Geographic threat distribution
        print("\n" + "="*50)
        print("ANALYSIS 3: Geographic Threat Distribution")
        print("="*50)
        
        geo_analysis = spark.sql("""
            SELECT 
                VARIANT_GET(event_details, '$.geo_location.source_country', 'string') as source_country,
                COUNT(*) as attack_count,
                COUNT(DISTINCT VARIANT_GET(event_details, '$.source_ip', 'string')) as unique_ips
            FROM security_events
            WHERE VARIANT_GET(event_details, '$.geo_location.source_country', 'string') IS NOT NULL
            GROUP BY VARIANT_GET(event_details, '$.geo_location.source_country', 'string')
            ORDER BY attack_count DESC
            LIMIT 8
        """)
        
        print("Attack Distribution by Source Country:")
        geo_analysis.show(10, truncate=False)
        
        # Analysis 4: Firewall Action Analysis
        print("\n" + "="*50)
        print("ANALYSIS 4: Firewall Action Analysis")
        print("="*50)
        
        firewall_analysis = spark.sql("""
            SELECT 
                VARIANT_GET(event_details, '$.action', 'string') as firewall_action,
                COUNT(*) as action_count,
                COUNT(DISTINCT VARIANT_GET(event_details, '$.source_ip', 'string')) as unique_source_ips
            FROM security_events
            WHERE source_system = 'firewall'
            GROUP BY VARIANT_GET(event_details, '$.action', 'string')
            ORDER BY action_count DESC
        """)
        
        print("Firewall Actions and Source IP Activity:")
        firewall_analysis.show(truncate=False)
        
        # Analysis 5: IDS Attack Type Analysis
        print("\n" + "="*50)
        print("ANALYSIS 5: IDS Attack Type Analysis")
        print("="*50)
        
        ids_analysis = spark.sql("""
            SELECT 
                VARIANT_GET(event_details, '$.attack_type', 'string') as attack_type,
                VARIANT_GET(event_details, '$.user_agent', 'string') as user_agent,
                COUNT(*) as attack_count,
                COUNT(DISTINCT VARIANT_GET(event_details, '$.source_ip', 'string')) as unique_source_ips
            FROM security_events
            WHERE source_system = 'ids'
            GROUP BY VARIANT_GET(event_details, '$.attack_type', 'string'), VARIANT_GET(event_details, '$.user_agent', 'string')
            ORDER BY attack_count DESC
        """)
        
        print("IDS Attacks by Type and User Agent:")
        ids_analysis.show(truncate=False)
        
        # Analysis 6: Severity analysis
        print("\n" + "="*50)
        print("ANALYSIS 6: Severity Analysis")
        print("="*50)
        
        severity_analysis = spark.sql("""
            SELECT 
                severity,
                source_system,
                COUNT(*) as incident_count
            FROM security_events
            GROUP BY severity, source_system
            ORDER BY incident_count DESC
            LIMIT 10
        """)
        
        print("Security Events by Severity and System:")
        severity_analysis.show(10, truncate=False)
        
        # Analysis 7: Source IP Cross-System Analysis
        print("\n" + "="*50)
        print("ANALYSIS 7: Source IP Cross-System Analysis")
        print("="*50)
        
        cross_system_analysis = spark.sql("""
            SELECT 
                VARIANT_GET(event_details, '$.source_ip', 'string') as source_ip,
                COUNT(*) as event_count,
                COUNT(DISTINCT source_system) as systems_triggered
            FROM security_events
            WHERE VARIANT_GET(event_details, '$.source_ip', 'string') IS NOT NULL
            GROUP BY VARIANT_GET(event_details, '$.source_ip', 'string')
            HAVING COUNT(DISTINCT source_system) > 1
            ORDER BY event_count DESC
            LIMIT 10
        """)
        
        print("Source IPs Triggering Multiple Security Systems:")
        cross_system_analysis.show(truncate=False)
        
        # Performance demonstration
        print("\n" + "="*50)
        print("PERFORMANCE DEMONSTRATION")
        print("="*50)
        
        # Complex security query using Variant
        start_time = time.time()
        high_severity_query = spark.sql("""
            SELECT COUNT(*) as high_risk_events
            FROM security_events 
            WHERE severity IN ('high', 'critical')
              AND VARIANT_GET(event_details, '$.threat_type', 'string') IS NOT NULL
        """).collect()[0]['high_risk_events']
        variant_time = time.time() - start_time
        
        print(f"Variant query time: {variant_time:.3f} seconds")
        print(f"High-risk security events identified: {high_severity_query}")
        
        # Show schema
        print("\n" + "="*50)
        print("DATASET SCHEMA")
        print("="*50)
        security_df.printSchema()
        
        print(f"\nSecurity analysis completed successfully!")
        print(f"Dataset size: {security_df.count()} records")
        print(f"Simplified security event structure with key insights:")
        print(f"- Firewall events: source_ip, action, geo_location (nested)")
        print(f"- Antivirus events: threat_type, action_taken, detection_score")
        print(f"- IDS events: attack_type, source_ip, user_agent")
        print(f"- Nested geo_location structure demonstrates Variant flexibility")
        
    finally:
        # Don't stop the spark session here since it's managed by the caller
        pass

if __name__ == "__main__":
    spark = create_spark_session("Security Log Analysis with Variant")
    try:
        run_security_analysis(spark)
    finally:
        spark.stop()