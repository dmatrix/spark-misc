"""
Security Log Analysis with Spark 4.0 Variant Data Type
======================================================

This use case demonstrates processing heterogeneous security logs from various
security tools (EDR, SIEM, Firewall, IDS) using the Variant data type for 
flexible threat detection and analysis.
"""

import json
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

import time
import hashlib
import ipaddress

def create_spark_session():
    """Create Spark session with Variant support"""
    return SparkSession.builder \
        .appName("Security Log Analysis with Variant") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

# Security data constants
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

def generate_random_ip():
    """Generate random IP address"""
    return str(ipaddress.IPv4Address(random.randint(1, 4294967294)))

def generate_file_hash():
    """Generate realistic file hash"""
    random_string = f"file_{random.randint(100000, 999999)}"
    return {
        "md5": hashlib.md5(random_string.encode()).hexdigest(),
        "sha256": hashlib.sha256(random_string.encode()).hexdigest(),
        "sha1": hashlib.sha1(random_string.encode()).hexdigest()[:40]
    }

def generate_antivirus_event(event_id, timestamp):
    """Generate antivirus/EDR security event (simplified)"""
    return {
        "threat_type": random.choice(THREAT_TYPES),
        "action_taken": random.choice(ACTION_TAKEN),
        "detection_score": round(random.uniform(0.1, 1.0), 3)
    }

def generate_firewall_event(event_id, timestamp):
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

def generate_ids_event(event_id, timestamp):
    """Generate IDS/IPS security event (simplified)"""
    attack_type = random.choice(ATTACK_TYPES)
    return {
        "attack_type": attack_type,
        "source_ip": generate_random_ip(),
        "user_agent": random.choice(USER_AGENTS)
    }

def self_generate_attack_payload(attack_type):
    """Generate realistic attack payloads"""
    payloads = {
        "sql_injection": [
            "1' OR '1'='1", "'; DROP TABLE users; --", "UNION SELECT * FROM passwords",
            "1' AND 1=1 --", "' OR 1=1 LIMIT 1 --"
        ],
        "xss": [
            "<script>alert('XSS')</script>", "javascript:alert('XSS')",
            "<img src=x onerror=alert('XSS')>", "<svg onload=alert('XSS')>"
        ],
        "command_injection": [
            "; cat /etc/passwd", "| whoami", "&& ls -la", "; rm -rf /"
        ],
        "directory_traversal": [
            "../../../etc/passwd", "..\\..\\..\\windows\\system32\\config\\sam",
            "....//....//....//etc/passwd"
        ]
    }
    return random.choice(payloads.get(attack_type, ["malicious_payload"]))



def generate_fake_security_data(num_records=60000):
    """Generate large dataset of security events"""
    print(f"Generating {num_records} security event records...")
    
    event_types = [
        ("firewall", generate_firewall_event, 0.4),     # 40% of events
        ("antivirus", generate_antivirus_event, 0.35),  # 35% of events
        ("ids", generate_ids_event, 0.25)               # 25% of events
    ]
    
    # Create weighted list for realistic distribution
    weighted_events = []
    for event_type, generator, weight in event_types:
        weighted_events.extend([(event_type, generator)] * int(weight * 100))
    
    data = []
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    
    for i in range(num_records):
        # Generate timestamp with realistic patterns (more attacks during certain hours)
        hour_weight = random.choices(
            range(24), 
            weights=[3, 2, 1, 1, 2, 3, 5, 8, 10, 12, 15, 16, 16, 15, 14, 16, 18, 16, 12, 8, 6, 4, 3, 2]
        )[0]
        
        timestamp = start_time + timedelta(
            days=random.randint(0, 30),
            hours=hour_weight,
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
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

def assign_severity(event_type, event_data):
    """Assign severity based on event type and data"""
    if event_type == "antivirus":
        if event_data.get("threat_type") in ["ransomware", "rootkit"]:
            return "critical"
        elif event_data.get("detection_score", 0) > 0.8:
            return "high"
        else:
            return "medium"
    elif event_type == "firewall":
        if event_data.get("geo_location", {}).get("is_tor") or event_data.get("geo_location", {}).get("source_country") in ["CN", "RU"]:
            return "high"
        elif event_data.get("action") == "blocked":
            return "medium"
        else:
            return "low"
    elif event_type == "ids":
        rule_count = len(event_data.get("detection_rules", []))
        if rule_count > 2 or event_data.get("attack_type") in ["sql_injection", "command_injection"]:
            return "critical"
        elif rule_count > 1:
            return "high"
        else:
            return "medium"
    elif event_type == "siem_correlation":
        return event_data.get("risk_level", "medium")
    
    return "medium"

def run_security_analysis():
    """Run the security log analysis"""
    print("=" * 60)
    print("Security Log Analysis with Spark 4.0 Variant")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Generate fake data
        start_time = time.time()
        fake_data = generate_fake_security_data(60000)
        print(f"Data generation completed in {time.time() - start_time:.2f} seconds")
        
        # Create DataFrame
        print("\nCreating DataFrame...")
        df = spark.createDataFrame(fake_data)
        
        # Create temp table and convert JSON to Variant
        df.createOrReplaceTempView("security_raw")
        
        print("Converting JSON strings to Variant data type...")
        security_df = spark.sql("""
            SELECT 
                event_id,
                source_system,
                severity,
                CAST(timestamp AS TIMESTAMP) as timestamp,
                PARSE_JSON(event_details_json) as event_details
            FROM security_raw
        """)
        
        security_df.createOrReplaceTempView("security_events")
        print(f"Created security dataset with {security_df.count()} records")
        
        # Analysis 1: Security event overview
        print("\n" + "="*50)
        print("ANALYSIS 1: Security Event Overview")
        print("="*50)
        
        event_overview = spark.sql("""
            SELECT 
                source_system,
                severity,
                COUNT(*) as event_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM security_events
            GROUP BY source_system, severity
            ORDER BY source_system, 
                     CASE severity 
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
        spark.stop()

if __name__ == "__main__":
    run_security_analysis()