# Apache Spark 4.0 Variant Data Type Use Cases

This repository demonstrates three comprehensive use cases for the new **Variant data type** in Apache Spark 4.0, showcasing how it handles semi-structured data with superior flexibility and performance compared to traditional JSON string processing.

## 🚀 What is the Variant Data Type?

The Variant data type in Apache Spark 4.0 is designed for efficient storage and processing of semi-structured data like JSON. Key benefits include:

- **8x faster performance** compared to JSON string processing
- **No schema enforcement** - handle evolving data structures naturally
- **Direct SQL querying** of nested fields without parsing overhead
- **Efficient binary encoding** for optimized storage and retrieval
- **Open source standard** - no vendor lock-in

## 📁 Files Overview

| File | Description | Dataset Size |
|------|-------------|--------------|
| `iot_sensor_processing.py` | **10 critical offshore oil rig sensors**: pressure, flow, gas detection, temperature, vibration, position, weather, level, current, oil spill detection | 50,000 records |
| `ecommerce_event_analytics.py` | E-commerce user behavior events | 75,000 records |
| `security_log_analysis.py` | Security logs from EDR, Firewall, IDS, SIEM | 60,000 records |
| `run_variant_usecase.py` | Main runner script for all use cases | - |

### 🛢️ Offshore Oil Rig - 10 Critical Sensors (Simplified)

The oil rig use case focuses on the **10 most important sensors** with **3 key measurements each** for safe and efficient offshore drilling operations:

1. **Pressure Sensors** - `wellhead_pressure`, `drilling_pressure`, `mud_pump_pressure` (PSI)
2. **Flow Meters** - `mud_flow_rate` (gpm), `oil_flow_rate` (bph), `gas_flow_rate` (cfh)
3. **Gas Detection** - `h2s_concentration` (ppm), `methane_concentration` (ppm), `oxygen_level` (%)
4. **Temperature Sensors** - `equipment_temperature`, `ambient_temperature`, `sea_water_temperature` (°C)
5. **Vibration Sensors** - `overall_vibration`, `x_axis`, `y_axis` (mm/s RMS)
6. **Position Sensors** - `drill_bit_depth` (m), `hook_load` (tons), `rotary_position` (degrees)
7. **Weather Sensors** - `wind_speed` (knots), `wave_height` (m), `barometric_pressure` (mbar)
8. **Level Sensors** - `fluid_level` (%), `volume` (liters), `tank_type` (category)
9. **Current Sensors** - `current_speed` (knots), `current_direction` (degrees), `water_temperature` (°C)
10. **Oil Spill Detection** - `oil_detected` (boolean), `spill_thickness` (mm), `detection_confidence` (0-1)

**Simplified Design**: Each sensor type now has exactly **3 core measurements** for cleaner analytics and easier blog content demonstrations.

## 🛠 Prerequisites

- **Apache Spark 4.0+** with Variant data type support
- **Python 3.8+**
- **PySpark** compatible with Spark 4.0
- Sufficient memory for processing large datasets (recommend 8GB+ RAM)

## 🏃‍♂️ Quick Start

### Install Dependencies
```bash
# Install PySpark 4.0 (when available)
pip install pyspark>=4.0.0

# For preview/development versions:
pip install pyspark==4.0.0.dev0
```

### Run All Use Cases
```bash
python run_variant_usecase.py
```

### Run Individual Use Cases
```bash
# Offshore Oil Rig Sensor Processing
python run_variant_usecase.py iot

# E-commerce Event Analytics
python run_variant_usecase.py ecommerce

# Security Log Analysis
python run_variant_usecase.py security

# Show help information
python run_variant_usecase.py --help

# Show detailed use case information
python run_variant_usecase.py --info
```

### Run Individual Files Directly
```bash
python iot_sensor_processing.py
python ecommerce_event_analytics.py
python security_log_analysis.py
```

## 📋 Complete Usage Guide

The `run_variant_usecase.py` script provides a comprehensive interface for running and exploring the Variant data type demonstrations:

### Command Line Options

```bash
# Show help and available options
python run_variant_usecase.py --help

# Output:
# usage: run_variant_usecase.py [-h] [--info] [{iot,ecommerce,security,all}]
# 
# Apache Spark 4.0 Variant Data Type Use Cases
# 
# positional arguments:
#   {iot,ecommerce,security,all}  Use case to run: iot, ecommerce, security, or all (default: all)
# 
# options:
#   -h, --help            show this help message and exit
#   --info                Show detailed information about all use cases
```

### Detailed Information

```bash
# Show comprehensive details about all use cases
python run_variant_usecase.py --info

# This displays:
# - Complete descriptions of each use case
# - Dataset sizes and key features
# - Benefits of the Variant data type
```

### Execution Examples

```bash
# Default: Run all use cases sequentially
python run_variant_usecase.py
# or explicitly:
python run_variant_usecase.py all

# Run specific use cases by name
python run_variant_usecase.py iot         # ~7-8 seconds execution
python run_variant_usecase.py ecommerce   # ~9-10 seconds execution  
python run_variant_usecase.py security    # ~9-10 seconds execution
```

### Expected Output Structure

Each use case provides:
- **Dependency verification**: Checks PySpark 4.0 and Variant support
- **Data generation timing**: Synthetic data creation performance
- **Multiple analytics**: 5-7 different analysis queries per use case
- **Performance metrics**: Query execution times using Variant
- **Dataset schema**: Shows the Variant column structure
- **Summary statistics**: Key insights and record counts

## 🔬 Use Case Details

### 1. Offshore Oil Rig Sensor Processing (`iot_sensor_processing.py`)

**Scenario**: Offshore oil rig monitoring system collecting data from 10 critical sensor types for safe drilling operations.

**10 Critical Sensor Types** (3 key measurements each):
- **Pressure sensors**: `wellhead_pressure`, `drilling_pressure`, `mud_pump_pressure`
- **Flow meters**: `mud_flow_rate`, `oil_flow_rate`, `gas_flow_rate`
- **Gas detection**: `h2s_concentration`, `methane_concentration`, `oxygen_level`
- **Temperature sensors**: `equipment_temperature`, `ambient_temperature`, `sea_water_temperature`
- **Vibration sensors**: `overall_vibration`, `x_axis`, `y_axis`
- **Position sensors**: `drill_bit_depth`, `hook_load`, `rotary_position`
- **Weather sensors**: `wind_speed`, `wave_height`, `barometric_pressure`
- **Level sensors**: `fluid_level`, `volume`, `tank_type`
- **Current sensors**: `current_speed`, `current_direction`, `water_temperature`
- **Oil spill detection**: `oil_detected`, `spill_thickness`, `detection_confidence`

**Key Analytics**:
- Average pressure analysis across all wellhead, drilling, and mud pump systems
- Average flow rate analysis for mud, oil, and gas production
- Gas concentration safety analysis for H2S, methane, and oxygen levels
- Weather and environmental monitoring of wind, waves, and barometric pressure
- Multi-sensor operational summary with safety threshold analysis

**Variant Benefits Demonstrated**:
- Flexible schema for 10 different sensor types with varying data structures
- Efficient VARIANT_GET queries for nested sensor data
- Fast performance analysis across heterogeneous sensor readings

### 2. E-commerce Event Analytics (`ecommerce_event_analytics.py`)

**Scenario**: E-commerce platform tracking simplified user interaction events (3 key measurements each).

**3 Key Event Types** (simplified structure):
- **Purchase events**: `total_amount`, `customer_type`, `payment` (nested - method, processor, card_type)
- **Search events**: `search_query`, `results_count`, `results_clicked`
- **Wishlist events**: `product_id`, `action`, `product_price`

**Key Analytics**:
- Event distribution overview across the three event types
- Purchase behavior analysis by customer type and spending
- Search behavior analysis with top queries and effectiveness
- Wishlist behavior analysis by action type and product pricing
- User behavior patterns with spending analysis
- Payment method analysis demonstrating nested Variant structure
- Performance demonstration with high-value purchase queries

**Variant Benefits Demonstrated**:
- Flexible handling of 3 different event schemas with varying complexity
- **Nested payment structure** in purchase events shows Variant's nested data capabilities
- Efficient aggregation across heterogeneous event types
- Blog-ready simplified queries perfect for educational content

### 3. Security Log Analysis (`security_log_analysis.py`)

**Scenario**: Cybersecurity platform analyzing simplified logs from 3 key security systems (3 measurements each).

**3 Key Security Sources** (simplified structure):
- **Firewall events**: `source_ip`, `action`, `geo_location` (nested - source_country, dest_country, confidence)
- **Antivirus events**: `threat_type`, `action_taken`, `detection_score`
- **IDS events**: `attack_type`, `source_ip`, `user_agent`

**Key Analytics**:
- Security event overview by system and severity levels
- Antivirus threat analysis by type and remediation action
- Geographic threat distribution analysis demonstrating nested Variant structure
- Firewall action analysis by blocked/allowed/dropped traffic
- IDS attack type analysis by user agent patterns
- Cross-system source IP correlation analysis
- Security event severity distribution across all systems

**User Agent Focus** (IDS Events):
- **Chrome/91.0.4472.124**: Most common browser in attack traffic
- **Firefox/89.0**: Second most frequent in intrusion attempts
- **Safari/14.1.1**: Third most common in web-based attacks

**Variant Benefits Demonstrated**:
- Flexible handling of 3 different security log schemas with varying complexity
- **Nested geo_location structure** in firewall events shows Variant's nested data capabilities
- Efficient correlation across heterogeneous security event types
- Blog-ready simplified queries perfect for security analytics tutorials

## 📊 Performance Characteristics

Each use case demonstrates the performance advantages of Variant:

- **Data Generation**: 50-75K records in under 1 second
- **Data Processing**: Complete analytics suite in 7-10 seconds per use case
- **Query Performance**: Individual VARIANT_GET queries in 0.1-0.2 seconds
- **Memory Efficiency**: Optimized binary encoding reduces memory footprint
- **Schema Flexibility**: No predefined schemas needed for varying data structures

### Actual Performance Metrics
- **IoT (Oil Rig)**: 50K records, 7.5s total execution, 0.18s query time
- **E-commerce**: 75K records, 9.1s total execution, 0.10s query time  
- **Security**: 60K records, 9.5s total execution, 0.11s query time

## 📝 Short SQL Queries

All SQL queries have been simplified for educational and blog content purposes:

- **Concise**: 3-8 lines per query instead of complex CTEs
- **Clear**: Single-purpose queries with obvious intent
- **VARIANT-focused**: Direct demonstration of `VARIANT_GET()` syntax
- **Practical**: Real-world scenarios with meaningful results

### Example VARIANT Query
```sql
-- Simple, clear Variant data access (simplified structure)
SELECT 
    AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double')) as avg_wellhead_pressure,
    AVG(VARIANT_GET(sensor_data, '$.drilling_pressure', 'double')) as avg_drilling_pressure,
    AVG(VARIANT_GET(sensor_data, '$.mud_pump_pressure', 'double')) as avg_mud_pump_pressure,
    COUNT(*) as reading_count
FROM oil_rig_sensors 
WHERE sensor_type = 'pressure'
```

## 🔍 Sample Output

### IoT Sensor Analysis
```
ANALYSIS 1: Pressure Monitoring Analytics
Average Pressure Analysis Across All Sensors:
+---------------------+---------------------+---------------------+-------------+
|avg_wellhead_pressure|avg_drilling_pressure|avg_mud_pump_pressure|reading_count|
+---------------------+---------------------+---------------------+-------------+
|2261.39              |8440.13              |2990.71              |4906         |
+---------------------+---------------------+---------------------+-------------+
```

### E-commerce Event Analysis
```
ANALYSIS 1: Event Distribution Overview
Event Distribution:
+----------+-----------+------------+----------+
|event_type|event_count|unique_users|percentage|
+----------+-----------+------------+----------+
|  purchase|      30141|        9543|     40.19|
|    search|      26196|        9264|     34.93|
|  wishlist|      18663|        8447|     24.88|
+----------+-----------+------------+----------+
```

### Security Log Analysis
```
ANALYSIS 1: Security Event Overview
Security Event Distribution by System and Severity:
+-------------+--------+-----------+----------+
|source_system|severity|event_count|percentage|
+-------------+--------+-----------+----------+
|antivirus    |critical|5233       |8.72      |
|antivirus    |high    |3518       |5.86      |
|antivirus    |medium  |12256      |20.43     |
|firewall     |high    |4246       |7.08      |
|firewall     |medium  |6547       |10.91     |
|firewall     |low     |13042      |21.74     |
|ids          |critical|4328       |7.21      |
|ids          |medium  |10830      |18.05     |
+-------------+--------+-----------+----------+
```

## 🎯 Key Takeaways

1. **Schema Flexibility**: Handle evolving data structures without breaking existing queries
2. **Performance**: Significant speed improvements over JSON string processing
3. **Unified Analytics**: Single data type for diverse semi-structured data sources
4. **Production Ready**: Open source standard suitable for enterprise deployments

## 🔧 Customization

Each use case can be customized by modifying:

- **Data volume**: Change `num_records` parameters in generation functions
- **Data complexity**: Adjust nested structure depth and variety
- **Analysis queries**: Modify SQL queries to focus on specific business questions
- **Performance testing**: Add timing measurements for specific operations

## 📈 Monitoring and Optimization

The use cases include built-in performance monitoring:

- Data generation timing
- Query execution timing
- Memory usage patterns
- Dataset size reporting

## 🤝 Contributing

To extend these use cases:

1. Add new sensor types to IoT use case
2. Include additional event types for e-commerce
3. Integrate more security tools in security analysis
4. Create hybrid use cases combining multiple domains

## 📝 License

This demonstration code is provided for educational and evaluation purposes. Please refer to Apache Spark licensing for production usage.

---

**Note**: This demonstration requires Apache Spark 4.0 with Variant data type support. For earlier versions of Spark, the Variant-specific functionality will not be available, but the data generation and basic analytics can still provide valuable insights into semi-structured data processing patterns.