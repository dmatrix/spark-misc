# Apache Spark 4.0 Variant Data Type Tutorial

**Learn to Create, Process, and Analyze Semi-Structured Data with Variant**

This comprehensive tutorial teaches you how to work with Apache Spark 4.0's **Variant data type** through three real-world use cases. You'll learn both **PySpark DataFrame** and **SQL** approaches for handling semi-structured JSON data efficiently.

## What You'll Learn

This tutorial covers everything you need to master Variant data type:

### 📚 **Core Concepts**
- What is the Variant data type and when to use it
- Converting JSON strings to Variant format
- Querying nested data with `VARIANT_GET()`
- Schema flexibility for evolving data structures

### 🛠️ **Practical Skills**
- **SQL Approach**: Writing Variant queries with Spark SQL
- **DataFrame Approach**: Using PySpark DataFrame operations
- **Data Creation**: Generating realistic semi-structured data
- **Performance**: Optimizing queries with CTEs

### 🎯 **Real-World Applications**
- **Oil Rig IoT**: Sensor data with 10 different measurement types
- **E-commerce**: User events with nested payment structures  
- **Security Logs**: Multi-system events with geo-location data

## Architecture

This project follows a modular architecture:

```
variants/
├── data_utility.py              # Shared data generation utilities (442 lines)
├── iot_sensor_processing.py     # Oil rig sensor analytics (200 lines)
├── ecommerce_event_analytics.py # E-commerce event analytics (230 lines)
├── security_log_analysis.py     # Security log analytics (238 lines)
├── sample_data_generator.py     # Configurable data generator (193 lines)
├── sql_to_dataframe_example.py  # SQL to DataFrame conversion guide

├── run_variant_usecase.py       # Main test runner
└── test_environment.py          # Environment validation
```

### Shared Data Utility Module

The `data_utility.py` module centralizes all data generation logic:

- Common constants (threat types, payment methods, countries, etc.)
- Helper functions (IP generation, file hashing, timestamp patterns)
- Oil rig generators (10 sensor types with 3 measurements each)
- E-commerce generators (purchase, search, wishlist events)
- Security generators (firewall, antivirus, IDS events)
- Bulk data generation functions for all use cases
- Comprehensive oil rig data (all 10 sensors in single JSON record)

## Tutorial Structure

| Tutorial Module | File | Learning Focus | Dataset Size | Key Techniques |
|-----------------|------|----------------|--------------|----------------|
| **Module 1: IoT Sensors** | `iot_sensor_processing.py` | Basic Variant operations with 10 sensor types | 50,000 records | `VARIANT_GET()`, aggregations, filtering |
| **Module 2: E-commerce** | `ecommerce_event_analytics.py` | Nested data structures and user analytics | 75,000 records | Nested JSON, payment data, CTEs |
| **Module 3: Security** | `security_log_analysis.py` | Multi-source data correlation | 60,000 records | Geographic data, cross-system analysis |
| **Module 4: SQL ↔ DataFrame** | `sql_to_dataframe_example.py` | Converting between SQL and DataFrame APIs | 10 records | `expr()`, `agg()`, step-by-step patterns |


## Tutorial Modules

### 📊 Module 1: IoT Sensor Data (Beginner)

**Learning Goal**: Master basic Variant operations with structured sensor data

**What You'll Practice**:

- **Creating Variant DataFrames** from JSON strings
- **Basic VARIANT_GET()** syntax for field extraction
- **Aggregations** (AVG, COUNT) on Variant fields
- **Filtering** records based on Variant data

**Key SQL Patterns**:
```sql
-- Convert JSON to Variant
SELECT PARSE_JSON(sensor_data_json) as sensor_data FROM sensors

-- Extract nested values
SELECT VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double') as pressure
FROM oil_rig_sensors

-- Aggregate Variant data
SELECT AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double')) as avg_pressure
FROM oil_rig_sensors WHERE sensor_type = 'pressure'
```

**Key DataFrame Patterns (Mixed API Approach)**:
```python
# Convert to Variant using DataFrame API
from pyspark.sql.functions import col, parse_json, expr
df_variant = df.select("*", parse_json(col("sensor_data_json")).alias("sensor_data"))

# Extract and aggregate using expr() for VARIANT_GET (no DataFrame equivalent)
df_result = df_variant.agg(
    expr("AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double'))").alias('avg_pressure')
)
```

**Why Mixed API?**
- ✅ **parse_json()**: Available as DataFrame function
- ❌ **VARIANT_GET()**: No DataFrame equivalent - use `expr()` with SQL
- 🎯 **Best Practice**: Use DataFrame API where available, `expr()` for SQL-only functions

### 🛒 Module 2: E-commerce Events (Intermediate)

**Learning Goal**: Handle complex nested JSON structures and user behavior analytics

**What You'll Practice**:

- **Deeply nested JSON** with payment structures
- **CTE (Common Table Expression)** optimizations
- **Cross-event analysis** and user segmentation
- **Multiple data types** in single Variant column

**Key SQL Patterns**:
```sql
-- Access nested payment data
SELECT 
    VARIANT_GET(event_data, '$.payment.method', 'string') as payment_method,
    VARIANT_GET(event_data, '$.payment.card_type', 'string') as card_type,
    VARIANT_GET(event_data, '$.total_amount', 'double') as amount
FROM ecommerce_events WHERE event_type = 'purchase'

-- CTE for percentage calculations (avoiding window functions)
WITH event_totals AS (
    SELECT event_type, COUNT(*) as event_count
    FROM user_events GROUP BY event_type
),
total_events AS (
    SELECT SUM(event_count) as total_count FROM event_totals
)
SELECT et.event_type, 
       ROUND(et.event_count * 100.0 / te.total_count, 2) as percentage
FROM event_totals et CROSS JOIN total_events te
```

**Key DataFrame Patterns**:
```python
# Extract nested payment info
df_payments = df_variant.select(
    expr("VARIANT_GET(event_data, '$.payment.method', 'string')").alias('method'),
    expr("VARIANT_GET(event_data, '$.total_amount', 'double')").alias('amount')
).filter(col('method').isNotNull())

# User behavior analysis
df_user_stats = df_variant.groupBy('user_id').agg(
    count('*').alias('total_events'),
    sum(expr("VARIANT_GET(event_data, '$.total_amount', 'double')")).alias('total_spent')
)
```

### 🔒 Module 3: Security Logs (Advanced)

**Learning Goal**: Master multi-source data correlation and geographic analysis

**What You'll Practice**:

- **Heterogeneous data sources** (Firewall, Antivirus, IDS)
- **Geographic data analysis** with nested coordinates
- **Cross-system correlation** and threat intelligence
- **Advanced filtering** and threat classification

**Key SQL Patterns**:
```sql
-- Geographic threat analysis
SELECT 
    VARIANT_GET(event_details, '$.geo_location.source_country', 'string') as country,
    COUNT(*) as attack_count,
    COUNT(DISTINCT VARIANT_GET(event_details, '$.source_ip', 'string')) as unique_ips
FROM security_events 
WHERE source_system = 'firewall'
GROUP BY VARIANT_GET(event_details, '$.geo_location.source_country', 'string')
ORDER BY attack_count DESC

-- Multi-system threat correlation
SELECT source_ip, COUNT(DISTINCT source_system) as systems_triggered
FROM (
    SELECT VARIANT_GET(event_details, '$.source_ip', 'string') as source_ip, source_system
    FROM security_events 
    WHERE VARIANT_GET(event_details, '$.source_ip', 'string') IS NOT NULL
) GROUP BY source_ip HAVING COUNT(DISTINCT source_system) > 1
```

**Key DataFrame Patterns**:
```python
# Geographic analysis
df_geo = df_variant.select(
    expr("VARIANT_GET(event_details, '$.geo_location.source_country', 'string')").alias('country'),
    expr("VARIANT_GET(event_details, '$.source_ip', 'string')").alias('source_ip')
).filter(col('country').isNotNull())

df_country_stats = df_geo.groupBy('country').agg(
    count('*').alias('attack_count'),
    countDistinct('source_ip').alias('unique_ips')
).orderBy(desc('attack_count'))
```

### 🔄 Module 4: SQL ↔ DataFrame Conversion (Expert)

**Learning Goal**: Master converting between SQL and DataFrame approaches

**What You'll Practice**:

- **Three conversion methods**: Direct translation, aggregation-focused, step-by-step
- **Performance considerations** for each approach
- **When to use SQL vs DataFrame** APIs
- **Complex query optimization** techniques

**Example Conversion**:
```sql
-- Original SQL
SELECT AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double')) as avg_pressure,
       COUNT(*) as reading_count
FROM oil_rig_sensors WHERE sensor_type = 'pressure'
```

**Method 1: Direct Translation**
```python
df_result = df_variant.filter(col('sensor_type') == 'pressure').agg(
    expr("AVG(VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double'))").alias('avg_pressure'),
    count('*').alias('reading_count')
)
```

**Method 2: Step-by-Step**
```python
# Extract first, then aggregate
df_extracted = df_variant.filter(col('sensor_type') == 'pressure').select(
    expr("VARIANT_GET(sensor_data, '$.wellhead_pressure', 'double')").alias('pressure')
)
df_result = df_extracted.agg(avg('pressure').alias('avg_pressure'), count('*').alias('reading_count'))
```

## 🛠 Prerequisites

- **Apache Spark 4.0+** with Variant data type support
- **Python 3.8+**
- **PySpark** compatible with Spark 4.0
- Sufficient memory for processing large datasets (recommend 8GB+ RAM)

## 🚀 Tutorial Quick Start

### Step 1: Verify Your Environment
```bash
# Test Spark 4.0 and Variant support
python test_environment.py
```

### Step 2: Start with Module 1 (Beginner)
```bash
# Learn basic Variant operations with IoT sensor data
python run_variant_usecase.py iot
```
**What you'll see**: Basic `VARIANT_GET()` operations, aggregations, and 10 different sensor types

### Step 3: Progress to Module 2 (Intermediate)  
```bash
# Master nested JSON and CTE optimizations
python run_variant_usecase.py ecommerce
```
**What you'll see**: Complex nested payment data, user behavior analysis, CTE patterns

### Step 4: Advance to Module 3 (Advanced)
```bash
# Handle multi-source data correlation
python run_variant_usecase.py security
```
**What you'll see**: Geographic analysis, cross-system correlation, threat intelligence

### Step 5: Master SQL ↔ DataFrame Conversion
```bash
# Learn three conversion methods
python sql_to_dataframe_example.py
```
**What you'll see**: Side-by-side SQL and DataFrame code with performance comparisons

### Step 6: Run Complete Tutorial
```bash
# Execute all modules in sequence
python run_variant_usecase.py all
```

### 4. Generate Sample Data
```bash
# Generate 1 comprehensive record each (default)
python sample_data_generator.py

# Generate 100 comprehensive records each
python sample_data_generator.py --count 100

# Custom counts per use case
python sample_data_generator.py --oil-rig 50 --ecommerce 75 --security 100

# Save to custom directory
python sample_data_generator.py --count 10 --output-dir ./sample_data

# Just show samples without saving
python sample_data_generator.py --no-output --show-samples 5
```

**Note**: Oil rig data now generates **comprehensive records** containing all 10 sensor types in a single JSON structure, perfect for demonstrating Variant's nested data capabilities.

### 5. SQL to DataFrame Conversion
```bash
# Learn how to convert SQL queries to PySpark DataFrame operations
python sql_to_dataframe_example.py
```

## Performance Optimizations

### CTE-Based Query Optimization

All analytical queries use Common Table Expressions (CTEs) instead of window functions to eliminate Spark warnings:

**Before (caused warnings):**
```sql
SELECT event_type, COUNT(*) as count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM events GROUP BY event_type
```

**After (optimized with CTEs):**
```sql
WITH event_totals AS (
    SELECT event_type, COUNT(*) as event_count
    FROM events GROUP BY event_type
),
total_events AS (
    SELECT SUM(event_count) as total_count FROM event_totals
)
SELECT et.event_type, et.event_count,
       ROUND(et.event_count * 100.0 / te.total_count, 2) as percentage
FROM event_totals et CROSS JOIN total_events te
```

**Benefits:**
- Eliminates `WARN WindowExec` messages
- Maintains Spark's parallel processing
- Better performance on large datasets
- More readable and maintainable code

## Sample Output

### Oil Rig Analytics
```
==================================================
ANALYSIS 1: Pressure Monitoring Analytics
==================================================
+---------------------+---------------------+---------------------+
|avg_wellhead_pressure|avg_drilling_pressure|avg_mud_pump_pressure|
+---------------------+---------------------+---------------------+
|2244.5393975663274   |8503.662278077       |3022.542389786555    |
+---------------------+---------------------+---------------------+

Critical operational insights from 10 sensor types:
- High pressure readings: 2,130 (safety threshold exceeded)
- Dangerous H2S levels: 2,515 (>10 ppm detected)
- High wind conditions: 2,017 (>30 knots)
- Oil spill detections: 1,251 (environmental alerts)
```

### E-commerce Analytics
```
==================================================
ANALYSIS 2: Purchase Behavior Analytics
==================================================
+-------------+--------------+------------------+
|customer_type|purchase_count|avg_order_value   |
+-------------+--------------+------------------+
|returning    |7535          |1089.1132329130724|
|vip          |7594          |1079.3609428496181|
|premium      |7290          |1074.8872866941015|
|new          |7766          |1074.745394025238 |
+-------------+--------------+------------------+
```

### Security Analytics
```
==================================================
ANALYSIS 3: Geographic Threat Distribution
==================================================
+--------------+------------+----------+
|source_country|attack_count|unique_ips|
+--------------+------------+----------+
|CN            |2189        |2189      |
|RU            |2180        |2180      |
|Unknown       |2230        |2230      |
+--------------+------------+----------+
```

## 🎓 Tutorial Learning Outcomes

After completing this tutorial, you'll have mastered:

### ✅ **Core Variant Skills**
- Converting JSON strings to Variant format using `PARSE_JSON()`
- Extracting nested data with `VARIANT_GET()` syntax
- Handling schema evolution without breaking queries
- Working with mixed data types in single columns

### ✅ **SQL Mastery**
```sql
-- Schema-flexible queries
SELECT VARIANT_GET(sensor_data, '$.pressure.wellhead_pressure', 'double') as pressure
FROM oil_rig_sensors WHERE sensor_type = 'comprehensive'

-- Complex nested access
SELECT VARIANT_GET(event_data, '$.payment.method', 'string') as payment_method,
       VARIANT_GET(event_data, '$.payment.card_type', 'string') as card_type
FROM ecommerce_events WHERE event_type = 'purchase'
```

### ✅ **DataFrame Expertise (Mixed API Mastery)**
```python
# Mixed API approach: DataFrame + SQL where needed
from pyspark.sql.functions import col, parse_json, expr, count

# Step 1: DataFrame API for JSON-to-Variant conversion
df_variant = df.select(parse_json(col('json_col')).alias('variant_data'))

# Step 2: expr() for VARIANT_GET (no DataFrame equivalent)
df_result = df_variant.filter(col('sensor_type') == 'comprehensive').agg(
    expr("AVG(VARIANT_GET(sensor_data, '$.pressure.wellhead_pressure', 'double'))").alias('avg_pressure'),
    count('*').alias('reading_count')
)
```

### ✅ **Production-Ready Patterns**
- CTE-based query optimization for Spark performance
- Error handling for missing or null Variant fields
- Best practices for nested data structure design
- Monitoring and debugging Variant queries

## Testing

### Environment Validation
```bash
python test_environment.py
```
Validates:
- Python version compatibility (3.8+)
- PySpark installation and version
- Spark session creation
- Variant data type support
- System resources

### Comprehensive Testing
All use cases include:
- Data generation validation
- DataFrame creation and schema verification
- Variant data type operations
- Complex analytical queries
- Performance measurements
- Error handling and cleanup


## Development

### Code Organization
- Modular design with shared utilities
- Comprehensive error handling
- Performance optimizations with CTEs
- Extensive documentation and examples

### Adding New Use Cases
1. Import generators from `data_utility.py`
2. Create analysis functions using Variant operations
3. Add to `run_variant_usecase.py` for integration
4. Follow CTE patterns for optimal performance
5. Use comprehensive data structures for complex nested scenarios
6. Reference `sql_to_dataframe_example.py` for DataFrame conversion patterns

## 📚 Continue Learning

### 🔗 **Official Resources**
- **Apache Spark 4.0 Documentation**: [Variant Data Type Guide](https://spark.apache.org/docs/latest/sql-ref-datatypes.html#variant-data-type)
- **PySpark API Reference**: [DataFrame and SQL Functions](https://spark.apache.org/docs/latest/api/python/)

### 📖 **Deep Dive Tutorial**
- **Complete Developer Guide**: `developer_user_guide.md` - Comprehensive tutorial with advanced patterns, expert-level implementations, and production best practices

### 🛠️ **Practice Exercises**
- Modify the sample data generators to create your own JSON structures
- Experiment with different `VARIANT_GET()` path expressions
- Try converting the provided SQL examples to DataFrame operations
- Build your own CTE-optimized queries for complex analytics

## 🤝 Contributing

1. Follow the DRY principle - use shared utilities
2. Optimize queries with CTEs instead of window functions
3. Add comprehensive tests for new features
4. Update documentation for any changes

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

Start with `python test_environment.py` and then `python run_variant_usecase.py all`.