# PySpark 4.0 Data Quality Strategies

A comprehensive collection of developer-ready data quality strategies for PySpark 4.0, covering ingestion, validation, deduplication, and error handling patterns with extensive testing.

## 📋 Overview

This repository demonstrates **5 core data quality strategies** for developer testing and data pipeline development:

1. **Smart JSON Loading** - Intelligent data ingestion with fallback mechanisms
2. **Schema Enforcement** - Strict data structure validation
3. **Business Logic Validation** - Custom data quality rules
4. **Permissive Mode Handling** - Graceful error recovery
5. **Advanced Deduplication** - Sophisticated duplicate removal with business logic

## 🛠 Prerequisites

```bash
# Required packages
pip install pyspark>=4.0.0
pip install pytest  # For running tests

# Verify PySpark installation
python -c "import pyspark; print(pyspark.__version__)"
```

## 📊 Data Quality Strategies

### 1. Smart JSON Loading (`smart_json_loader.py`)

**Strategy**: Try strict parsing first, fallback to permissive mode for graceful error recovery.

**Use Cases**:
- Mixed quality JSON data sources
- Development and testing of data pipelines
- Batch processing with error tolerance

**Key Features**:
- FAILFAST mode for clean data (optimal performance)
- PERMISSIVE fallback for corrupted data
- Automatic bad record separation

```bash
# Run the example
python smart_json_loader.py

# Test the functionality
python test_all_json_loaders.py
```

**Sample Output**:
```
🧪 TESTING: data/clean_transactions.json
✅ FAILFAST succeeded: 3 clean records

🧪 TESTING: data/mixed_transactions.json  
⚠️ FAILFAST failed, switching to PERMISSIVE mode...
🔄 PERMISSIVE recovery: 2 good, 1 bad records
```

### 2. Schema Enforcement (`schema_enforcement.py`)

**Strategy**: Enforce strict data schemas with detailed error reporting.

**Use Cases**:
- Critical financial/healthcare data
- Regulatory compliance requirements
- Data contract validation

**Key Features**:
- Predefined schema validation
- Detailed error reporting
- Type safety enforcement

```bash
# Run schema enforcement example
python schema_enforcement.py

# Test schema validation
python test_all_json_loaders.py
```

### 3. Business Logic Validation (`business_logic_validation.py`)

**Strategy**: Apply custom business rules beyond basic schema validation.

**Use Cases**:
- Financial transaction validation
- Customer data quality checks
- Domain-specific rules

**Key Features**:
- Custom validation functions
- Business rule chaining
- Detailed violation reporting

```bash
# Run business logic validation
python business_logic_validation.py

# Test business rules
python test_business_logic_validation.py
```

**Example Business Rules**:
- Transaction amounts must be positive and reasonable
- Customer tiers must be valid (GOLD/SILVER/BRONZE)
- Date ranges must be logical

### 4. Permissive Mode Handling (`permissive_mode.py`)

**Strategy**: Handle corrupted records gracefully while preserving data pipeline flow.

**Use Cases**:
- Real-time streaming data
- External data sources with quality issues
- ETL pipelines requiring high uptime

```bash
# Run permissive mode example
python permissive_mode.py
```

### 5. Advanced Deduplication

**Strategy**: Intelligent duplicate removal based on business priorities.

#### Basic Deduplication (`deduplication_snippet.py`)

**Use Cases**:
- Transaction fraud detection
- Customer record consolidation
- System error cleanup

```bash
# Run basic deduplication
python deduplication_snippet.py

# Test deduplication logic
python test_deduplication_example.py
```

#### Advanced Deduplication (Shorter Version)

**File**: `advanced_deduplication_shorter_snippet.py`

**Key Techniques**:
- Keep highest value records
- Keep most recent records  
- Multi-criteria priority ranking
- Aggregation instead of removal

```bash
# Run advanced deduplication (concise version)
python advanced_deduplication_shorter_snippet.py

# Test advanced techniques
python test_advanced_dedup_shorter.py
```

#### Advanced Deduplication (Comprehensive Version)

**File**: `advanced_deduplication_longer_snippet.py`

**Additional Features**:
- Rank vs row_number handling
- Complex priority hierarchies
- Comprehensive tie-breaking rules

```bash
# Run advanced deduplication (full version)
python advanced_deduplication_longer_snippet.py

# Test comprehensive functionality
python test_advanced_dedup_longer.py
```

## 🧪 Running All Tests

### Individual Test Suites

```bash
# Test smart JSON loading strategies
python test_all_json_loaders.py

# Test business logic validation
python test_business_logic_validation.py

# Test basic deduplication
python test_deduplication_example.py

# Test advanced deduplication (shorter)
python test_advanced_dedup_shorter.py

# Test advanced deduplication (longer)
python test_advanced_dedup_longer.py
```

### Run All Tests at Once

```bash
# Use the comprehensive test runner for full reporting
python run_all_tests.py
```

**Sample Output:**
```
🚀 PySpark 4.0 Data Quality Strategies - Test Suite Runner
======================================================================
✅ PASS Smart JSON Loading                  (6.04s)
✅ PASS Business Logic Validation           (7.31s) 
✅ PASS Basic Deduplication                 (9.02s)
✅ PASS Advanced Deduplication (Shorter)    (6.99s)
✅ PASS Advanced Deduplication (Longer)     (7.52s)

🎉 ALL TESTS PASSED! Data quality strategies are developer-ready and tested.
```

## 📈 Performance Considerations

### Spark Configuration for Data Quality

```python
# Recommended Spark settings for data quality pipelines
spark = SparkSession.builder \
    .appName("DataQualityPipeline") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()
```

### Performance Tips

1. **Use FAILFAST first** - 10x faster for clean data
2. **Cache intermediate results** - Avoid recomputation
3. **Partition by validation keys** - Improve parallel processing
4. **Window functions** - More efficient than self-joins for deduplication
5. **Broadcast small lookup tables** - Speed up validation joins

## 🎯 Strategy Selection Guide

| Scenario | Recommended Strategy | File to Use |
|----------|---------------------|-------------|
| **Mixed quality JSON** | Smart JSON Loading | `smart_json_loader.py` |
| **Strict compliance** | Schema Enforcement | `schema_enforcement.py` |
| **Business rules** | Business Logic Validation | `business_logic_validation.py` |
| **Streaming data** | Permissive Mode | `permissive_mode.py` |
| **Simple duplicates** | Basic Deduplication | `deduplication_snippet.py` |
| **Complex duplicates** | Advanced Deduplication | `advanced_deduplication_*_snippet.py` |

## 💡 Real-World Use Cases

### Financial Services
```bash
# Transaction validation pipeline
python business_logic_validation.py  # Validate amounts, dates
python advanced_deduplication_shorter_snippet.py  # Remove duplicate transactions
```

### Customer Data Management
```bash
# Customer record consolidation
python advanced_deduplication_longer_snippet.py  # Keep best customer records
python schema_enforcement.py  # Ensure data consistency
```

### Data Lake Ingestion
```bash
# Handle mixed-quality data sources
python smart_json_loader.py  # Smart loading with fallbacks
python permissive_mode.py  # Handle corrupted records
```

### Real-time Streaming
```bash
# Streaming data quality
python permissive_mode.py  # Continue processing despite errors
python deduplication_snippet.py  # Remove duplicates in micro-batches
```

## 🔧 Customization Guide

### Adapting for Your Data

1. **Modify Schemas** - Update schema definitions in each file
2. **Custom Business Rules** - Add validation functions in `business_logic_validation.py`
3. **Priority Logic** - Adjust ranking criteria in advanced deduplication
4. **Error Handling** - Customize corrupt record processing

### Example: Custom Business Rule

```python
def validate_custom_field(df):
    """Add your custom validation logic"""
    return df.filter(
        (col("your_field").isNotNull()) & 
        (col("your_field") > your_threshold)
    )
```

### Example: Custom Deduplication Priority

```python
# Modify priority in advanced deduplication
window_spec = Window.partitionBy("customer_id") \
                   .orderBy(desc("your_priority_field"), 
                           desc("your_secondary_field"))
```

## 🚀 Developer Testing & Deployment

### Monitoring
- Track FAILFAST vs PERMISSIVE usage ratios
- Monitor data quality metrics
- Set up alerts for validation failures

### Scaling
- Partition data by validation keys
- Use appropriate cluster sizing for data volume
- Consider caching strategies for repeated validations

## 📝 Sample Data

The repository includes sample data files in the `data/` directory:
- `clean_transactions.json` - Perfect data for FAILFAST testing
- `mixed_transactions.json` - Mixed quality data
- `invalid_transactions.json` - Corrupted data for error handling tests

## 🤝 Contributing

When adding new data quality strategies:

1. Create the main implementation file
2. Add comprehensive tests
3. Update this README with usage instructions
4. Include sample data if needed
5. Document performance characteristics

## 📄 License

This code is provided as educational examples for PySpark 4.0 data quality patterns. Adapt and modify as needed for your development and testing use cases.

---

**Remember**: Data quality is not one-size-fits-all. Choose the strategies that match your specific requirements for performance, error tolerance, and business logic complexity. 
