# REST API Data Source for PySpark

A simple, single-file REST API Data Source implementation for Apache Spark using the Python Data Source API (Spark 4.0+). This allows you to read from and write to REST APIs directly using Spark's DataFrame API with full partitioning support for parallel processing.

## 🚀 Package Status

[![Test PyPI](https://img.shields.io/badge/Test%20PyPI-0.2.0-blue)](https://test.pypi.org/project/pyspark-rest-datasource/0.2.0/)
[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Spark](https://img.shields.io/badge/spark-4.0+-red.svg)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Current Status**: Available on Test PyPI for testing  
**Test PyPI Package**: https://test.pypi.org/project/pyspark-rest-datasource/0.2.0/

```bash
# Install from Test PyPI
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ pyspark-rest-datasource
```

## Features

✅ **Full PySpark Data Source API Implementation**
- `spark.read.format("restapi")` - Read from REST APIs
- `spark.write.format("restapi")` - Write to REST APIs  
- `spark.sql()` - SQL queries with temporary views
- Schema inference from API responses
- Custom authentication headers support
- Proper error handling and validation

✅ **Advanced Partitioning Support**
- **Single partition** - Default mode for simple APIs
- **URL-based partitioning** - Multiple endpoints processed in parallel
- **Page-based partitioning** - Pagination with parallel page processing
- **Parallel processing** - Utilizes all available Spark cores
- **Pure DataFrame API** - No RDD operations, fully DataFrame-based

✅ **Development Ready**
- Comprehensive test suite with real API calls
- Error handling for network issues
- Configurable timeouts and headers
- Proper logging and debugging
- Full compliance with PySpark Data Source API
- Single-file implementation for easy deployment

## Installation

### Prerequisites
- Python 3.9+
- Apache Spark 4.0+ (for Python Data Source API support)
- Required packages:
  - `pyspark>=4.0.0`
  - `pyarrow>=10.0.0`
  - `requests>=2.25.0`

### Install from Test PyPI

The package is currently available on Test PyPI for testing:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ pyspark-rest-datasource
```

**Test PyPI Package**: https://test.pypi.org/project/pyspark-rest-datasource/0.2.0/

### Install from Production PyPI (Coming Soon)

Once uploaded to production PyPI, you'll be able to install with:

```bash
pip install pyspark-rest-datasource
# or
uv add pyspark-rest-datasource
```

### Package Information

This package uses modern Python packaging standards with a simple single-file structure:

#### Package Structure
- **`restapi.py`** - Main implementation file containing all REST API Data Source classes
- **`example.py`** - Example usage script demonstrating basic and advanced features
- **`pyproject.toml`** - Package configuration and dependencies
- **`README.md`** - Complete documentation with examples
- **`CHANGELOG.md`** - Version history and feature updates

#### Package Benefits
- **Single file deployment** - Easy to understand and modify
- **No complex package hierarchy** - All functionality in one place
- **Easy debugging** - All code visible in a single file
- **Simplified maintenance** - No synchronization between multiple files

#### Package Metadata Summary
```
Name: pyspark-rest-datasource
Version: 0.2.0
Python Required: >=3.9
Dependencies:
  - pyspark>=4.0.0
  - pyarrow>=10.0.0
  - requests>=2.25.0
Test PyPI: https://test.pypi.org/project/pyspark-rest-datasource/0.2.0/
```

### Setup Options

#### Option 1: Editable Installation (Recommended for Development)
```bash
# Clone or download the source code
cd pyspark-rest-datasource/

# Install in editable mode using pip
pip install --editable .

# Or install using uv (if available)
uv pip install --editable .

# Run tests to verify installation
python run_tests.py
```

#### Option 2: Direct Installation
```bash
# Install dependencies manually
pip install pyarrow>=20.0.0 pyspark>=4.0.0 pytest>=8.4.1

# Or install from pyproject.toml
pip install .

# Run tests to verify installation
python run_tests.py
```

#### Option 3: Using uv (Modern Python Package Manager)
```bash
# Install with uv
uv pip install --editable .

# Or sync from lock file
uv pip sync uv.lock

# Run tests
python run_tests.py
```

### Verifying Installation

After installation from Test PyPI, verify the package works correctly:

```bash
# Test basic import
python -c "from restapi import RestApiDataSource; print('✅ Package imported successfully!')"

# Test package functionality
python -c "from restapi import RestApiDataSource; print('Package name:', RestApiDataSource.name())"

# Check dependencies are available
python -c "import pyspark; import pyarrow; import requests; print('✅ All dependencies OK')"

# Run example script
python example.py
```

## Usage

### Basic Usage

#### Reading from REST APIs
```python
from pyspark.sql import SparkSession
from restapi import RestApiDataSource

# Initialize Spark
spark = SparkSession.builder.appName("REST API Example").getOrCreate()

# Register the data source
spark.dataSource.register(RestApiDataSource)

# Read from REST API
df = spark.read \
    .format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/users") \
    .option("method", "GET") \
    .load()

# Display results
df.show()
df.count()  # Should return 10 for JSONPlaceholder users
```

#### Writing to REST APIs
```python
# Create sample data
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField("title", StringType(), True),
    StructField("body", StringType(), True),
    StructField("userId", StringType(), True)
])

data = [("Test Post", "This is a test post", "1")]
df = spark.createDataFrame(data, schema)

# Write to REST API
df.write \
    .format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("method", "POST") \
    .mode("append") \
    .save()
```

#### SQL Integration
```python
# Create temporary view
spark.read \
    .format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/users") \
    .load() \
    .createOrReplaceTempView("users_api")

# Query with SQL
result = spark.sql("SELECT name, email FROM users_api WHERE id = '1'")
result.show()
```

### Advanced Partitioning

The REST API Data Source supports three partitioning strategies for parallel processing:

#### 1. Single Partition (Default)
```python
# Standard single partition - processes one API call
df = spark.read \
    .format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .load()
```

#### 2. URL-based Partitioning
```python
# Multiple URLs processed in parallel across Spark cores
df = spark.read \
    .format("restapi") \
    .option("partitionStrategy", "urls") \
    .option("urls", "https://jsonplaceholder.typicode.com/posts/1,https://jsonplaceholder.typicode.com/posts/2,https://jsonplaceholder.typicode.com/posts/3") \
    .load()
```

#### 3. Page-based Partitioning
```python
# Paginated API calls processed in parallel
df = spark.read \
    .format("restapi") \
    .option("partitionStrategy", "pages") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("totalPages", "5") \
    .option("pageSize", "20") \
    .load()
```

### Configuration Options

#### Basic Options
| Option | Default | Description |
|--------|---------|-------------|
| `url` | *required* | REST API endpoint URL |
| `method` | `GET` | HTTP method (GET, POST, PUT, DELETE) |
| `headers` | `{}` | Custom headers as JSON string |
| `timeout` | `30` | Request timeout in seconds |

#### Partitioning Options
| Option | Default | Description |
|--------|---------|-------------|
| `partitionStrategy` | `single` | Partitioning strategy: `single`, `urls`, `pages` |
| `urls` | - | Comma-separated URLs for URL-based partitioning |
| `totalPages` | `1` | Number of pages for page-based partitioning |
| `pageSize` | `100` | Items per page for page-based partitioning |

### Authentication Examples

#### Custom Headers
```python
import json

# API key authentication
headers = {"Authorization": "Bearer YOUR_API_KEY"}
df = spark.read \
    .format("restapi") \
    .option("url", "https://api.example.com/data") \
    .option("headers", json.dumps(headers)) \
    .load()

# Custom User-Agent
headers = {"User-Agent": "MyApp/1.0"}
df = spark.read \
    .format("restapi") \
    .option("url", "https://api.example.com/data") \
    .option("headers", json.dumps(headers)) \
    .load()
```

## Testing

### Testing the Package

After installing from Test PyPI, you can test the package:

```bash
# Test basic import
python -c "from restapi import RestApiDataSource; print('✅ Package works!')"

# Run the example script
python example.py
```

### Development Testing

For development, you can run the original test files:

```bash
# Run all tests
python run_tests.py

# Run individual test file
python test_format_api.py
```

### Test Suite Overview

The test suite includes comprehensive tests for:

1. **Basic Read Operations** - Reading from JSONPlaceholder API
2. **Single Resource Reads** - Reading specific resources by ID
3. **Multiple Resource Reads** - Reading collections (posts, users)
4. **SQL Integration** - Temporary views and SQL queries
5. **Schema Inference** - Automatic schema detection
6. **Error Handling** - Invalid URLs and network errors
7. **Custom Headers** - Authentication and custom headers
8. **Write Operations** - Writing data to REST APIs
9. **Streaming Methods** - Proper NotImplementedError handling
10. **Validation** - Required options validation

### Expected Test Results

When you run `python run_tests.py`, you should see:

```
🚀 Starting REST API Data Source Tests
================================================================================
✅ Spark session initialized and REST API data source registered

🧪 Testing spark.read.format('restapi') - Basic Read
📊 DataFrame content:
+---+--------------------+--------------------+...
| id|                name|               email|...
+---+--------------------+--------------------+...
|  1|       Leanne Graham|   Sincere@april.biz|...
|  2|        Ervin Howell|   Shanna@melissa.tv|...
...
+---+--------------------+--------------------+...

📊 DataFrame count: 10
✅ Basic read test passed - Retrieved 10 users

🧪 Testing spark.read.format('restapi') - Single User
📊 Single user DataFrame content:
+---+-------------+-----------------+...
| id|         name|            email|...
+---+-------------+-----------------+...
|  1|Leanne Graham|Sincere@april.biz|...
+---+-------------+-----------------+...

📊 DataFrame count: 1
✅ Single user test passed

🧪 Testing spark.read.format('restapi') - Posts
📊 Posts DataFrame content (first 20 rows):
📊 Posts DataFrame count: 100
✅ Posts test passed - Retrieved 100 posts

🧪 Testing SQL API with temporary view
📊 Initial DataFrame content:
[Users data displayed]
📊 Initial DataFrame count: 10

📊 SQL Query result:
+-------------+-----------------+
|         name|            email|
+-------------+-----------------+
|Leanne Graham|Sincere@april.biz|
+-------------+-----------------+

📊 SQL Query result count: 1
✅ SQL API test passed

🧪 Testing schema inference
📊 DataFrame content with inferred schema:
root
 |-- id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- phone: string (nullable = true)
 |-- website: string (nullable = true)
 |-- username: string (nullable = true)
 |-- address: string (nullable = true)
 |-- company: string (nullable = true)

📊 Schema inference DataFrame count: 10
✅ Schema inference test passed

🧪 Testing spark.write.format('restapi') - Mock Write
✅ Write test completed with expected error: AnalysisException

🧪 Testing error handling
✅ Error handling test passed - Got expected error: PythonException

🧪 Testing custom headers

📊 Custom headers DataFrame count: 1
✅ Custom headers test passed

🧪 Testing streaming methods raise NotImplementedError
✅ Streaming methods NotImplementedError test passed

🧪 Testing required options validation
✅ Required options validation test passed

================================================================================
🎉 All DataSource API tests passed!
✅ spark.read.format('restapi') works correctly
✅ spark.write.format('restapi') API works correctly
✅ spark.dataSource.register() works correctly
✅ SQL API with temporary views works
✅ Schema inference works
✅ Error handling works
✅ Custom headers work
✅ Streaming methods properly raise NotImplementedError
✅ Required options validation works
```

### Test Validation

All tests should pass with these key metrics:
- **Users API**: 10 users retrieved
- **Single User**: 1 user retrieved (Leanne Graham)
- **Posts API**: 100 posts retrieved
- **SQL Query**: 1 result from filtered query
- **Schema Inference**: 8 string fields detected
- **Error Handling**: Proper exceptions for invalid URLs
- **Custom Headers**: Authentication headers working

## Architecture

### Components

1. **RestApiReader** - Implements `DataSourceReader` for reading from REST APIs
2. **RestApiWriter** - Implements `DataSourceWriter` for writing to REST APIs  
3. **RestApiDataSource** - Main data source class implementing `DataSource`

### Key Methods

- `name()` - Returns "restapi" for format registration
- `schema()` - Infers schema from API response
- `reader()` - Creates reader for batch operations
- `writer()` - Creates writer for batch operations
- `streamReader()` - Raises NotImplementedError (not yet implemented)
- `streamWriter()` - Raises NotImplementedError (not yet implemented)

## Error Handling

The data source handles various error conditions:

- **Network Errors**: Connection timeouts, DNS resolution failures
- **HTTP Errors**: 404 Not Found, 500 Internal Server Error, etc.
- **Invalid JSON**: Malformed API responses
- **Missing Options**: Required URL parameter validation
- **Schema Mismatches**: Graceful handling of unexpected response formats

## Logging

The implementation includes comprehensive logging:

```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Logs include:
# - API request URLs and methods
# - Response status codes
# - Error details and stack traces
# - Performance metrics
```

## Limitations & Future Enhancements

### Current Limitations
- **Pagination**: No automatic pagination support
- **Streaming**: Streaming read/write not implemented
- **Batch Size**: No configurable batch size for large datasets
- **Caching**: No response caching mechanism
- **Rate Limiting**: No built-in rate limiting

### Planned Enhancements
1. **Pagination Support** - Automatic handling of paginated APIs
2. **Streaming Implementation** - Real-time data processing
3. **Authentication Methods** - OAuth, JWT, API keys
4. **Performance Optimization** - Connection pooling, caching
5. **Advanced Schema Handling** - Nested JSON, data type inference

## Package Maintenance

### Updating Package Metadata

When you modify the project (add files, change dependencies, update version), you need to update the package metadata:

#### 1. Update `pyproject.toml`
```toml
[project]
name = "pyspark-rest-datasource"
version = "0.2.0"  # Update version as needed
description = "Add your description here"
readme = "README.md"
requires-python = ">=3.13"
dependencies = [
    "pyarrow>=20.0.0",
    "pyspark>=4.0.0",
    "pytest>=8.4.1",
    # Add new dependencies here
]
```

#### 2. Regenerate egg-info Files
```bash
# Reinstall in editable mode to update egg-info
uv pip install --editable .

# Or with regular pip
pip install --editable .
```

#### 3. Verify Updates
```bash
# Check updated metadata
python -c "import restapi; print('✅ Module loaded successfully')"

# Check package dependencies
cat pyproject.toml | grep -A 5 "dependencies ="

# Check build outputs
ls -la dist/
```

### Package Distribution

#### Creating a Distribution Package
```bash
# Install build tools
pip install build

# Build distribution packages
python -m build

# This creates:
# - dist/pyspark_rest_datasource-0.2.0-py3-none-any.whl
# - dist/pyspark_rest_datasource-0.2.0.tar.gz
```

#### Publishing to PyPI (Optional)
```bash
# Install twine
pip install twine

# Upload to PyPI
twine upload dist/*

# Or upload to Test PyPI first
twine upload --repository testpypi dist/*
```

### Development Workflow

1. **Make changes** to `restapi.py` or `example.py`
2. **Update `pyproject.toml`** if dependencies change
3. **Reinstall package** with `uv pip install --editable .`
4. **Run tests** with `python run_tests.py`
5. **Test partitioning** with `python example_partition_usage.py`
6. **Build and upload** new package version

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Update package metadata if needed
5. Ensure all tests pass
6. Submit a pull request

## License

MIT License - feel free to use in your projects.

## Support

For questions or issues:
1. Check the test suite for usage examples
2. Review the source code documentation
3. Create an issue with reproduction steps

---

**Ready for development with any REST API that returns JSON data!** 🚀
