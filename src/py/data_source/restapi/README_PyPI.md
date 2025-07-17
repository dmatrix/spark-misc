# PySpark REST DataSource

[![PyPI version](https://badge.fury.io/py/pyspark-rest-datasource.svg)](https://badge.fury.io/py/pyspark-rest-datasource)
[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Spark](https://img.shields.io/badge/spark-4.0+-red.svg)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A development-ready REST API Data Source implementation for Apache Spark using the Python Data Source API (Spark 4.0+). This allows you to read from and write to REST APIs directly using Spark's DataFrame API.

## 🚀 Features

✅ **Full PySpark Data Source API Implementation**
- `spark.read.format("restapi")` - Read from REST APIs
- `spark.write.format("restapi")` - Write to REST APIs  
- `spark.sql()` - SQL queries with temporary views
- Schema inference from API responses
- Custom authentication headers support
- Proper error handling and validation

✅ **Development Ready**
- Comprehensive error handling for network issues
- Configurable timeouts and headers
- Proper logging and debugging
- Full compliance with PySpark Data Source API
- Type-safe data conversion

## 📦 Installation

### From Test PyPI (Current)
The package is currently available on Test PyPI for testing:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ pyspark-rest-datasource
```

**Test PyPI Package**: https://test.pypi.org/project/pyspark-rest-datasource/0.1.0/

### From Production PyPI (Coming Soon)
Once uploaded to production PyPI:

```bash
# Using pip
pip install pyspark-rest-datasource

# Using uv (recommended)
uv add pyspark-rest-datasource
```

### Requirements
- Python 3.9+
- Apache Spark 4.0+
- PyArrow 10.0.0+
- Requests 2.25.0+

## 🔧 Quick Start

```python
from pyspark.sql import SparkSession
from pyspark_rest_datasource import RestApiDataSource

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
```

## 📖 Usage Examples

### Reading from REST APIs

```python
# Basic usage
users_df = spark.read \
    .format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/users") \
    .option("method", "GET") \
    .load()

print(f"Retrieved {users_df.count()} users")
users_df.show(5)
```

### Custom Headers & Authentication

```python
import json

# API key authentication
headers = {"Authorization": "Bearer YOUR_API_KEY"}
df = spark.read \
    .format("restapi") \
    .option("url", "https://api.example.com/data") \
    .option("headers", json.dumps(headers)) \
    .option("timeout", "60") \
    .load()
```

### SQL Integration

```python
# Create temporary view
spark.read \
    .format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .load() \
    .createOrReplaceTempView("posts_api")

# Query with SQL
result = spark.sql("""
    SELECT userId, COUNT(*) as post_count 
    FROM posts_api 
    GROUP BY userId 
    ORDER BY post_count DESC 
    LIMIT 5
""")
result.show()
```

### Writing to REST APIs

```python
from pyspark.sql.types import StructType, StructField, StringType

# Create sample data
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

## ⚙️ Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `url` | *required* | REST API endpoint URL |
| `method` | `GET` | HTTP method (GET, POST, PUT, DELETE) |
| `headers` | `{}` | Custom headers as JSON string |
| `timeout` | `30` | Request timeout in seconds |

## 🏗️ Architecture

The package consists of three main components:

1. **RestApiReader** - Implements `DataSourceReader` for reading from REST APIs
2. **RestApiWriter** - Implements `DataSourceWriter` for writing to REST APIs  
3. **RestApiDataSource** - Main data source class implementing `DataSource`

### Key Features

- **Automatic Schema Inference**: Detects data types from API responses
- **Type-Safe Conversion**: Properly handles integers, floats, booleans, and strings
- **Error Handling**: Comprehensive error handling for network and parsing issues
- **Logging**: Built-in logging for debugging and monitoring

## 🛠️ Development

### Local Development

```bash
# Clone the repository
git clone https://github.com/yourusername/pyspark-rest-datasource.git
cd pyspark-rest-datasource

# Install in development mode
pip install -e .

# Run tests
python -m pytest
```

### Building the Package

```bash
# Install build tools
pip install build

# Build the package
python -m build

# Upload to PyPI
pip install twine
twine upload dist/*
```

## 🔍 Error Handling

The data source handles various error conditions:

- **Network Errors**: Connection timeouts, DNS resolution failures
- **HTTP Errors**: 404 Not Found, 500 Internal Server Error, etc.
- **Invalid JSON**: Malformed API responses
- **Missing Options**: Required URL parameter validation
- **Schema Mismatches**: Graceful handling of unexpected response formats

## 📈 Performance

- **Lazy Evaluation**: Leverages Spark's lazy evaluation for optimal performance
- **Schema Caching**: Caches inferred schemas to avoid repeated API calls
- **Connection Reuse**: Efficient HTTP connection handling
- **Memory Efficient**: Streams data without loading entire responses into memory

## 🚨 Limitations

- **Pagination**: No automatic pagination support (planned for future versions)
- **Streaming**: Streaming read/write not implemented yet
- **Batch Size**: No configurable batch size for large datasets
- **Rate Limiting**: No built-in rate limiting

## 🗺️ Roadmap

- [ ] Pagination support for large datasets
- [ ] Streaming implementation
- [ ] Advanced authentication methods (OAuth, JWT)
- [ ] Connection pooling and caching
- [ ] Rate limiting and retry logic

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋‍♂️ Support

- **Documentation**: Check the examples in this README
- **Issues**: [GitHub Issues](https://github.com/yourusername/pyspark-rest-datasource/issues)
- **Questions**: Create a discussion in the repository

## 🔗 Links

- [Test PyPI Package](https://test.pypi.org/project/pyspark-rest-datasource/0.1.0/)
- [Production PyPI Package](https://pypi.org/project/pyspark-rest-datasource/) (Coming Soon)
- [GitHub Repository](https://github.com/yourusername/pyspark-rest-datasource)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Python Data Source API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html)

---

**Ready to use with any REST API that returns JSON data!** 🚀 