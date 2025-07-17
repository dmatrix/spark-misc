# Installing from Test PyPI

## 🚀 Quick Installation

The `pyspark-rest-datasource` package is currently available on Test PyPI for testing and evaluation.

### Install Command

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ pyspark-rest-datasource
```

### Why Use Test PyPI?

- **Testing**: Verify the package works correctly before production release
- **Early Access**: Try the latest features and provide feedback
- **Safe Environment**: Test PyPI is separate from production PyPI

## 📦 Package Information

- **Package Name**: `pyspark-rest-datasource`
- **Version**: `0.1.0`
- **Test PyPI URL**: https://test.pypi.org/project/pyspark-rest-datasource/0.1.0/
- **Python Requirements**: >=3.9
- **Dependencies**: PySpark 4.0+, PyArrow 10.0+, Requests 2.25+

## 🔧 Quick Test

After installation, test that everything works:

```bash
# Test basic import
python -c "from pyspark_rest_datasource import RestApiDataSource; print('✅ Package works!')"

# Test with Spark (requires Spark session)
python -c "
from pyspark.sql import SparkSession
from pyspark_rest_datasource import RestApiDataSource

spark = SparkSession.builder.appName('Test').getOrCreate()
spark.dataSource.register(RestApiDataSource)
print('✅ Data source registered successfully!')
spark.stop()
"
```

## 📋 Full Example

```python
from pyspark.sql import SparkSession
from pyspark_rest_datasource import RestApiDataSource

# Initialize Spark
spark = SparkSession.builder.appName("REST API Test").getOrCreate()

# Register the data source
spark.dataSource.register(RestApiDataSource)

# Read from REST API
df = spark.read \
    .format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/users") \
    .option("method", "GET") \
    .load()

# Display results
print(f"Retrieved {df.count()} records")
df.show(3)

# Clean up
spark.stop()
```

## 🛠️ Troubleshooting

### Common Issues

1. **Import Error**: Make sure you installed from Test PyPI with the correct command
2. **Spark Not Found**: Ensure PySpark 4.0+ is installed
3. **PyArrow Missing**: Install PyArrow 10.0+ if not automatically installed

### Getting Help

- Check the [Test PyPI package page](https://test.pypi.org/project/pyspark-rest-datasource/0.1.0/)
- Review the main [README.md](README.md) for full documentation
- Run the example script: `python example.py`

## 🚀 What's Next?

Once testing is complete, the package will be uploaded to production PyPI where you can install it with:

```bash
pip install pyspark-rest-datasource
# or
uv add pyspark-rest-datasource
```

---

**Ready to test the PySpark REST DataSource!** 🎯 