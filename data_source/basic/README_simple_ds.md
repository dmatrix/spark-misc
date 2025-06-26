# SimpleDataSource for PySpark

This package provides a minimal example of a custom DataSource for PySpark 4.0, supporting both reading and writing simple data. It demonstrates how to implement the new Python DataSource API.

## Features
- **Read**: Generates two synthetic rows or reads from a simple CSV-like file (no header, format: `name,age`).
- **Write**: Writes DataFrame rows to a simple CSV-like file (no header, format: `name,age`).

## File Structure
- `simple_ds.py`: Implementation of the SimpleDataSource, reader, and writer.
- `test_simple_ds.py`: Pytest-based test cases for the data source.
- `test_data/`: Directory for test output files.

## Usage

### 1. Register and Use the DataSource in PySpark

``python
from pyspark.sql import SparkSession, Row
from simple_ds import SimpleDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(SimpleDataSource)

# Reading synthetic data
df = spark.read.format("simple").load()
df.show()

# Writing data
sample_data = [Row(name="Alice", age=20), Row(name="Bob", age=30)]
df = spark.createDataFrame(sample_data)
df.write.format("simple").mode("overwrite").option("path", "test_data/my_output.txt").save()

# Reading from file
df2 = spark.read.format("simple").option("path", "test_data/my_output.txt").load()
df2.show()

spark.stop()
``

### 2. Run the Example Script

You can also run the example directly:

```
python simple_ds.py
```

This will:
- Show the synthetic data
- Write sample data to `test_data/simple_output.txt`
- Read it back and display the result

### 3. Run the Test Cases

Tests are written using `pytest` and require PySpark 4.0+.

#### Install requirements (if needed):
```
pip install pyspark pytest
```

#### Run the tests:
```
pytest test_simple_ds.py
```

This will run three tests:
- Reading synthetic data
- Writing and reading back data
- Reading from a manually created file

## Notes
- The file format is very simple: each line is `name,age` (no header).
- Only the last partition's data is written if you use multiple partitions (for demonstration purposes).
- Output files are written to the `test_data/` directory.

## License
MIT 