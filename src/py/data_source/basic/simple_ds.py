from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession, Row
from typing import Iterator, Dict, Any
import os
import tempfile


class SimpleDataSource(DataSource):
    """
    A simple data source for PySpark that generates exactly two rows of synthetic data.
    Supports both reading and writing operations.
    """

    @classmethod
    def name(cls):
        return "simple"

    def __init__(self, options: Dict[str, Any]):
        super().__init__(options=options)
        self.options = options
        self.path = options.get("path", "")

    def schema(self):
        return StructType([
            StructField("name", StringType()),
            StructField("age", IntegerType())
        ])

    def reader(self, schema: StructType):
        return SimpleDataSourceReader(options=self.options, schema=schema)

    def writer(self, schema: StructType, overwrite: bool):
        """Create a writer for this data source."""
        return SimpleDataSourceWriter(self.options, schema, overwrite)


class SimpleDataSourceReader(DataSourceReader):

    def __init__(self, options: Dict[str, Any] = None, schema: StructType = None):
        self.options = options or {}
        self.schema = schema
        self.path = self.options.get("path", "")

    def read(self, partition):
        # If path is provided, read from file
        if self.path and os.path.exists(self.path):
            with open(self.path, 'r') as file:
                for line in file:
                    line = line.strip()
                    if line:  # Skip empty lines
                        parts = line.split(',')
                        if len(parts) >= 2:
                            name = parts[0]
                            try:
                                age = int(parts[1])
                                yield (name, age)
                            except ValueError:
                                # Skip invalid age values
                                continue
                        else:
                            print(f"SimpleDataSourceReader: Invalid line: {line}")
                            yield("invalid name", -1)
        else:
            # Default synthetic data
            yield ("Alice", 20)
            yield ("Bob", 30)


class SimpleDataSourceWriter(DataSourceWriter):
    """Writer for the simple data source."""
    
    def __init__(self, options: Dict[str, Any], schema: StructType, overwrite: bool):
        self.options = options
        self.schema = schema
        self.overwrite = overwrite
        self.path = options.get("path", "")

    def write(self, iterator: Iterator[Row], partition: int = 0) -> WriterCommitMessage:
        # Use partition index to create unique file per partition
        if self.path:
            dir_path = os.path.dirname(self.path)
            os.makedirs(dir_path, exist_ok=True)
            file_path = f"{self.path}"
        else:
            file_path = None

        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, dir=dir_path if self.path else None)
        try:
            with temp_file:
                for row in iterator:
                    name = row["name"]
                    age = row["age"]
                    temp_file.write(f"{name},{age}\n")
            if file_path:
                os.rename(temp_file.name, file_path)
            else:
                os.unlink(temp_file.name)
            return WriterCommitMessage()
        except Exception as e:
            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            raise e


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.dataSource.register(SimpleDataSource)


    # Test writing
    sample_data = [
        Row(name="Charlie", age=25),
        Row(name="Diana", age=28),
        Row(name="Eve", age=32)
    ]
    
    df = spark.createDataFrame(sample_data)
    print(f"Created DataFrame: {df.show()}")

    out_path = "test_data/simple_output"
    partition = 0
    output_path = f"{out_path}.part-{partition:05d}.txt"
    print(f"Writing to {output_path}")
    df.coalesce(1).write.format("simple").mode("overwrite").option("path", output_path).save()
    
    # Read back the written data
    print("\nReading written data:")
    read_df = spark.read.format("simple").option("path", output_path).load()
    read_df.show()
    
    spark.stop()