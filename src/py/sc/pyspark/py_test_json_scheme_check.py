from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaEnforcement").getOrCreate()

# Define the schema
schema = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Create a sample JSON file
data = """
{"name": "Alice", "age": 30, "city": "New York"}
{"name": "Bob", "age": "40", "city": "London"}
{"name": "Charlie", "location": "Paris"}
"""
with open("people.json", "w") as f:
    f.write(data)

# Read the JSON file with the defined schema and permissive mode
df = spark.read.schema(schema).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "corrupt_record").json("people.json")

df.printSchema()
df.show()