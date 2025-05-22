
# This script demonstrates how to enforce schema in PySpark DataFrames.
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SchemaEnforcement") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Create an empty DataFrame with the same schema as the target table
df = spark.createDataFrame([], schema="c INT")

# Loop to insert values into the DataFrame
c = 10
while c > 0:
    df = df.union(spark.createDataFrame([(c,)], schema="c INT"))
    c -= 1

# Insert the DataFrame into the target table
df.write.insertInto("t")

# Display the contents of the table
spark.sql("SELECT * FROM t").show()