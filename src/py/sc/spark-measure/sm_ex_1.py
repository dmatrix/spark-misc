from pyspark.sql import SparkSession
import time

# Initialize Spark session with spark-measure
spark = SparkSession.builder \
    .appName("SparkMeasureExample") \
    .config("spark.extraListeners", "ch.cern.sparkmeasure.TaskMetricsListener") \
    .getOrCreate()

# Import spark-measure listener
from py4j.java_gateway import java_import

java_import(spark._jvm, "ch.cern.sparkmeasure.TaskMetrics")
tm_listener = spark._jvm.ch.cern.sparkmeasure.TaskMetrics(spark._jsc)

# Start collecting metrics
tm_listener.begin()

# Example: Generate a large synthetic dataset
data = [(i, i * 2, i % 10) for i in range(1, 1000001)]  # 1 million rows
columns = ["id", "value", "category"]

df = spark.createDataFrame(data, schema=columns)

# Perform some transformations
start_time = time.time()

# Select and filter operation
filtered_df = df.filter(df["value"] > 100).select("id", "value")

# GroupBy operation
grouped_df = filtered_df.groupBy("category").count()

# Sorting the results
sorted_df = grouped_df.orderBy("count", ascending=False)

# Action to trigger job execution
sorted_df.show()

# End collecting metrics
tm_listener.end()

# Display task metrics
print("\nTask Metrics Summary:")
print(tm_listener.report())

# Log additional information
end_time = time.time()
print(f"\nTotal Execution Time: {end_time - start_time:.2f} seconds")

# Stop Spark session
spark.stop()