from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

# Initialize Spark and create DataFrame
spark = SparkSession.builder.appName("EmployeeAnalysis").getOrCreate()

employee_df = spark.createDataFrame([
    ("John Smith", 28, 65000.00, "Engineering"),
    ("Sarah Johnson", 34, 75000.00, "Marketing"),
    ("Mike Davis", 42, 85000.00, "Finance"),
    ("Emily Brown", 29, 58000.00, "HR")
], ["name", "age", "salary", "department"])

# Calculate and display totals
employee_df.agg(
    spark_sum("salary").alias("Total Salary"),
    spark_sum("age").alias("Total Ages")
).show()

spark.stop()