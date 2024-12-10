from delta import *
from pyspark.sql import SparkSession
import pyspark

import warnings
warnings.filterwarnings("ignore")

builder = pyspark.sql.SparkSession.builder.appName("Delta Exampl 1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(type(spark))
data = spark.range(0, 5)
data.write.format("delta").save("/tmp/delta-table")
