""" A simple Spark Connect application that reads a file and counts characters"""

from pyspark.sql import SparkSession

README_FILE="/Users/jules/spark_homes/current/README.md"

if __name__ == "__main__":
    spark = (SparkSession.builder
        #.remote("sc://localhost")
        .appName("SimpleApp")
        .getOrCreate())
    
    log_data = spark.read.text(README_FILE).cache()

    # Count character and word spark occurances
    num_of_a = log_data.filter(log_data.value.contains('a')).count()
    num_of_b = log_data.filter(log_data.value.contains('b')).count()

    spark_count = log_data.filter(log_data.value.contains("spark")).count()

    # print values out
    print(f"Character 'a' occurances: {num_of_a}")
    print(f"Character 'b' occurances: {num_of_b}")
    print(f"Word 'spark' occurances : {spark_count}")
    
    spark.stop()
