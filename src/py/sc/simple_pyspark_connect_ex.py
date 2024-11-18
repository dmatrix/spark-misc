from pyspark.sql import SparkSession
from IPython.display import display


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    columns = ["id", "name"]
    data = [(1, "jules"), (2, "denny"), (3, "brooke"), (4, "td")]
    df = spark.createDataFrame(data).toDF(*columns)
    
    df.show()

    # Convert to Pandas
    display(df.toPandas())