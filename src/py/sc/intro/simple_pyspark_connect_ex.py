#
# simple example
#
from pyspark.sql import SparkSession
from IPython.display import display

from datetime import datetime, date
from pyspark.sql import Row


if __name__ == "__main__":
    # let's stop any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create a new session with Spark Connect
    spark = SparkSession.builder.remote("local[*]").getOrCreate()

    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    columns = ["id", "name"]
    data = [(1, "jules"), (2, "denny"), (3, "brooke"), (4, "td")]
    df = spark.createDataFrame(data).toDF(*columns)
    
    df.show()

    # Convert to Pandas
    display(df.toPandas())

    # try a data example
    df = spark.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
    ])
    df.show()
    
    spark.stop()
