#
# simple example
#
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")
from pyspark.sql import SparkSession


from IPython.display import display

from datetime import datetime, date
from pyspark.sql import Row

if __name__ == "__main__":

    spark = (SparkSession
                    .builder
                    .remote("local[*]")
                    .appName("First PySpark Connect application") 
                    .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # First simple example
    df = spark.range(10)
    print(df.show())
    print("---" * 10)

    # Second simple example
    columns = ["id", "name"]
    data = [(1, "jules"), (2, "denny"), (3, "brooke"), (4, "td")]
    df = spark.createDataFrame(data).toDF(*columns)
    print(df.show())
    print(df.schema)

    spark.stop()