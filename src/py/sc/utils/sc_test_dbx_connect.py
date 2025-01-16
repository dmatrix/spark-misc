import sys
sys.path.append('.')
import os

import warnings
warnings.filterwarnings("ignore")
from IPython.display import display

from src.py.sc.utils.spark_session_cls import DatabrckSparkSession


if __name__ == "__main__":
    cluster_id = os.environ.get("clusterID")
    assert cluster_id
    spark = DatabrckSparkSession().get()
    
    # Ensure we are connected vis Spark Connect Session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++ Making sure it's using SparkConnect session:{spark} +++++")

    df = spark.range(10)
    print(df.show())

    columns = ["id", "name"]
    data = [(1, "jules"), (2, "denny"), (3, "brooke"), (4, "td")]
    df = spark.createDataFrame(data).toDF(*columns)
    print(df.show())

    # Convert to Pandas
    display(df.toPandas())
    spark.stop()
