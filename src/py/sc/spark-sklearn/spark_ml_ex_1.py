from pyspark.ml.classification import (LogisticRegression, LogisticRegressionModel)
from pyspark.ml.linalg import Vectors, DenseVector
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from src.py.sc.utils.print_utils import print_seperator, print_header
from src.py.sc.utils.spark_session_cls import SparkConnectSession

if __name__ == "__main__":
    # Step 1: Initialize Spark Session
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

        # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("local[*]")
                .appName("PySpark and spark-sklearn Distributed Training with SparkSession") \
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    df = spark.createDataFrame([(Vectors.dense([1.0, 2.0]), 1),
                                (Vectors.dense([2.0, -1.0]), 1),
                                (Vectors.dense([-3.0, -2.0]), 0),
                                (Vectors.dense([-1.0, -2.0]), 0)
                                ], schema=['features', 'label'])
    lr = LogisticRegression()
    lr.setMaxIter(30)
    lr.setThreshold(0.8)
    lr.write().overwrite().save("/tmp/connect-ml-demo/estimator")
    loaded_lr = LogisticRegression.load("/tmp/connect-ml-demo/estimator")                                                                

    assert (loaded_lr.getThreshold() == 0.8)                                                                                               
    assert loaded_lr.getMaxIter() == 30

    model: LogisticRegressionModel = lr.fit(df)
    assert (model.getThreshold() == 0.8)                                                                                                    
    assert model.getMaxIter() == 30
    model.predictRaw(Vectors.dense([1.0, 2.0]))
    DenseVector([-21.1048, 21.1048])
    model.summary.roc.show()