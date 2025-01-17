"""
A combination ChatGPT, CodePilot, and docs used to generate code sample for testing
"""
import os
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")  # Ignore warnings coming from Arrow optimizations. 
from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import SparkSession
from IPython.display import display
import pandas as pd
import numpy as np

if __name__ == "__main__":
    spark = None
    # Create a new session with Spark Connect mode={"dbconnect", "connect", "classic"}
    if len(sys.argv) <= 1:
        args = ["dbconnect", "classic", "connect"]
        print(f"Command line must be one of these values: {args}")
        sys.exit(1)  

    mode = sys.argv[1]
    print(f"++++ Using Spark Connect mode: {mode}")
    
    # create Spark Connect type based on type of SparkSession you want
    if mode == "dbconnect":
        cluster_id = os.environ.get("clusterID")
        assert cluster_id
        spark = spark = DatabrckSparkSession().get()
    else:
        spark = SparkConnectSession(remote="local[*]", mode=mode,
                                app_name="Pandas Example 1").get()
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    def cube(n: int) -> int:
        return n**3

    s = pd.Series([1, 3, 5, np.nan, 6, 8])
    print_header("\nBASIC SERIES OPERATIONS:")
    print(s)
    print(f"max: {s.max()}; min:{s.min()}; size={s.size}; has NaN:{s.hasnans}")
    print_header("APPLY CUBE TO SERIES:")
    print(s.apply(cube))
    print_seperator()
    print_header("MUL OPERATION by 5:")
    print(s.mul(5, fill_value=-1, axis=0))
    print_seperator(size=10)
    df = pd.DataFrame({ 'a': [1, 2, 3, 4, 5, 6],
                        'b': [100, 200, 300, 400, 500, 600],
                        'c': ["one", "two", "three", "four", "five", "six"]},
                         index=[10, 20, 30, 40, 50, 60])

    print_header("\nA SIMPLE DATAFRAME:")
    display(df)
    print_seperator(size=10)
    spark.stop()