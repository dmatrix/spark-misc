"""
A combination ChatGPT, CodePilot, and docs used to generate code sample for testing
"""
import sys
sys.path.append('.')

from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import SparkSession
from IPython.display import display
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("local[*]")
                .appName('Pandas Example 1') 
                .getOrCreate())
    
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