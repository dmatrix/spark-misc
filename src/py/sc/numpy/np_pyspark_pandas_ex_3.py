"""
PySpark on Pandas notebook converted into Pyspark application 
Source: Spark 4.0.0preview-2 documentation: 
https://mybinder.org/v2/gh/apache/spark/f0d465e09b8?filepath=python%2Fdocs%2Fsource%2Fgetting_started%2Fquickstart_ps.ipynb
"""
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")  # Ignore warnings coming from Arrow optimizations. 

import time
import pyspark.pandas as ps
from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd

if __name__ == "__main__":

    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("local[*]")
                .appName("Pyspark Pandas/Numpy Example 2") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Object Creation
    # Creating a pandas-on-Spark Series by passing a list of values, letting pandas API on Spark create a default integer index:
    s = ps.Series([1, 3, 5, np.nan, 6, 8])
    print_header("SIMPLE OBJECT CREATION:")
    print(s)
    print_seperator(size=10)

    # Creating a pandas-on-Spark DataFrame by passing a dict of objects that 
    # can be converted to series-like.
    print_header("CREATE A PANDAS-ON-SPARK-DATAFRAME:")
    psdf = ps.DataFrame(
        {'a': [1, 2, 3, 4, 5, 6],
        'b': [100, 200, 300, 400, 500, 600],
        'c': ["one", "two", "three", "four", "five", "six"]},
        index=[10, 20, 30, 40, 50, 60])
    print(psdf)
    print_seperator(size=10)

    # Creating a pandas DataFrame by passing a numpy array, with a datetime index and labeled columns:
    dates = pd.date_range('20130101', periods=6)
    pdf = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD'))
    print_header("PANDAS DATAFRAME WITH DATES AS INDEX:")
    print(pdf)
    print_seperator(size=10)

    # Now, this pandas DataFrame can be converted to a pandas-on-Spark DataFrame
    psdf = ps.from_pandas(pdf)
    print_header("CONVERT A PANDAS DATAFRAME INTO PANDAS-ON-SPARK DATAFRAME:")
    print(psdf.head())
    print(f"pandas-on-spark-df: {type(psdf)}")
    print_seperator(size=10)

    # Also, it is possible to create a pandas-on-Spark DataFrame from Spark DataFrame easily.
    # Creating a Spark DataFrame from pandas DataFrame
    print_header("CREATE A SPARK DATAFRAME FROM PANDAS DATAFRAME:")
    sdf = spark.createDataFrame(pdf)
    sdf.show()
    print(f"Spark dataframe : {type(sdf)}")
    print_seperator(size=10)

    # Creating pandas-on-Spark DataFrame from Spark DataFrame.
    print_header("CREATING A PANDAS-ON-SPARK DATAFRAME FROM SPARK DATAFRAME:")
    psdf = sdf.pandas_api()
    print(psdf.head())
    print(f"pandas-on-spark-df: {type(psdf)}")
    print_seperator(size=10)

    # Showing a quick statistic summary of your data
    print_header("SHOWING STATS FROM PANDAS-ON-SPARK-DATAFRAME:")
    print(psdf.describe())
    print_seperator(size=10)

    # Transposing your data
    print_header("TRANSPOSING PANDAS-ON-SPARK-DATAFRAME:")
    print(psdf.T)
    print_seperator(size=10)

    # Sorting by its index
    print_header("SORTING PANDAS-ON-SPARK DF BY INDEX:")
    print(psdf.sort_index(ascending=False))
    print_seperator(size=10)

    # Sorting by value
    print_header("SORTING PANDAS-ON-SPARK DF BY VALUE:")
    print(psdf.sort_values(by='B', ascending=False))
    print_seperator(size=10)
    print(psdf.sort_values(by='B'))
    print_seperator(size=10)

    # Handling missing data
    # Pandas API on Spark primarily uses the value np.nan to represent missing data. 
    # It is by default not included in computations.
    print_header("CREATING A PANDAS-ON-SPARK WITH NAN AND DROPPING THEM:")
    pdf1 = pdf.reindex(index=dates[0:4], columns=list(pdf.columns) + ['E'])
    pdf1.loc[dates[0]:dates[1], 'E'] = 1

    # convert from pandas to pandas--> pandas-on-spark
    psdf1 = ps.from_pandas(pdf1)
    print(psdf1.head())
    print_seperator(size=10)

    # Now drop NaN values
    print_header("DROPPING NAN VALUES FROM PANDAS-ON-SPARK DF:")
    print(psdf1.dropna(how='any'))
    print_seperator(size=10)

    # Filling missing data. Note that values are not changed
    # in-place. Rather a new DF is returned.
    print_header("FILLING IN NAN VALUES IN PANDAS-ON-SPARK DF:")
    print(psdf1.fillna(value=5))
    print_seperator(size=10)

    # PySpark Operations: Stats
    # Performing a descriptive statistic:
    print_header("DF OPERATIONS ON PANDAS-ON-SPARK: STATS:")
    print(psdf.mean())
    print_seperator(size=10)

    # Spark Configurations
    # Various configurati ons in PySpark could be applied internally in pandas API on Spark.
    # For example, you can enable Arrow optimization to hugely speed up internal pandas 
    # conversion. See also PySpark Usage Guide for Pandas with Apache Arrow in PySpark 
    # documentation.

    prev = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")  # Keep its default value.
    ps.set_option("compute.default_index_type", "distributed")  # Use default index prevent overhead.  

    # Time the operation
    print_header("TIME THE OPERAITON: with arraow pyspark execution enabled:")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
    start_time = time.time()
    ps.range(300000).to_pandas()
    duration = time.time() - start_time
    print(f"Duration: {duration:.4f} sec")
    print_seperator(size=10)

    print_header("TIME THE OPERAITON: with arraow pyspark execution disabled:")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", False)
    start_time = time.time()
    ps.range(300000).to_pandas()
    duration = time.time() - start_time
    print(f"Duration: {duration:.4f} sec")

    # Reset the options
    ps.reset_option("compute.default_index_type")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", prev)  # Set its default value back.

    # Grouping: By “group by” we are referring to a process involving 
    # one or more of the following steps:
        #  1) Splitting the data into groups based on some criteria
        #  2) Applying a function to each group independently
        #  3) Combining the results into a data structure

    # Create a pandas-on-pyspark DF
    print_header("\n\nGROUPING OPERATIONS ON PANDAS-ON-PYSPARK DF:")
    psdf = ps.DataFrame({'A': ['foo', 'bar', 'foo', 'bar',
                          'foo', 'bar', 'foo', 'foo'],
                        'B': ['one', 'one', 'two', 'three',
                          'two', 'two', 'one', 'three'],
                        'C': np.random.randn(8),
                        'D': np.random.randn(8)})
    print(psdf.head())
    print_seperator(size=15)

    # Grouping and then applying the sum() function to the resulting groups.
    print(psdf.groupby('A').sum())
    print_seperator(size=15)

    # Grouping by multiple columns forms a hierarchical index, and again we can apply 
    # the sum function.
    print(psdf.groupby(['A', 'B']).sum())

    # Plotting
    pser = pd.Series(np.random.randn(1000),
                 index=pd.date_range('1/1/2000', periods=1000))
    psser = ps.Series(pser)
    psser = psser.cummax()
    psser.plot().show()

    # On a DataFrame, the plot() method is a convenience to plot all of the columns 
    # with labels:
    pdf = pd.DataFrame(np.random.randn(1000, 4), index=pser.index,
                   columns=['A', 'B', 'C', 'D'])
    psdf = ps.from_pandas(pdf)
    psdf = psdf.cummax()
    psdf.plot().show()

    # Getting data in/out
    # CSV is straightforward and easy to use. See here to write a CSV file and here to read a CSV file.
    print_header("PANDAS-ON-SPARK I/O: CSV: WRITE-AND-READ BACK")
    psdf.to_csv('foo.csv')
    psdf_r = ps.read_csv('foo.csv').head(10)
    print(psdf_r.head())
    print_seperator(size=15)

    # Parquet
    # Parquet is an efficient and compact file format to read and write faster. 
    # See here to write a Parquet file and here to read a Parquet file.
    print_header("PANDAS-ON-SPARK I/O: PARQUET: WRITE-AND-READ BACK")
    psdf.to_parquet('bar.parquet')
    psdf_p = ps.read_parquet('bar.parquet').head(10)
    print(psdf_p.head())

    # Spark IO
    # In addition, pandas API on Spark fully supports Spark's various datasources such as 
    # ORC and an external datasource. See here to write it to the specified datasource and here to read it from the datasource.

    print_header("PANDAS-ON-SPARK I/O: ORC: WRITE-AND-READ BACK")
    psdf.spark.to_spark_io('zoo.orc', format="orc")
    psdf_orc = ps.read_spark_io('zoo.orc', format="orc").head(10)
    print(psdf_orc.head())
    
    spark.stop()
