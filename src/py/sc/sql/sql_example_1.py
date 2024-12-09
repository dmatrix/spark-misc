from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Part of the code borrowed from SampleExamples.com for PySpark and modified
# for this test

import sys
sys.path.append('.')
from src.py.sc.utils.print_utils import print_header
import time

DATA_FILE="/Users/jules/git-repos/spark-misc/src/py/sc/sql/data/zipcodes.csv"

def run_dataframe_query(sp, num:int=5):

    # Create DataFrame
    df = (sp.read 
            .option("header",True) 
            .csv(DATA_FILE))
    df.printSchema()
    df.show(num)

    # Dataframe Select query
    print_header("DATAFRAME SELECT COLUMNS:")
    (df.select("country","city","zipcode","state") 
        .show(5))
    
    # use select and where
    print_header("DATAFRAME SELECT COLUMNS WITH WHERE CLAUSE STATE=AZ:")
    (df.select("country","city","zipcode","state") 
        .where("state == 'AZ'") 
        .show(5))

    # sorting
    print_header("DATAFRAME SELECT COLUMNS WITH WHERE CLAUSE AND ORDER BY STATE:")
    (df.select("country","city","zipcode","state") 
        .where("state in ('PR','AZ','FL')") 
        .orderBy("state") 
        .show(10))
    
    print_header("DATAFRAME SELECT STATE COUNT FROM ZIPCODES:")
    (df.groupBy("state").agg(count("*").alias("count")).show())
    
def run_sql_query(sp):
    # Create TempView SQL table
    (spark.read 
            .option("header",True) 
            .csv(DATA_FILE) 
            .createOrReplaceTempView("Zipcodes"))
            
    print_header("SQL SELECT COLUMNS from ZIPCODES:")
    (sp.sql("SELECT  country, city, zipcode, state FROM ZIPCODES")
        .show(5))

    print_header("SQL SELECT COLUMNS from ZIPCODES WHERE STATE=AZ:")
    (sp.sql(""" SELECT  country, city, zipcode, state FROM ZIPCODES 
            WHERE state = 'AZ' """) 
        .show(5))
        
    print_header("SQL SELECT COLUMNS WITH WHERE CLAUSE AND ORDER BY STATE:")
    (sp.sql(""" SELECT  country, city, zipcode, state FROM ZIPCODES 
            WHERE state in ('PR','AZ','FL') order by state """) 
        .show(10))

    print_header("SQL SELECT STATE COUNT FROM ZIPCODES:")
    (sp.sql(""" SELECT state, count(*) as count FROM ZIPCODES 
            GROUP BY state""") 
        .show())

if __name__ == "__main__":

    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("local[*]")
                .appName('PySpark SQL EXAMPLE') 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")
                        
    run_dataframe_query(spark)
    run_sql_query(spark)
    
    time.sleep(30) # for examining the SparkUI for jobs/stages/tasks generated from SparkConnect session
    spark.stop()