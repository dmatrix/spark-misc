# Part of the code borrowed from SampleExamples.com for PySpark and modified
# for this test
import os
import sys
sys.path.append('.')

from pyspark.sql.functions import count

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header
import time

DATA_FILE = "/Users/jules/git-repos/spark-misc/src/py/sc/sql/data/zipcodes.csv"
DATA_COLUMNS = ["RecordNumber", "Country", "City", "Zipcode", "State"]
DATA_RECORDS = [(1,"US","PARC PARQUE",704,"PR"),
                (2,"US","PASEO COSTA DEL SUR",704,"PR"),
                (10,"US","BDA SAN LUIS",709,"PR"),
                (49347,"US","HOLT",32564,"FL"),
                (49348,"US","HOMOSASSA",34487,"FL"),
                (61391,"US","CINGULAR WIRELESS",76166,"TX"),
                (61392,"US","FORT WORTH",76177,"TX"),
                (61393,"US","FT WORTH",76177,"TX"),
                (54356,"US","SPRUCE PINE",35585,"AL"),
                (76511,"US","ASH HILL",27007,"NC"),
                (4,"US","URB EUGENE RICE",704,"PR"),
                (39827,"US","MESA",85209,"AZ"),
                (39828,"US","MESA",85210,"AZ"),
                (49345,"US","HILLIARD",32046,"FL"),
                (49346,"US","HOLDER",34445,"FL"),
                (3,"US","SECT LANAUSSE",704,"PR"),
                (54354,"US","SPRING GARDEN",36275,"AL"),
                (54355,"US","SPRINGVILLE",35146,"AL"),
                (76512,"US","ASHEBORO",27203,"NC"),
                (76513,"US","ASHEBORO",27204,"NC")]

def run_dataframe_query(sp, num:int=5):

    # Create DataFrame
    df = sp.createDataFrame(DATA_RECORDS).toDF(*DATA_COLUMNS)
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

    return df
    
def run_sql_query(sp, zdf):
    # Create TempView SQL table

    zdf.createOrReplaceTempView("ZIPCODES")
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
                                app_name="PySpark SQL EXAMPLE").get()
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")
                        
    df = run_dataframe_query(spark)
    run_sql_query(spark, df)
    
    spark.stop()