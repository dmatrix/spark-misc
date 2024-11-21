from pyspark.sql import SparkSession

# Part of the code borrowed from SampleExamples.com for PySpark and modified
# for this test

DATA_FILE="/Users/jules/git-repos/spark-misc/src/py/sc/sql/data/zipcodes.csv"

def run_dataframe_query(sp, num:int=5):

    # Create DataFrame
    df = (sp.read 
            .option("header",True) 
            .csv(DATA_FILE))
    df.printSchema()
    df.show(num)

    # Dataframe Select query
    (df.select("country","city","zipcode","state") 
        .show(5))
    
    # use select and where
    (df.select("country","city","zipcode","state") 
        .where("state == 'AZ'") 
        .show(5))

    # sorting
    (df.select("country","city","zipcode","state") 
        .where("state in ('PR','AZ','FL')") 
        .orderBy("state") 
        .show(10))
    
def run_sql_query(sp):
    # Create SQL table
    (spark.read 
            .option("header",True) 
            .csv(DATA_FILE) 
            .createOrReplaceTempView("Zipcodes"))
            

    (sp.sql("SELECT  country, city, zipcode, state FROM ZIPCODES")
        .show(5))

    (sp.sql(""" SELECT  country, city, zipcode, state FROM ZIPCODES 
            WHERE state = 'AZ' """) 
        .show(5))
        
    
    (sp.sql(""" SELECT  country, city, zipcode, state FROM ZIPCODES 
            WHERE state in ('PR','AZ','FL') order by state """) 
        .show(10))


    (sp.sql(""" SELECT state, count(*) as count FROM ZIPCODES 
            GROUP BY state""") 
        .show())

if __name__ == "__main__":

    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName('SqlExam[le') 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")
                        
    run_dataframe_query(spark)
    run_sql_query(spark)
    
    spark.stop()