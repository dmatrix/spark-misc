#
# Examples from pyspark-cookbook guide
#
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

def do_print_util(label:str) -> None:
    print(f"{label}")
    print("-" * len(label))

if __name__ == "__main__":

    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName('PysparkFunctionExample') 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Create a dataframe

    df_1 = spark.createDataFrame([
            Row(age=10, height=80.0, NAME="Alice"),
            Row(age=10, height=80.0, NAME="Alice"),
            Row(age=5, height=float("nan"), NAME="BOB"),
            Row(age=None, height=None, NAME="Tom"),
            Row(age=None, height=float("nan"), NAME=None),
            Row(age=9, height=78.9, NAME="josh"),
            Row(age=18, height=1802.3, NAME="bush"),
            Row(age=7, height=75.3, NAME="jerry"),
    ])

    df_1.show()
    column_str = "COLUMN RENAME FUNCTION"
    do_print_util(column_str)

    # Renmame a column
    df_2 = df_1.withColumnRenamed("NAME", "name")
    df_2.show()

    # Drop null values and NaN values
    column_str= "DROP NULL VALUES"
    do_print_util(column_str)
    df_3 = df_2.na.drop(subset="name")
    df_3.show()

    # Fill values with Null and NaN values
    column_str = "Fill MISSING VALUES"
    do_print_util(column_str)
    df_4 = df_3.na.fill({'age': 10, 'height': 80.1})
    df_4.show()

    # Remove outliers for height by supplying a valid range
    column_str = "REMOVE OUTLIERS"
    do_print_util(column_str)
    df_5 = df_4.where(df_4.height.between(65, 85))
    df_5.show()

    # Remove Duplicates
    column_str = "REMOVE DUPLICATES"
    do_print_util(column_str)
    df_6 = df_5.distinct()
    df_6.show()

    # String manipulation
    # convert to a single and consistance name as lower case
    column_str="STRING MANIPULATION-1"
    do_print_util(column_str)
    df_7 = df_6.withColumn("name", sf.lower("name"))
    df_7.show()

    column_str="STRING MANIPULATION-2"
    do_print_util(column_str)
    capitalize = sf.udf(lambda s: s.capitalize())
    df_8 = df_7.withColumn("name", capitalize("name"))
    df_8.show()

    # Reorder columns 
    column_str = "REORDER COLUMNS"
    do_print_util(column_str)
    df_9 = df_8.select("name", "age", "height")
    df_9.show()

    # Create a new dataframe and make transformations. As data engineer
    # this task is essential.

    column_str = "CREATE A DATAFRAME AND TRANSFORM"
    do_print_util
    df_1 = spark.range(10)
    for i in range(20):
        df_1 = df_1.withColumn(f"col_{i}", sf.lit(i))
    df_1.show()

    df_2 = df_1.select("id", "col_2", "col_3", sf.sqrt(sf.col("col_4") + sf.col("col_5")).alias("sqrt_col_4_plus_5"))
    do_print_util(column_str)
    df_2.show()

    # Filter rows with where()
    column_str = "FILTER ROWS WITH WHERE"
    do_print_util(column_str)
    # select odd number ids
    df_3 = df_2.where(sf.col("id") % 2 == 1)
    df_3.show()

    # One part of data engineering is summarizing data
    column_str = "SUMMARIZING DATA"
    do_print_util(column_str)

    df_1 = spark.createDataFrame([
        Row(incomes=[123.0, 456.0, 789.0], NAME="Alice"),
        Row(incomes=[234.0, 567.0], NAME="BOB"),
        Row(incomes=[100.0, 200.0, 100.0], NAME="Tom"),
        Row(incomes=[79.0, 128.0], NAME="josh"),
        Row(incomes=[123.0, 145.0, 178.0], NAME="bush"),
        Row(incomes=[111.0, 187.0, 451.0, 188.0, 199.0], NAME="jerry"),
    ])

    df_1.show()

    df_2 = df_1.select(sf.lower("NAME").alias("name"), "incomes")
    df_2.show(truncate=False)

    # Reshape data by using explode
    column_str = "RESHAPE BY USING EXPLODE"
    do_print_util(column_str)
    df_3 = df_2.select("name", sf.explode("incomes").alias("income"))
    df_3.show()

    # Summarizing via groupBy() and agg() functions
    column_str = "SUMMARIZING WITH GROUPBY() AND AGG()"
    do_print_util(column_str)
    df_4 = df_3.groupBy("name").agg(sf.avg("income").alias("avg_income"))
    df_4.show()

    # User orderBy() for columns
    column_str = "ORDERING COLUMNS BY NAME"
    do_print_util(column_str)
    df_5 = df_4.orderBy("name")
    df_5.show()

    

