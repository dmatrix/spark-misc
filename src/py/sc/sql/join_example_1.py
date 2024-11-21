#
# Examples from pyspark-cookbook guide
#
from pyspark.sql import Row
from pyspark.sql import SparkSession
from utils import do_print_util

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

    column_str = "THE ART OF JOINING....(DEFAULT:INNER JOIN)"
    do_print_util(column_str)

    # Create a DataFrame using Rows
    df_1 = spark.createDataFrame([
        Row(age=10, height=80.0, name="alice"),
        Row(age=9, height=78.9, name="josh"),
        Row(age=18, height=82.3, name="bush"),
        Row(age=7, height=75.3, name="tom"),
])

    df_2 = spark.createDataFrame([
        Row(incomes=[123.0, 456.0, 789.0], name="alice"),
        Row(incomes=[234.0, 567.0], name="bob"),
        Row(incomes=[79.0, 128.0], name="josh"),
        Row(incomes=[123.0, 145.0, 178.0], name="bush"),
        Row(incomes=[111.0, 187.0, 451.0, 188.0, 199.0], name="jerry"),
    ])

    # Join by name
    df_3 = df_1.join(df_2, on="name")
    df_3.show(truncate=False)

    # Let’s take LEFT join as another example. A left join includes all of the records 
    # from the first (left) of two tables, even if there are no matching values for records 
    # in the second (right) table.
    column_str = "THE ART OF JOINING....:LEFT JOIN)"
    do_print_util(column_str)
    df_4 = df_1.join(df_2, on="name", how="left")
    df_4.show(truncate=False)
    
    # And a RIGHT join keeps all of the records from the right table.
    column_str = "THE ART OF JOINING....:RIGHT JOIN)"
    do_print_util(column_str)
    df_5 = df_1.join(df_2, on="name", how="right")
    df_5.show(truncate=False)

