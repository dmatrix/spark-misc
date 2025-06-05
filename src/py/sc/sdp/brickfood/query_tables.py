from pyspark.sql import DataFrame, SparkSession

spark_db_location = "file:////Users/jules/git-repos/spark-misc/src/py/sc/sdp/brickfood/spark-warehouse/"
spark = (SparkSession.builder.appName("QueryOrders")
         .config("spark.sql.warehouse.dir", spark_db_location)
        .enableHiveSupport()
        .getOrCreate())

def query_orders_mv(db_name: str) -> DataFrame:
    """
    Query table that returns a DataFrame with order items.
    
    Returns:
        pyspark.sql.DataFrame: DataFrame containing order items.
    """
   
    df = spark.read.table(db_name)

    
    # Optionally, you can perform some transformations or actions on the DataFrame
    # For example, filtering or selecting specific columns
    df = df.filter(df.status == "approved").select("order_id", "order_item", "price")
    return df

def main():
    # Specify the database name
    db_name = "orders_mv"
    
    # Query the orders materialized view–
    orders_df = query_orders_mv(db_name)
    
    # Show the DataFrame
    print(orders_df.show(truncate=False))

if __name__ == "__main__":
    main()
    # Uncomment the line below to run the main function when this script is executed