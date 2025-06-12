from pyspark.sql import DataFrame, SparkSession

# This script queries a materialized view in Spark and returns a DataFrame with order items.
# Run this script to retrieve the data from the specified materialized view.
# Create by a previous run of Spark Declarative Pipelines (SDP) command line: spark-pipelines

# Location of the Spark database has all the materialized views
spark_db_location = "file:////Users/jules/git-repos/spark-misc/src/py/sc/sdp/brickfood/spark-warehouse/"

# Initialize Spark session with Hive support
spark = (SparkSession.builder.appName("QueryOrders")
         .config("spark.sql.warehouse.dir", spark_db_location)
         .enableHiveSupport()
         .getOrCreate())

# Function to query the materialized view and return a DataFrame
# This function reads from the specified database and returns a DataFrame with order items.
# The DataFrame can be further processed or analyzed as needed.
# The database name should match the one used when creating the materialized view.
# The DataFrame will contain columns such as order_id, order_item, price, etc.
# The function can also include filtering or selecting specific columns as needed.
# The DataFrame returned by this function can be used for further analysis or processing.
def query_orders_mv(db_name: str) -> DataFrame:
    """
    Query table that returns a DataFrame with order items.
    
    Returns:
        pyspark.sql.DataFrame: DataFrame containing order items.
    """
   
    df = spark.read.table(db_name)

    # Optionally, you can perform some transformations or actions on the DataFrame
    # For example, filtering or selecting specific columns
    df = df.filter(df.status == "approved").select("order_id", "status", "order_item", "price")
    return df

def main():
    # Specify the materialized view database table name
    # This should match the name used when creating the materialized view.
    # Ensure that the database name is correct and exists in the Spark warehouse.
    db_name = "orders_mv"
    
    # Query the orders materialized view–
    orders_df = query_orders_mv(db_name)
    
    # Show the DataFrame
    print(orders_df.show(10, truncate=False))

if __name__ == "__main__":
    main()
