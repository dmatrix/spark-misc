from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, round, sum

# This script calculates total order prices and sales tax for approved orders
# Run this script to compute 15% sales tax on total order amounts

# Location of the Spark database has all the materialized views
spark_db_location = "file:////Users/jules/git-repos/spark-misc/src/py/sc/sdp/brickfood/spark-warehouse/"

# Initialize Spark session with Hive support
spark = (SparkSession.builder.appName("CalculateOrderTotalsAndTax")
         .config("spark.sql.warehouse.dir", spark_db_location)
         .enableHiveSupport()
         .getOrCreate())

def query_orders_mv(db_name: str, tax_rate: float = 0.15) -> DataFrame:
    """
    Calculate total order prices and sales tax for approved orders.
    
    Args:
        db_name (str): Name of the materialized view to query
        tax_rate (float): Sales tax rate (default: 0.15 for 15%)
    
    Returns:
        pyspark.sql.DataFrame: DataFrame containing order items with total prices and tax calculations
    """
    # Read the base table
    df = spark.read.table(db_name)
    
    # Filter for approved orders and calculate totals and tax
    taxed_orders = (df.filter(col("status") == "approved")
                   .select(
                       "order_id",
                       "order_item",
                       "price",
                       "items_ordered",
                       "date_ordered",
                       # Calculate total price for the order
                       round(col("price") * col("items_ordered"), 2).alias("total_price"),
                       # Calculate tax on total price
                       round(col("price") * col("items_ordered") * tax_rate, 2).alias("sales_tax"),
                       # Calculate final total with tax
                       round(col("price") * col("items_ordered") * (1 + tax_rate), 2).alias("total_with_tax")
                   ))
    
    return taxed_orders

def main():
    # Specify the materialized view database table name
    db_name = "orders_mv"
    
    # Calculate totals and tax for approved orders
    taxed_orders_df = query_orders_mv(db_name)
    
    # Show the results
    print("\nApproved Orders with Total Prices and Sales Tax (15%):")
    taxed_orders_df.show(10, truncate=False)
    
    # Calculate and show summary statistics
    print("\nSummary Statistics:")
    summary_df = (taxed_orders_df
                 .select(
                     round(sum("total_price"), 2).alias("total_sales_before_tax"),
                     round(sum("sales_tax"), 2).alias("total_tax_collected"),
                     round(sum("total_with_tax"), 2).alias("total_sales_with_tax")
                 ))
    summary_df.show(truncate=False)
    
    # Show breakdown by order item
    print("\nBreakdown by Order Item:")
    item_summary = (taxed_orders_df
                   .groupBy("order_item")
                   .agg(
                       round(sum("total_price"), 2).alias("total_sales_before_tax"),
                       round(sum("sales_tax"), 2).alias("total_tax_collected"),
                       round(sum("total_with_tax"), 2).alias("total_sales_with_tax")
                   )
                   .orderBy("total_sales_with_tax", ascending=False))
    item_summary.show(truncate=False)

if __name__ == "__main__":
    main()