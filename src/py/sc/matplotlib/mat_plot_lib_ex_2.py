import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    # Initialize Spark Session
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("Matplotlib Example 2") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Sample data for five years
    data = [
        (2020, "January", 100),
        (2020, "February", 120),
        (2020, "March", 140),
        (2021, "January", 110),
        (2021, "February", 115),
        (2021, "March", 135),
        (2022, "January", 105),
        (2022, "February", 125),
        (2022, "March", 150),
        (2023, "January", 108),
        (2023, "February", 118),
        (2023, "March", 145),
        (2024, "January", 112),
        (2024, "February", 130),
        (2024, "March", 160)]
    
    # Define column names
    columns = ["year", "month", "rainfall"]

    spark_df = spark.createDataFrame(data,schema=columns)
    print(spark_df.show())

    # Select months with rain > 100

    print(spark_df.select(spark_df.rainfall > 100).show())

    # Convert to Pandas DataFrame
    df = spark_df.toPandas()
    print(df.head())

    # # Use pandas.pivot() to reshape the data
    pivot_df = df.pivot(index="month", columns="year", values="rainfall")

    # Sort rows by month order (optional, if needed)
    month_order = ["January", "February", "March"]
    pivot_df = pivot_df.reindex(month_order)

    # Display the pivoted DataFrame
    print("\nPivoted DataFrame:")
    print(pivot_df)

    # Plot a heatmap for visualization
    plt.figure(figsize=(8, 6))
    sns.heatmap(pivot_df, annot=True, fmt=".1f", cmap="coolwarm", cbar_kws={"label": "Rainfall (mm)"})
    plt.title("Monthly Rainfall Heatmap")
    plt.ylabel("Month")
    plt.xlabel("Year")
    plt.tight_layout()
    plt.show()