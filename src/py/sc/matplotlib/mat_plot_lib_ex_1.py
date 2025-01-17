"""
ChatGPT, CodePilot, and docs used to generate some or partial code sample for testing
"""
import os
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

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
                                app_name="Matplotlib Example 1").get()

    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Create sample data for five years
    data = [
    (2020, "January", 100, 15, 5),
    (2020, "February", 120, 18, 7),
    (2020, "March", 140, 20, 10),
    (2021, "January", 110, 16, 6),
    (2021, "February", 115, 19, 8),
    (2021, "March", 135, 21, 11),
    (2022, "January", 105, 14, 4),
    (2022, "February", 125, 17, 6),
    (2022, "March", 150, 22, 12),
    (2023, "January", 108, 15, 5),
    (2023, "February", 118, 18, 7),
    (2023, "March", 145, 23, 13),
    (2024, "January", 112, 15, 6),
    (2024, "February", 130, 20, 10),
    (2024, "March", 160, 24, 14),
    ]

    columns = ["year", "month", "rainfall", "max_temp", "min_temp"]

    # Create PySpark DataFrame
    df = spark.createDataFrame(data, schema=columns)

    # Convert to Pandas for Matplotlib visualization
    pdf = df.toPandas()

    # Step 1: Monthly Rainfall Bar Plot
    plt.figure(figsize=(10, 6))
    pdf.groupby("month")["rainfall"].mean().reindex(["January", "February", "March"]).plot(kind="bar", color="blue")
    plt.title("Average Monthly Rainfall")
    plt.ylabel("Rainfall (mm)")
    plt.xlabel("Month")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig("monthly_rainfall_barplot.png")
    plt.show()

    # Step 2: Max and Min Temperatures by Year Line Plot
    plt.figure(figsize=(10, 6))
    pdf.groupby("year")["max_temp"].mean().plot(label="Max Temp", marker="o", color="red")
    pdf.groupby("year")["min_temp"].mean().plot(label="Min Temp", marker="o", color="blue")
    plt.title("Average Max and Min Temperatures by Year")
    plt.ylabel("Temperature (°C)")
    plt.xlabel("Year")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("yearly_temperature_lineplot.png")
    plt.show()

    # Step 3: Scatter Plot of Rainfall vs Max Temperature
    plt.figure(figsize=(10, 6))
    plt.scatter(pdf["rainfall"], pdf["max_temp"], c="green", alpha=0.7)
    plt.title("Rainfall vs Max Temperature")
    plt.xlabel("Rainfall (mm)")
    plt.ylabel("Max Temperature (°C)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("rainfall_vs_temp_scatter.png")
    plt.show()

    # Step 4: Box Plot of Monthly Rainfall
    plt.figure(figsize=(10, 6))
    pdf.boxplot(column="rainfall", by="month", grid=False)
    plt.title("Monthly Rainfall Distribution")
    plt.suptitle("")
    plt.ylabel("Rainfall (mm)")
    plt.xlabel("Month")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("monthly_rainfall_boxplot.png")
    plt.show()

    spark.stop()