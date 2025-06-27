from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import plotly.express as px

# Location of the Spark database with materialized views
spark_db_location = "file:////Users/jules/git-repos/spark-misc/src/py/sc/sdp/oil_rigs/spark-warehouse/"

# Initialize Spark session with Hive support and plotly integration
spark = (SparkSession.builder.appName("OilRigTemperaturePlot")
         .config("spark.sql.warehouse.dir", spark_db_location)
         .config("spark.sql.plotting.backend", "plotly")  # Enable plotly backend
         .enableHiveSupport()
         .getOrCreate())

def plot_temperature_data():
    """
    Create a time series plot of temperature readings from both oil rigs
    using Spark's native plotly.express integration directly with Spark DataFrame.
    """
    # Read the temperature events materialized view
    temp_df = spark.read.table("temperature_events_mv")
    
    # Create the plot using Spark's native plotly express
    # No need to convert to pandas
    fig = px.line(
        data_frame=temp_df,
        x="timestamp",
        y="temperature_f",
        color="rig_name",
        title="Oil Rig Temperature Readings Over Time",
        labels={
            "timestamp": "Time",
            "temperature_f": "Temperature (°F)",
            "rig_name": "Oil Rig"
        },
        template="plotly_white"
    )
    
    # Customize the layout
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Temperature (°F)",
        legend_title="Oil Rig",
        hovermode='x unified'
    )
    
    # Add hover data formatting
    fig.update_traces(
        hovertemplate="<br>".join([
            "Time: %{x}",
            "Temperature: %{y:.1f}°F"
        ])
    )
    
    # Save the plot as an HTML file
    output_file = "temperature_plot.html"
    fig.write_html(output_file)
    print(f"Plot saved as {output_file}")
    
    # Display some basic statistics using Spark SQL functions
    print("\nTemperature Statistics by Oil Rig:")
    print("=" * 50)
    stats_df = temp_df.groupBy("rig_name").agg({
        "temperature_f": "min",
        "temperature_f": "max",
        "temperature_f": "avg"
    }).withColumnRenamed("min(temperature_f)", "min_temp").\
       withColumnRenamed("max(temperature_f)", "max_temp").\
       withColumnRenamed("avg(temperature_f)", "avg_temp")
    
    stats_df.show(truncate=False)

def main():
    try:
        plot_temperature_data()
    except Exception as e:
        print(f"Error creating temperature plot: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 