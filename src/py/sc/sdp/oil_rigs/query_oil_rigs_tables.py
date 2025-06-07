from pyspark.sql import DataFrame, SparkSession

# This script queries materialized views for oil rig sensor data and displays sample rows
# Run this script to retrieve data from all sensor-related materialized views
# Created by Spark Declarative Pipelines (SDP)

# Location of the Spark database with all materialized views
spark_db_location = "file:////Users/jules/git-repos/spark-misc/src/py/sc/sdp/oil_rigs/spark-warehouse/"

# Initialize Spark session with Hive support
spark = (SparkSession.builder.appName("QueryOilRigSensors")
         .config("spark.sql.warehouse.dir", spark_db_location)
         .enableHiveSupport()
         .getOrCreate())

def query_mv(mv_name: str) -> DataFrame:
    """
    Query a materialized view and return its DataFrame.
    
    Args:
        mv_name (str): Name of the materialized view to query
    
    Returns:
        pyspark.sql.DataFrame: DataFrame containing the materialized view data
    """
    return spark.read.table(mv_name)

def display_mv_data(mv_name: str, description: str):
    """
    Query and display data from a materialized view.
    
    Args:
        mv_name (str): Name of the materialized view to query
        description (str): Description of the data being displayed
    """
    print(f"\n{description}:")
    print("=" * 80)
    df = query_mv(mv_name)
    df.show(10, truncate=True)
    print(f"Total records: {df.count()}\n")

def main():
    # List of materialized views and their descriptions
    views = [
        ("permian_rig_mv", "Permian Basin Oil Rig Sensor Data"),
        ("eagle_ford_rig_mv", "Eagle Ford Shale Oil Rig Sensor Data"),
        ("temperature_events_mv", "Temperature Readings from All Rigs"),
        ("pressure_events_mv", "Pressure Readings from All Rigs"),
        ("water_level_events_mv", "Water Level Readings from All Rigs")
    ]
    
    # Query and display data from each materialized view
    for mv_name, description in views:
        try:
            display_mv_data(mv_name, description)
        except Exception as e:
            print(f"Error querying {mv_name}: {str(e)}\n")

if __name__ == "__main__":
    main() 