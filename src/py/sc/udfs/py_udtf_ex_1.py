import sys
sys.path.append('.')
import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf
from pyspark.sql.functions import lit, col

# Define the UDTF function
@udtf(returnType="start_city: string, end_city: string, miles: int, hrs_by_car: float, hrs_by_plane: float, plane_fair_dollar: float")
class TravelDuration:
    def __init__(self):
        self.car_speed = 65  # mph
        self.plane_speed = 350  # mph
        self.plane_fair_per_hour = 200

    def eval(self, start_city: str, end_city: str, miles: float):
        hrs_by_car = miles / self.car_speed
        hrs_by_plane = miles / self.plane_speed
        plane_fair_us_dollar = self.plane_fair_per_hour * hrs_by_plane
        yield start_city, end_city, miles, hrs_by_car, hrs_by_plane, plane_fair_us_dollar

if __name__ == "__main__":
    spark = (SparkSession.builder 
                .remote("local[*]") 
                .appName("PySpark UDTF Example 1")
                .getOrCreate())

    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Create a distance DataFrame
    distances_df = spark.createDataFrame([
        ("New York", "Boston", 190),
        ("Los Angeles", "San Francisco", 2375),
        ("Chicago", "Miami", 1197),
        ("Houston", "Dallas", 239),
        ("San Francisco", "Seattle", 810) ], 
        ["start_city", "end_city", "miles"])
    
    distances_df.show(truncate=False)

    # Use our UDTF to test it; this should return a single line
    TravelDuration(lit("New York"), lit("Boston"), lit(190)).show()

    # Apply our UDTF with a lateral join for transformation
    transfrom_df = distances_df.lateralJoin(TravelDuration(col("start_city").outer(), col("end_city").outer(), col("miles")))

    spark.stop()





