

import sys
sys.path.append('.')

import warnings
import uuid
import random
from datetime import datetime, timedelta
import json

warnings.filterwarnings("ignore")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, lit, rand, when, datediff, current_date, col, abs
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from src.py.sc.utils.data_validation_cls  import DataValidation


if __name__ == "__main__":

    # Set number of records to generate by default unless overridden by command line argument
    num_records = 100 
    if len(sys.argv) > 1:
        try:
            num_records = int(sys.argv[1])
        except ValueError:
            print("Invalid number of records specified. Using default value of 100.")
            num_records = 100
    print(f"Generating {num_records} records.")
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("JSON Data Generator") \
        .getOrCreate()

    # Helper function to generate random dates within a range
    def random_date(start_date, end_date):
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        return start_date + timedelta(days=random_number_of_days)

    # Helper function to generate a random SSN
    def random_ssn():
        return f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"

    # Helper function to calculate age from DOB
    def calculate_age(dob):
        today = datetime.now()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return age

    # Generate base data with conformant values
    data = []
    first_names = ["John", "Jane", "Michael", "Emily", "David", "Sarah", "Robert", "Jennifer", "William", "Elizabeth"]
    last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
    airlines = ["Delta", "American Airlines", "United", "Southwest", "JetBlue", "Alaska Airlines", "Spirit", "Frontier"]

    # Generate initial dataset with conformant data
    for i in range(num_records):
        # Generate a random date of birth
        start_date = datetime(1950, 1, 1)
        end_date = datetime(2005, 12, 31)
        dob = random_date(start_date, end_date)
        age = calculate_age(dob)
        
        # Generate mileage and determine status
        mileage = random.randint(0, 200000)
        if mileage > 100000:
            status = "GOLD"
        elif mileage >= 50000:
            status = "SILVER"
        elif mileage >= 25000:
            status = "BRONZE"
        else:
            status = "NONE"
        
        # Generate fares paid
        fares_paid = round(random.uniform(100, 50000), 2)
        
        # Create a record
        record = {
            "unique_id": str(uuid.uuid4()),
            "full_name": f"{random.choice(first_names)} {random.choice(last_names)}",
            # "date_of_birth": dob.strftime("%m/%d/%Y"),  # convert to string format
            "date_of_birth": dob,
            "age": age,
            "ssn": random_ssn(),
            "city": random.choice(cities),
            "state": random.choice(states),
            "airline_of_choice": random.choice(airlines),
            "mileage_flown": mileage,
            "fares_paid_to_date": fares_paid,
            "status": status
        }
        data.append(record)
        # generate a duplicate record for every 50th record
        if i % 25 == 0: 
            data.append(record)

    # Now introduce missing and non-conformant data
    # Modify some records to have issues
    for i in range(len(data)):
        # Every 10th record: set negative mileage but keep status as is (incongruent data)
        if i % 10 == 0:
            data[i]["mileage_flown"] = -random.randint(1, 10000)
        
        # Every 7th record: set wrong status for mileage (non-conformant)
        if i % 7 == 0 and i % 10 != 0:  # Avoid overlap with negative mileage
            data[i]["status"] = random.choice(["PLATINUM", "BASIC", "INVALID"])
        
        # Every 13th record: remove some fields (missing data)
        if i % 13 == 0:
            fields_to_remove = random.sample(["ssn", "city", "state", "airline_of_choice"], 
                                            random.randint(1, 2))
            for field in fields_to_remove:
                data[i].pop(field, None)
        
        # Every 17th record: age doesn't match DOB (incongruent data) 
        if i % 17 == 0:
            data[i]["age"] = random.randint(18, 80)
        
        # Every 23rd record: set mileage to 0
        if i % 23 == 0:
            data[i]["mileage_flown"] = 0
        
        # Every 19th record: set fares_paid to 0 or negative
        if i % 19 == 0:
            if random.random() < 0.5:
                data[i]["fares_paid_to_date"] = 0
            else:
                data[i]["fares_paid_to_date"] = -random.uniform(100, 1000)

    # Create a DataFrame from the data with explicit schema
    schema = StructType([
        StructField("unique_id", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("date_of_birth", DateType(), False),
        StructField("age", IntegerType(), False),
        StructField("ssn", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("airline_of_choice", StringType(), True),
        StructField("mileage_flown", IntegerType(), False),
        StructField("fares_paid_to_date", DoubleType(), False),
        StructField("status", StringType(), False)
    ])

    df = spark.createDataFrame(data, schema)

    # Convert the DataFrame to JSON format
    json_df = df.select(to_json(struct([df[c] for c in df.columns])).alias("json"))

    # Show the first few JSON records
    print("Sample JSON records:")
    json_df.show(5, False)

    # Define output file path
    output_file = "airline_customer_data.json"

    # Write data to file, one JSON object per line (JSON Lines format)
    with open(output_file, "w") as f:
        for entry in data:
            # Convert each entry to JSON and write to file
            # Convert date to string format for JSON serialization
            if isinstance(entry["date_of_birth"], datetime):
                entry["date_of_birth"] = entry["date_of_birth"].strftime("%Y-%m-%d")  # Convert date to string
            f.write(json.dumps(entry) + "\n")
   
    print("\nJSON data has been generated and saved to 'airline_customer_data.json'")

    # Stop the Spark session
    spark.stop()