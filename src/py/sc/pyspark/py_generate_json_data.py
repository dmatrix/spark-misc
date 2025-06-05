from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType
import random
import json
import os
from faker import Faker

# Initialize Faker to generate realistic data
fake = Faker()

# Create Spark session
spark = SparkSession.builder \
    .appName("Generate JSON Data") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("email", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("salary", FloatType(), True)
])

# US states data
states = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida",
    "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
    "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska",
    "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
    "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]

# Function to generate valid entries
def generate_valid_entry():
    first_name = fake.first_name()
    last_name = fake.last_name()
    name = f"{first_name} {last_name}"
    state = random.choice(states)
    
    return {
        "name": name,
        "age": random.randint(18, 85),
        "city": fake.city(),
        "state": state,
        "email": fake.email(),
        "is_active": random.choice([True, False]),
        "salary": round(random.uniform(25000.0, 150000.0), 2)
    }

# Function to generate invalid entries - deliberately introducing errors
def generate_invalid_entry():
    # Select a random type of error to introduce
    error_type = random.randint(1, 5)
    entry = generate_valid_entry()
    
    if error_type == 1:
        # Missing field
        field_to_remove = random.choice(list(entry.keys()))
        entry.pop(field_to_remove)
    elif error_type == 2:
        # Wrong data type for age (string instead of int)
        entry["age"] = fake.word()
    elif error_type == 3:
        # Wrong data type for is_active (string instead of boolean)
        entry["is_active"] = "yes" if random.random() > 0.5 else "no"
    elif error_type == 4:
        # Wrong data type for salary (string instead of float)
        entry["salary"] = f"${entry['salary']}"
    elif error_type == 5:
        # Add an extra field that's not in the schema
        entry["extra_field"] = fake.word()
    
    return entry

# Generate 10,000 entries with 25% being invalid
num_entries = 10000
invalid_entries_count = int(num_entries * 0.25)
valid_entries_count = num_entries - invalid_entries_count

# Generate the data
data = []
for _ in range(valid_entries_count):
    data.append(generate_valid_entry())
for _ in range(invalid_entries_count):
    data.append(generate_invalid_entry())

# Shuffle the data to mix valid and invalid entries
random.shuffle(data)

# Define output file path
output_file = "generated_data.json"

# Write data to file, one JSON object per line (JSON Lines format)
with open(output_file, "w") as f:
    for entry in data:
        f.write(json.dumps(entry) + "\n")

# Verify we can read it with Spark (this would validate the valid entries)
try:
    df = spark.read.schema(schema).json(output_file)
    print(f"Successfully loaded valid entries. Total valid entries: {df.count()}")
    print("Sample of 5 valid entries:")
    df.show(5, truncate=False)
except Exception as e:
    print(f"Error when loading with schema due to invalid entries (expected): {str(e)}")

# Read without schema to show all entries
df_all = spark.read.json(output_file)
print(f"Total entries (valid and invalid): {df_all.count()}")
print("Sample of data (including potentially invalid entries):")
df_all.show(5, truncate=False)

print(f"Generated {num_entries} JSON entries ({invalid_entries_count} invalid) in {output_file}")

# Stop Spark session
spark.stop()