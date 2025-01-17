"""
A combination ChatGPT, CodePilot, and docs used to generate code sample for testing
"""
import os 
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")  # Ignore warnings coming from Arrow optimizations. 
from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator
import random
import pyspark.pandas as ps
from faker import Faker
from dateutil.relativedelta import relativedelta
from datetime import datetime

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
                                app_name="PySpark Pandas Example 2").get()
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

    # Set default options for PySpark Pandas
    ps.set_option("compute.default_index_type", "distributed")

    # Initialize Faker for generating random names
    fake = Faker()

    # Function to generate random dates
    def random_date(start_date, end_date):
        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        return start_date + relativedelta(days=random_days)

    # Constants
    NUM_ROWS = 275_000
    START_DATE = datetime(2000, 1, 1)
    END_DATE = datetime(2023, 1, 1)

    # Generate data for the main dataset
    data = {
        "Employee_ID": range(1, NUM_ROWS + 1),
        "First_Name": [fake.first_name() for _ in range(NUM_ROWS)],
        "Last_Name": [fake.last_name() for _ in range(NUM_ROWS)],
        "Age": [random.randint(18, 65) for _ in range(NUM_ROWS)],
        "Department": [random.choice(["HR", "IT", "Finance", "Engineering", "R&D","Marketing", "Sales"]) for _ in range(NUM_ROWS)],
        "Salary": [random.randint(30000, 120000) for _ in range(NUM_ROWS)],
        "Join_Date": [random_date(START_DATE, END_DATE).strftime("%Y-%m-%d") for _ in range(NUM_ROWS)],
        "City": [fake.city() for _ in range(NUM_ROWS)],
        "Performance_Rating": [random.choice(["Poor", "Average", "Good", "Excellent"]) for _ in range(NUM_ROWS)],
        "Remote_Work": [random.choice([True, False]) for _ in range(NUM_ROWS)],
    }

    # Create a PySpark Pandas DataFrame
    df = ps.DataFrame(data)
    print(f"++++Ensure it is PySpark Pandas datatype++++:{type(df)}")
    print_header(f"Generated Main DataFrame with { NUM_ROWS} rows:")
    print(df.head())
    print_seperator()

    # Create a department-location DataFrame for join
    departments_data = {
        "Department": ["HR", "IT", "Finance", "Marketing", "Sales","Engineering","R&D"],
        "Location": ["New York", "San Francisco", "London", "Paris", "Sydney", "San Francisco", "New York"],
        "Department_Budget": [200000, 500000, 300000, 250000, 400000, 800000, 100000],
    }

    # Create PySpark Pandas DataFrame
    departments_df = ps.DataFrame(departments_data)

    print_header("Department DataFrame:")
    print(departments_df)
    print_seperator()

    # Join operation: Merge the main dataset with department-location data
    joined_df = df.merge(departments_df, on="Department", how="left")
    print_header("Joined DataFrame (with Department Locations and Budgets):")
    print(joined_df.head())
    print_seperator()

    # Perform operations
    # Select specific columns
    selected_df = joined_df[["First_Name", "Last_Name", "Department", "Salary", "Location"]]
    print_header("Selected Columns (First_Name, Last_Name, Department, Salary, Location):")
    print(selected_df.head())
    print_seperator()

    # Complex groupby: Average Salary and Employee Count by Department
    grouped_df = joined_df.groupby("Department").agg(
        Avg_Salary=("Salary", "mean"),
        Employee_Count=("Employee_ID", "count"),
    )
    print_header("Groupby Operations (Avg Salary and Employee Count by Department):")
    print(grouped_df)
    print_seperator()

    # Filter employees with Salary > 80000 and Age < 40
    filtered_df = joined_df[(joined_df["Salary"] > 80000) & (joined_df["Age"] < 40)]
    print_header("Filtered DataFrame (Salary > 80000 and Age < 40):")
    print(filtered_df.head())
    print_seperator()

    # Add a UDF to categorize age
    def age_category(age):
        if age < 30:
            return "Young"
        elif age < 50:
            return "Mid-aged"
        else:
            return "Senior"

    joined_df["Age_Category"] = joined_df["Age"].apply(age_category)
    print_header("DataFrame with Age Category:")
    print(joined_df[["Age", "Age_Category"]].head())
    print_seperator()

    # Save the resulting DataFrame to a CSV
    if mode != "dbconnect":
        output_path = "output/large_dataset_with_join.csv"
        joined_df.to_csv(output_path)
        print(f"\nGenerated DataFrame saved to {output_path}")