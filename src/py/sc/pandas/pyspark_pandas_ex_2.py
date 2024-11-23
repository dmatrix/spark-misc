import sys
sys.path.append('.')

import pyspark.pandas as ps
import random
from faker import Faker
from dateutil.relativedelta import relativedelta
from datetime import datetime
from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import SparkSession


if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("sc://localhost")
                .appName("PySpark Pandas Example 2") 
                .getOrCreate())
    
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
    NUM_ROWS = 100000
    START_DATE = datetime(2000, 1, 1)
    END_DATE = datetime(2023, 1, 1)

    # Generate data for the main dataset
    data = {
        "Employee_ID": range(1, NUM_ROWS + 1),
        "First_Name": [fake.first_name() for _ in range(NUM_ROWS)],
        "Last_Name": [fake.last_name() for _ in range(NUM_ROWS)],
        "Age": [random.randint(18, 65) for _ in range(NUM_ROWS)],
        "Department": [random.choice(["HR", "IT", "Finance", "Marketing", "Sales"]) for _ in range(NUM_ROWS)],
        "Salary": [random.randint(30000, 120000) for _ in range(NUM_ROWS)],
        "Join_Date": [random_date(START_DATE, END_DATE).strftime("%Y-%m-%d") for _ in range(NUM_ROWS)],
        "City": [fake.city() for _ in range(NUM_ROWS)],
        "Performance_Rating": [random.choice(["Poor", "Average", "Good", "Excellent"]) for _ in range(NUM_ROWS)],
        "Remote_Work": [random.choice([True, False]) for _ in range(NUM_ROWS)],
    }

    # Convert to PySpark Pandas DataFrame
    df = ps.DataFrame(data)
    print(f"++++Ensure it is PySpark Pandas datatype++++:{type(df)}")
    print("Generated Main DataFrame with 100,000 rows:")
    print(df.head())

    # Create a department-location DataFrame for join
    departments_data = {
        "Department": ["HR", "IT", "Finance", "Marketing", "Sales"],
        "Location": ["New York", "San Francisco", "London", "Paris", "Sydney"],
        "Department_Budget": [200000, 500000, 300000, 250000, 400000],
    }
    departments_df = ps.DataFrame(departments_data)

    print("\nDepartment DataFrame:")
    print(departments_df)

    # Join operation: Merge the main dataset with department-location data
    joined_df = df.merge(departments_df, on="Department", how="left")
    print("\nJoined DataFrame (with Department Locations and Budgets):")
    print(joined_df.head())

    # Perform operations
    # Select specific columns
    selected_df = joined_df[["First_Name", "Last_Name", "Department", "Salary", "Location"]]
    print("\nSelected Columns (First_Name, Last_Name, Department, Salary, Location):")
    print(selected_df.head())

    # Complex groupby: Average Salary and Employee Count by Department
    grouped_df = joined_df.groupby("Department").agg(
        Avg_Salary=("Salary", "mean"),
        Employee_Count=("Employee_ID", "count"),
    )
    print("\nGroupby Operations (Avg Salary and Employee Count by Department):")
    print(grouped_df)

    # Filter employees with Salary > 80000 and Age < 40
    filtered_df = joined_df[(joined_df["Salary"] > 80000) & (joined_df["Age"] < 40)]
    print("\nFiltered DataFrame (Salary > 80000 and Age < 40):")
    print(filtered_df.head())

    # Add a UDF to categorize age
    def age_category(age):
        if age < 30:
            return "Young"
        elif age < 50:
            return "Mid-aged"
        else:
            return "Senior"

    joined_df["Age_Category"] = joined_df["Age"].apply(age_category)
    print("\nDataFrame with Age Category:")
    print(joined_df[["Age", "Age_Category"]].head())

    # Save the resulting DataFrame to a CSV
    output_path = "output/large_dataset_with_join.csv"
    joined_df.to_csv(output_path)
    print(f"\nGenerated DataFrame saved to {output_path}")