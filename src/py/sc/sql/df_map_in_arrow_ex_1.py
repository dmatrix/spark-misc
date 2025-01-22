import os
import sys
sys.path.append('.')

from src.py.sc.utils.spark_session_cls import SparkConnectSession
from src.py.sc.utils.spark_session_cls import DatabrckSparkSession
from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from faker import Faker
import pyarrow as pa

from typing import Iterator

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
                                app_name="PySpark MapInArrow Example").get()
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")
    print_seperator(size=20)

    NUM_OF_ROWS = 750_000
    NUM_OF_COLUMNS = 6

    # Step 2: Generate a large DataFrame with NUM_OF_ROWS rows and six columns
    print_header(f"Create an initial DataFrame of {NUM_OF_ROWS} rows:")
    fake = Faker()
    data = [(i, fake.name(), fake.job(), fake.random_int(20, 60), fake.pyfloat(positive=True, right_digits=2), fake.state()) 
            for i in range(1, NUM_OF_ROWS + 1)]
    columns = ["id", "name", "job", "age", "salary", "state"]

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("job", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", FloatType(), True),
        StructField("state", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema)
    print(df.show())
    print_seperator(size=15)

    print_header("Convert PySpark DataFrame --> PyArrow Table:")
    print(df.toArrow().slice(length=5))
    print_seperator(size=15)

    # Step 3: Define a PyArrow-based user-defined function for mapInArrow
    def process_record_batch(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        """
        Processes each RecordBatch in PyArrow and performs a transformation:
        - Adds 10% to the 'salary' column.
        - Concatenates 'name' and 'job' into a new column 'full_description'.
        """
        for idx, batch in enumerate(batches):
            # Convert RecordBatch to a Pandas DataFrame
            print(f"Processing batch: {idx+1} of {batch.num_rows} rows...")
            table = batch.to_pandas()

            # Perform transformations
            table['salary'] = table['salary'] * 1.10  # Increase salary by 10%
            table['full_description'] = table['name'] + " - " + table['job']  # Concatenate name and job

            # Convert back to PyArrow RecordBatch and yield
            yield pa.RecordBatch.from_pandas(table)

    # Step 4: Apply mapInArrow to the DataFrame
    print_header("Apply mapInArrow to process batches of transformation:")
    result_df = df.mapInArrow(process_record_batch, schema=schema.add(StructField("full_description", StringType(), True)))

    # Step 5: Show a few rows from the resulting DataFrame
    result_df.show(10, truncate=False)
    print_seperator()

    # Stop the Spark session
    spark.stop()