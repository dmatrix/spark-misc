import os
import sys
sys.path.append('.')

import warnings
warnings.filterwarnings("ignore")

from src.py.sc.utils.print_utils import print_seperator, print_header

import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Define Delta table path
delta_table_path = "/tmp/tables/delta/delta_table_example"

# Step 1: Create a Delta table using Pandas DataFrame
data = {
    "ID": [1, 2, 3],
    "Name": ["Alice", "Bob", "Cathy"],
    "Age": [25, 30, 27],
    "Join_Date": [datetime(2021, 1, 1), datetime(2022, 6, 15), datetime(2023, 3, 10)],
}
df = pd.DataFrame(data)
df_read = df

print_header("INITIAL PANDAS DATAFRAME:")
print(df)
print_seperator("---")

# Write the Pandas DataFrame to a Delta table
print_header("CREATE A DELTA TABLE & LIST FILES:")
write_deltalake(delta_table_path, df, mode="overwrite")
print(DeltaTable(delta_table_path).files())
print_seperator("----")

# Step 2: Read the Delta table
print_header("\nREAD THE DELTA TABLE")
delta_table = DeltaTable(delta_table_path)
try:
    df_read = delta_table.to_pandas()
except OSError:
    print("OSError: Repetition level histogram size mismatch: Need to update pyarrow to 19.0.1")
print(df_read)
print_seperator("----")

# Step 3: Update the Delta table
# Add two years to the Join_Date column
print_header("DELTA TABLE AFTER UPDATEING JOIN_DATES:")
df_read["Join_Date"] = df_read["Join_Date"].apply(lambda d: d + relativedelta(years=2))
write_deltalake(delta_table_path, df_read, mode="overwrite")

try:

    print(DeltaTable(delta_table_path).to_pandas())
except OSError:
     print("OSError: Repetition level histogram size mismatch: Need to update pyarrow to 19.0.1")
     print(df_read)
     print_seperator("----")

# Step 4: Append new rows to the Delta table
new_data = {
    "ID": [4, 5],
    "Name": ["David", "Eva"],
    "Age": [35, 28],
    "Join_Date": [datetime(2023, 12, 1), datetime(2024, 1, 10)],
}
df_new = pd.DataFrame(new_data)
write_deltalake(delta_table_path, df_new, mode="append")

print_header("\nDelta Table After Appending Rows:")
try:

    df_new = DeltaTable(delta_table_path).to_pandas()
except OSError:
    print("OSError: Repetition level histogram size mismatch: Need to update pyarrow to 19.0.1")
    print(df_new)
    print_seperator("----")

# Step 5: sort the table by Age
df_age_sorted = df_new.sort_values(by="Age", ascending=False)
print_header("sorted by age:")
print(f"sorted dataframe: {df_age_sorted}")

