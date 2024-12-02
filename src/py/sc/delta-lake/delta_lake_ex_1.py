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

# Write the Pandas DataFrame to a Delta table
write_deltalake(delta_table_path, df, mode="overwrite")
print("\nInitial Delta Table:")
print(DeltaTable(delta_table_path).files())

# Step 2: Read the Delta table
delta_table = DeltaTable(delta_table_path)
df_read = delta_table.to_pandas()
print("\nRead Delta Table:")
print(df_read)

# Step 3: Update the Delta table
# Add two years to the Join_Date column
df_read["Join_Date"] = df_read["Join_Date"].apply(lambda d: d + relativedelta(years=2))
write_deltalake(delta_table_path, df_read, mode="overwrite")

print("\nDelta Table After Updating Join_Date:")
print(DeltaTable(delta_table_path).to_pandas())

# Step 4: Append new rows to the Delta table
new_data = {
    "ID": [4, 5],
    "Name": ["David", "Eva"],
    "Age": [35, 28],
    "Join_Date": [datetime(2023, 12, 1), datetime(2024, 1, 10)],
}
df_new = pd.DataFrame(new_data)
write_deltalake(delta_table_path, df_new, mode="append")

print("\nDelta Table After Appending Rows:")
df_new = DeltaTable(delta_table_path).to_pandas()
print(df_new)

# Step 5: sort the table by Age
df_age_sorted = df_new.sort_values(by="Age", ascending=False)
print(f"sorted by age: {df_age_sorted}")

