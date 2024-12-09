"""
A combination ChatGPT, CodePilot, and docs used to generate code sample for testing
"""

import sys
sys.path.append('.')

from src.py.sc.utils.print_utils import print_header, print_seperator
from pyspark.sql import SparkSession
from IPython.display import display
import pandas as pd
import numpy as np

if __name__ == "__main__":
    # let's top any existing SparkSession if running at all
    SparkSession.builder.master("local[*]").getOrCreate().stop()

    # Create SparkSession
    spark = (SparkSession
                .builder
                .remote("local[*]")
                .appName("Python Pandas Example 4") 
                .getOrCreate())
    
    # Ensure we are conneccted to the spark session
    assert("<class 'pyspark.sql.connect.session.SparkSession'>" == str(type((spark))))
    print(f"+++++Making sure it's using SparkConnect session:{spark}+++++")

# Create a DataFrame with X million of rows and 25 columns

    num_rows = 20_000_000
    num_cols = 25
    columns = [f'col_{i+1}' for i in range(num_cols)]

    # Random numeric data
    data = np.random.rand(num_rows, num_cols)

    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)

    # Add additional columns for complex operations
    df['group'] = np.random.choice(['A', 'B', 'C', 'D'], size=num_rows)
    df['date'] = pd.date_range(start='2020-01-01', periods=num_rows, freq='T').strftime('%Y-%m-%d %H:%M:%S')
    df['category'] = np.random.choice(['X', 'Y', 'Z'], size=num_rows)

    # Display basic info
    print("DataFrame info:")
    print(df.info())

    # --- Complex Operations ---
    # 1. **Multi-Level GroupBy**: Group by multiple columns and compute aggregations
    operation_str = "COMPLEX DATAFERAME OPERATION:MULILEVEL-GROUPBY"
    print_header(operation_str)
    multi_grouped = df.groupby(['group', 'category']).agg(
        mean_col1=('col_1', 'mean'),
        max_col2=('col_2', 'max'),
        count=('col_3', 'count')
    ).reset_index()
    print("\nMulti-level grouped results:")
    print(multi_grouped.head())

    # 2. **Window Operations**: Add a rolling average and cumulative sum
    operation_str = "COMPLEX DATAFERAME OPERATION:WINDOWING"
    print_header(operation_str)
    df['rolling_avg_col1'] = df['col_1'].rolling(window=100).mean()
    df['cumsum_col2'] = df['col_2'].cumsum()
    print("\nSample after rolling average and cumulative sum:")
    print(df[['col_1', 'rolling_avg_col1', 'col_2', 'cumsum_col2']].head(150))

    # 3. **Pivot Table**: Create a pivot table summarizing data
    operation_str = "COMPLEX DATAFERAME OPERATION:PIVOT TABLE"
    print_header(operation_str)
    pivot_table = df.pivot_table(
        values='col_3',
        index='group',
        columns='category',
        aggfunc='mean'
    )
    print("\nPivot table:")
    print(pivot_table)

    #4. **Apply Custom Function**: Normalize a column using custom logic
    # operation_str = "COMPLEX DATAFERAME OPERATION:CUSTOM FUNCTIONS-UDFs"
    # print_header(operation_str)

    # def normalize_column(series):
    #     return (series - series.min()) / (series.max() - series.min())


    # df['normalized_col1'] = df['col_1'].apply(lambda x: normalize_column(df['col_1']))
    # print("\nSample after normalizing 'col_1':")
    # print(df[['col_1', 'normalized_col1']].head())

    # 5. **Datetime Operations**: Extract date components and analyze trends
    operation_str = "COMPLEX DATAFERAME OPERATION:DATA EXTRACTION"
    print_header(operation_str)
    df['year'] = pd.to_datetime(df['date']).dt.year
    df['month'] = pd.to_datetime(df['date']).dt.month
    monthly_trend = df.groupby(['year', 'month']).agg(
        mean_col1=('col_1', 'mean'),
        sum_col2=('col_2', 'sum')
    ).reset_index()

    print("\nMonthly trends:")
    print(monthly_trend.head())

    # 6. **Merge DataFrames**: Create and merge another DataFrame
    operation_str = "COMPLEX DATAFERAME OPERATION:MERGE"
    print_header(operation_str)
    df_summary = df.groupby('group').agg(total_col1=('col_1', 'sum')).reset_index()
    merged_df = pd.merge(df, df_summary, on='group', how='left')
    print("\nMerged DataFrame:")
    print(merged_df.head())

