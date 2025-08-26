# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamWriter,
    WriterCommitMessage
)
from pyspark.sql.types import StructType
import dlt
import requests
import json
from datetime import datetime

# COMMAND ----------

class RequestBinDataSource(DataSource):
    """
    Custom Python data source that writes to requestbin.com
    """
    def __init__(self, options):
        self.options = options

    @classmethod
    def name(cls):
        return "requestbin_sink"

    def schema(self):
        return "order_id string, customer_name string, product string, amount int, timestamp timestamp"

    def streamWriter(self, schema: StructType, overwrite: bool):
        return RequestBinStreamWriter(self.options)

# COMMAND ----------

class SimpleCommitMessage(WriterCommitMessage):
    def __init__(self, partition_id, count, success_count=0):
        self.partition_id = partition_id
        self.count = count
        self.success_count = success_count

class RequestBinStreamWriter(DataSourceStreamWriter):
    def __init__(self, options):
        self.options = options
        # You'll need to replace this with your actual requestbin URL
        # Go to https://requestbin.com and create a new bin
        self.requestbin_url = options.get("requestbin_url", "https://requestbin.com/YOUR_BIN_ID")

    def write(self, iterator):
        from pyspark import TaskContext
        context = TaskContext.get()
        partition_id = context.partitionId()
        cnt = 0
        success_cnt = 0
        
        # Collect rows to send in batch
        batch_data = []
        for row in iterator:
            cnt += 1
            # Convert row to dictionary
            row_dict = {
                "order_id": row[0],
                "customer_name": row[1], 
                "product": row[2],
                "amount": row[3],
                "timestamp": row[4].isoformat() if row[4] else None,
                "partition_id": partition_id,
                "batch_time": datetime.now().isoformat()
            }
            batch_data.append(row_dict)
        
        # Send batch to requestbin
        if batch_data:
            try:
                payload = {
                    "batch_id": f"batch_{partition_id}_{datetime.now().timestamp()}",
                    "partition_id": partition_id,
                    "record_count": len(batch_data),
                    "records": batch_data
                }
                
                response = requests.post(
                    self.requestbin_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )
                
                if response.status_code == 200:
                    success_cnt = cnt
                    print(f"Successfully sent {cnt} records to RequestBin")
                else:
                    print(f"Failed to send to RequestBin: {response.status_code}")
                    
            except Exception as e:
                print(f"Error sending to RequestBin: {str(e)}")
                
        return SimpleCommitMessage(partition_id=partition_id, count=cnt, success_count=success_cnt)

    def commit(self, messages, batchId) -> None:
        total_rows = sum(msg.count for msg in messages)
        success_rows = sum(msg.success_count for msg in messages)
        print(f"Batch {batchId} committed: {success_rows}/{total_rows} rows successfully sent to RequestBin")

    def abort(self, messages, batchId) -> None:
        print(f"Batch {batchId} aborted")

# COMMAND ----------

# Register the custom data source
spark.dataSource.register(RequestBinDataSource)

# COMMAND ----------

# IMPORTANT: Replace 'YOUR_BIN_ID' with actual RequestBin URL
# Go to https://requestbin.com and create a new bin first!

print("🚨 SETUP REQUIRED:")
print("1. Go to https://requestbin.com")
print("2. Click 'Create Request Bin'") 
print("3. Copy the URL (e.g. https://eootz3cf8xg9rz1.m.pipedream.net)")
print("4. Replace YOUR_BIN_ID in the code below")
print()

# Create DLT sink using RequestBin
dlt.create_sink(
    name="requestbin_demo_sink",
    format="requestbin_sink",
    options={
        "numRows": "5",
        "requestbin_url": "https://eo4yjoe725ib0e3.m.pipedream.net"  # 👈 REPLACE THIS
    }
)

# COMMAND ----------

# Create source table
@dlt.table(name="demo_orders")
def order_data():
    from pyspark.sql.functions import current_timestamp, concat, lit, expr
    
    return spark.range(10).select(
        concat(lit("ORD-"), expr("cast(id + 10000 as string)")).alias("order_id"),
        concat(lit("Customer_"), expr("cast(id as string)")).alias("customer_name"),
        expr("case when id % 5 = 0 then 'Laptop' when id % 5 = 1 then 'Phone' when id % 5 = 2 then 'Tablet' when id % 5 = 3 then 'Headphones' else 'Camera' end").alias("product"),
        expr("cast((id * 50 + 200) as int)").alias("amount"),
        current_timestamp().alias("timestamp")
    )

# COMMAND ----------

# Create append flow to send data to RequestBin
@dlt.append_flow(name="flow_to_requestbin", target="requestbin_demo_sink")
def stream_to_requestbin():
    return dlt.read_stream("demo_orders").repartition(4)

# COMMAND ----------

# Instructions for verification
print("📋 TO VERIFY THE DATA:")
print("1. Run this DLT pipeline")
print("2. Go to your RequestBin URL")
print("3. You should see POST requests with the order data")
print("4. Each request will contain a JSON batch with multiple records")
print()
print("🔍 EXPECTED DATA STRUCTURE:")
print("""{
  "batch_id": "batch_0_1234567890.123",
  "partition_id": 0,
  "record_count": 2,
  "records": [
    {
      "order_id": "ORD-10000",
      "customer_name": "Customer_0", 
      "product": "Laptop",
      "amount": 200,
      "timestamp": "2024-01-01T10:00:00.000",
      "partition_id": 0,
      "batch_time": "2024-01-01T10:00:00.000"
    },
    ...
  ]
}""")
