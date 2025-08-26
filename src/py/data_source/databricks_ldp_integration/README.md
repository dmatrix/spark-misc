# Python Data Sources for Databricks Delta Live Pipelines

This project demonstrates [Python Data Sources](https://www.databricks.com/blog/simplify-data-ingestion-new-python-data-source-api) integrated with Databricks [Lakeflow Declarative Pipelines](https://www.databricks.com/product/data-engineering/lakeflow-declarative-pipelines), featuring real-time aircraft tracking data from OpenSky Network and webhook testing capabilities with RequestBin.

## 🚀 Overview

This implementation leverages the new **Python Data Source API** introduced in Spark 4.0 to create custom data connectors that seamlessly integrate with Databricks LDP. The project includes:

- **OpenSky Data Source (`opensky_source_in_ldp.py`)**: Real-time aircraft tracking data ingestion from European airspace
- **RequestBin Sink (`requestbin_sink_in_ldp.py`)**: HTTP webhook testing and debugging capabilities with demo data generation

## 📋 Prerequisites

### Dependencies
```python
# Core dependencies
import dlt
import requests
from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader, DataSourceStreamWriter
from pyspark.sql.types import *
```

## 🛩️ OpenSky Flight Data Source

### Features
- **Real-time streaming**: Continuous ingestion of aircraft state data
- **European coverage**: Focused on European airspace (35°-72°N, -25°-45°E)
- **Data validation**: Filters invalid or incomplete flight records
- **Rate limiting**: Configurable polling interval (default: 10 seconds)

### Schema
```python
StructType([
    StructField("timestamp", TimestampType()),      # Data collection time
    StructField("icao24", StringType()),           # Aircraft identifier
    StructField("callsign", StringType()),         # Flight callsign
    StructField("country", StringType()),          # Country of registration
    StructField("longitude", DoubleType()),        # GPS longitude
    StructField("latitude", DoubleType()),         # GPS latitude  
    StructField("altitude_m", DoubleType()),       # Altitude in meters
    StructField("velocity_ms", DoubleType()),      # Velocity in m/s
    StructField("vertical_rate", DoubleType()),    # Climb/descent rate
    StructField("on_ground", BooleanType())        # Ground status
])
```

### Key Components

#### 1. Custom Data Source Implementation
```python
class OpenSkyDataSource(DataSource):
    @classmethod 
    def name(cls):
        return "opensky-flights"
    
    def simpleStreamReader(self, schema):
        return OpenSkyStreamReader(schema, self.options)
```

#### 2. Stream Reader with Offset Management
```python
class OpenSkyStreamReader(SimpleDataSourceStreamReader):
    def initialOffset(self):
        return {'last_fetch': 0}
    
    def read(self, start):
        # Fetch data, parse, and return with new offset
        return (parsed_data, new_offset)
```

#### 3. LDP Integration
```python
@dlt.table
def opensky_flights():
    return spark.readStream.format("opensky-flights").load()
```

## 🔗 RequestBin Webhook Sink

### Features
- **Batch processing**: Sends data in JSON batches to webhooks
- **Demo data generation**: Creates synthetic order data for testing

### Demo Data Schema
```python
{
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
    }
  ]
}
```

### Setup Instructions
1. **Create RequestBin endpoint**:
   ```bash
   # Go to https://requestbin.com
   # Click "Create Request Bin"
   # Copy the generated URL (e.g., https://eootz3cf8xg9rz1.m.pipedream.net)
   ```

2. **Update configuration**:
   ```python
   dlt.create_sink(
       name="requestbin_demo_sink",
       format="requestbin_sink", 
       options={
           "requestbin_url": "YOUR_REQUESTBIN_URL_HERE"  # Replace this!
       }
   )
   ```

### Key Components
1. **Custom Sink Data Source Implementation**:
    ```python
    class RequestBinDataSource(DataSource):
        @classmethod
        def name(cls):
            return "requestbin_sink"
    
        def streamWriter(self, schema: StructType, overwrite: bool):
            return RequestBinStreamWriter(self.options)
    ```

2. **Stream Writer with Batch Processing**:
    ```python
    class RequestBinStreamWriter(DataSourceStreamWriter):
        def write(self, iterator):
            # Collect rows into batches
            batch_data = [row_to_dict(row) for row in iterator]
            
            # Send HTTP POST with JSON payload
            response = requests.post(self.requestbin_url, json=payload)
            return SimpleCommitMessage(partition_id, count, success_count)
    ```

3. **Commit Message Handling**:
    ```python
    class SimpleCommitMessage(WriterCommitMessage):
        def __init__(self, partition_id, count, success_count=0):
            self.partition_id = partition_id
            self.count = count
            self.success_count = success_count
    ```

4. **LDP Sink Integration**:
    ```python
    dlt.create_sink(
        name="requestbin_demo_sink",
        format="requestbin_sink",
        options={"requestbin_url": "YOUR_REQUESTBIN_URL"}
    )

    @dlt.append_flow(name="flow_to_requestbin", target="requestbin_demo_sink")
    def stream_to_requestbin():
        return dlt.read_stream("demo_orders").repartition(4)
    ```

## Verify Data Flow

### OpenSky Data Verification:
```sql
-- Check flight data ingestion
SELECT COUNT(*), MAX(timestamp) as latest_data
FROM your_catalog.your_schema.opensky_flights;

-- View sample flight data
SELECT * FROM your_catalog.your_schema.opensky_flights 
ORDER BY timestamp DESC LIMIT 10;
```

### RequestBin Verification:
- Navigate to your RequestBin URL
- Look for POST requests with JSON payloads
- Verify batch structure and data content

## 📚 References

- [Python Data Source API Documentation](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
- [Databricks Delta Live Tables](https://docs.databricks.com/aws/en/dlt/concepts)
- [OpenSky Network API](https://openskynetwork.github.io/opensky-api/)
- [RequestBin Testing Platform](https://requestbin.com)
