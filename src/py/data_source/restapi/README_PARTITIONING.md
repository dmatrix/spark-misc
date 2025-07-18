# REST API Data Source Partitioning

This document explains how to use the partitioning features of the REST API Data Source for PySpark to enable parallel processing of REST API calls.

## Overview

The REST API Data Source supports partitioning to enable Spark to parallelize REST API calls across multiple cores/executors. This can significantly improve performance when dealing with:

- Multiple API endpoints
- Paginated APIs
- Large datasets that can be divided into chunks

## Partitioning Strategies

### 1. Single Partition (Default)

By default, the data source uses a single partition, making one API call.

```python
df = spark.read.format("restapi") \
    .option("url", "https://api.example.com/data") \
    .option("method", "GET") \
    .load()
```

### 2. URL-based Partitioning

Use different URLs for each partition, allowing parallel requests to different endpoints.

```python
df = spark.read.format("restapi") \
    .option("partitionStrategy", "urls") \
    .option("urls", "https://api.example.com/data/1,https://api.example.com/data/2,https://api.example.com/data/3") \
    .option("method", "GET") \
    .load()
```

**Use Cases:**
- Different API endpoints for different data sets
- Regional API endpoints
- Different resource types from the same API

### 3. Page-based Partitioning

For paginated APIs, divide the data into pages and process them in parallel.

```python
df = spark.read.format("restapi") \
    .option("partitionStrategy", "pages") \
    .option("url", "https://api.example.com/data") \
    .option("totalPages", "10") \
    .option("pageSize", "100") \
    .option("method", "GET") \
    .load()
```

**Use Cases:**
- Large datasets with pagination
- APIs that support page/limit parameters
- Batch processing of large data sets

## Configuration Options

### Basic Options

| Option | Description | Default |
|--------|-------------|---------|
| `url` | Base URL for the API | Required |
| `method` | HTTP method | `GET` |
| `headers` | JSON string of HTTP headers | `{}` |
| `timeout` | Request timeout in seconds | `30` |

### Partitioning Options

| Option | Description | Required For |
|--------|-------------|--------------|
| `partitionStrategy` | Strategy: `single`, `urls`, `pages` | All strategies |
| `urls` | Comma-separated URLs | URL-based partitioning |
| `totalPages` | Total number of pages | Page-based partitioning |
| `pageSize` | Items per page | Page-based partitioning |

## Examples

### Example 1: Multiple API Endpoints

```python
# Process data from multiple geographic regions
df = spark.read.format("restapi") \
    .option("partitionStrategy", "urls") \
    .option("urls", "https://api.example.com/us,https://api.example.com/eu,https://api.example.com/asia") \
    .option("headers", '{"Authorization": "Bearer YOUR_TOKEN"}') \
    .load()

print("Processing multiple partitions in parallel")
df.show()
```

### Example 2: Paginated Data

```python
# Process 1000 records in chunks of 100 (10 pages)
df = spark.read.format("restapi") \
    .option("partitionStrategy", "pages") \
    .option("url", "https://api.example.com/data") \
    .option("totalPages", "10") \
    .option("pageSize", "100") \
    .option("headers", '{"Authorization": "Bearer YOUR_TOKEN"}') \
    .load()

print("Processing multiple pages in parallel")
df.show()
```

### Example 3: Custom Headers with Partitioning

```python
# Use custom headers with URL-based partitioning
headers = {
    "User-Agent": "MyApp/1.0",
    "Accept": "application/json",
    "Authorization": "Bearer YOUR_TOKEN"
}

df = spark.read.format("restapi") \
    .option("partitionStrategy", "urls") \
    .option("urls", "https://api.example.com/users,https://api.example.com/orders") \
    .option("headers", json.dumps(headers)) \
    .load()
```

## Performance Benefits

Partitioning provides several performance benefits:

1. **Parallel Processing**: Multiple API calls execute simultaneously
2. **Better Resource Utilization**: Utilizes all available cores/executors
3. **Faster Data Loading**: Reduces total processing time
4. **Scalability**: Handles larger datasets more efficiently

### Performance Comparison

```python
import time

# Single partition
start = time.time()
df_single = spark.read.format("restapi") \
    .option("url", "https://api.example.com/large-dataset") \
    .load()
count_single = df_single.count()
time_single = time.time() - start

# Multiple partitions (4 pages)
start = time.time()
df_multi = spark.read.format("restapi") \
    .option("partitionStrategy", "pages") \
    .option("url", "https://api.example.com/large-dataset") \
    .option("totalPages", "4") \
    .option("pageSize", "1000") \
    .load()
count_multi = df_multi.count()
time_multi = time.time() - start

print(f"Single partition: {count_single} rows in {time_single:.2f}s")
print(f"Multi partition: {count_multi} rows in {time_multi:.2f}s")
print(f"Speedup: {time_single/time_multi:.2f}x")
```

## Best Practices

### 1. Choose the Right Strategy

- **Single partition**: Small datasets, single endpoints
- **URL-based**: Different endpoints, regional APIs
- **Page-based**: Large paginated datasets

### 2. Optimize Partition Size

- Too few partitions: Underutilized resources
- Too many partitions: Overhead costs
- Aim for 2-4 partitions per CPU core

### 3. Handle Rate Limits

- Consider API rate limits when choosing partition count
- Use appropriate delays if needed
- Monitor API response times

### 4. Error Handling

- Each partition is processed independently
- Failed partitions don't affect successful ones
- Check logs for partition-specific errors

## Troubleshooting

### Common Issues

1. **No partitions created**: Check `partitionStrategy` option
2. **Rate limit errors**: Reduce partition count or add delays
3. **Schema mismatch**: Ensure all partitions return compatible data
4. **Memory issues**: Adjust `pageSize` for page-based partitioning

### Debugging

```python
# Check partition distribution using DataFrame API
from pyspark.sql.functions import spark_partition_id
partition_counts = [row['count'] for row in df.select(spark_partition_id().alias("partition_id")).groupBy("partition_id").count().orderBy("partition_id").collect()]
for i, count in enumerate(partition_counts):
    print(f"Partition {i}: {count} rows")
```

## Supported Response Formats

The data source handles various JSON response formats:

1. **Array of objects**: `[{"id": 1}, {"id": 2}]`
2. **Single object**: `{"id": 1, "name": "test"}`
3. **Paginated with 'data' key**: `{"data": [...], "total": 100}`
4. **Paginated with 'items' key**: `{"items": [...], "page": 1}`

## Integration with Spark SQL

```python
# Register as a temporary view
df.createOrReplaceTempView("api_data")

# Use SQL queries
result = spark.sql("""
    SELECT partition_id, COUNT(*) as row_count
    FROM api_data
    GROUP BY partition_id
    ORDER BY partition_id
""")
result.show()
```

This partitioning functionality makes the REST API Data Source a powerful tool for parallel processing of REST API data in PySpark applications. 