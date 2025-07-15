# News Data Source for Apache Spark

A comprehensive news data source implementation using the **Python Data Source API** in Apache Spark 4.0. This data source provides seamless integration with NewsAPI.org to fetch, process, and analyze news articles in both batch and streaming modes.

## Features

- 🔥 **Spark 4.0 Ready**: Built with the latest Python Data Source API
- 📰 **Rich News Data**: Access to thousands of news sources worldwide
- ⚡ **Batch & Streaming**: Support for both batch and real-time processing
- 🚀 **Parallel Processing**: Automatic partitioning for cluster-wide execution
- 🔧 **Highly Configurable**: Extensive options for customization
- 📊 **Analytics Ready**: Perfect for news analytics and sentiment analysis

## Installation

### Prerequisites

- Python 3.13+
- Apache Spark 4.0+
- NewsAPI.org API key (free at [newsapi.org](https://newsapi.org/register))

### Install Dependencies

```bash
pip install pyspark>=4.0.0 newsapi-python>=0.2.7
```

Or using the provided `pyproject.toml`:

```bash
pip install -e .
```

## Quick Start

### 1. Set Up Your API Key

```bash
export NEWSAPI_KEY="your_api_key_here"
```

### 2. Basic Usage

```python
from pyspark.sql import SparkSession
from main import NewsDataSource

# Create Spark session
spark = SparkSession.builder \
    .appName("NewsDataSourceExample") \
    .master("local[*]") \
    .getOrCreate()

# Register the news data source
spark.dataSource.register(NewsDataSource)

# Read news articles
df = spark.read.format("news").load()

# Show results
df.select("source_name", "title", "published_at").show(10)

spark.stop()
```

### 3. Streaming Example

```python
# Create streaming DataFrame
streaming_df = spark.readStream.format("news") \
    .option("query", "breaking news") \
    .option("interval", "60") \
    .load()

# Start streaming query
query = streaming_df.select("source_name", "title", "published_at") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
```

## Schema

The news data source provides the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `source_id` | string | Unique identifier for the news source |
| `source_name` | string | Display name of the news source |
| `title` | string | Article headline |
| `description` | string | Article summary/description |
| `url` | string | URL to the full article |
| `url_to_image` | string | URL to article's featured image |
| `published_at` | timestamp | Publication timestamp |
| `content` | string | Article content (truncated) |
| `author` | string | Article author |

## Configuration Options

### Batch Reading Options

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `query` | Search query | "technology" | "artificial intelligence" |
| `sources` | Comma-separated source list | null | "techcrunch,bbc-news" |
| `from_date` | Start date (YYYY-MM-DD) | null | "2024-01-01" |
| `to_date` | End date (YYYY-MM-DD) | today | "2024-12-31" |
| `page_size` | Articles per page | 100 | "50" |
| `max_pages` | Maximum pages to fetch | 1 | "10" |
| `sort_by` | Sort order | "publishedAt" | "relevancy", "popularity" |
| `api_key` | NewsAPI key | env var | "your_api_key" |

### Streaming Options

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `query` | Search query | "breaking news" | "technology" |
| `sources` | Comma-separated source list | null | "cnn,reuters" |
| `interval` | Polling interval (seconds) | 300 | "60" |
| `api_key` | NewsAPI key | env var | "your_api_key" |

## Usage Examples

### Advanced Batch Queries

```python
# Search for AI articles from specific sources
df = spark.read.format("news") \
    .option("query", "artificial intelligence OR machine learning") \
    .option("sources", "techcrunch,the-verge,wired") \
    .option("sort_by", "relevancy") \
    .option("max_pages", "3") \
    .load()

# Filter and analyze
ai_articles = df.filter(col("title").contains("ChatGPT"))
ai_articles.groupBy("source_name").count().show()
```

### Time Series Analysis

```python
from datetime import datetime, timedelta

# Get last week's news
week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

df = spark.read.format("news") \
    .option("query", "climate change") \
    .option("from_date", week_ago) \
    .option("sort_by", "publishedAt") \
    .load()

# Daily article counts
daily_counts = df.withColumn("date", col("published_at").cast("date")) \
    .groupBy("date") \
    .count() \
    .orderBy("date")

daily_counts.show()
```

### Streaming Analytics

```python
# Real-time news monitoring
streaming_df = spark.readStream.format("news") \
    .option("query", "stock market") \
    .option("interval", "120") \
    .load()

# Window-based aggregations
windowed_counts = streaming_df \
    .groupBy(
        window(col("published_at"), "10 minutes"),
        col("source_name")
    ) \
    .count()

query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
```

### Data Pipeline Example

```python
# Extract, Transform, Load pipeline
raw_df = spark.read.format("news") \
    .option("query", "electric vehicles") \
    .option("max_pages", "5") \
    .load()

# Transform: Clean and enrich
cleaned_df = raw_df.filter(
    col("title").isNotNull() & 
    col("description").isNotNull()
).withColumn(
    "word_count", 
    col("title").split(" ").size()
).withColumn(
    "processing_date", 
    current_date()
)

# Load: Save to Delta Lake or Parquet
cleaned_df.write \
    .mode("overwrite") \
    .partitionBy("processing_date") \
    .parquet("/path/to/output")
```

## Testing

### Run Batch Tests

```bash
python test_batch_news.py
```

### Run Streaming Tests

```bash
python test_streaming_news.py
```

### Run Examples

```bash
python examples.py
```

## Performance Considerations

### Batch Processing

- **Partitioning**: Automatic partitioning by pages for parallel processing
- **Page Size**: Adjust `page_size` based on your cluster resources
- **Max Pages**: Limit `max_pages` to prevent excessive API calls
- **Caching**: Cache frequently accessed DataFrames

### Streaming Processing

- **Polling Interval**: Balance between freshness and API rate limits
- **Checkpointing**: Enable checkpointing for fault tolerance
- **Rate Limiting**: Be aware of NewsAPI rate limits (1000 requests/day free tier)

### API Rate Limits

NewsAPI.org has the following rate limits:
- **Free Tier**: 1,000 requests/day
- **Paid Tiers**: Up to 1,000,000 requests/day

For production streaming use cases, consider:
- Using paid API tiers
- Implementing exponential backoff
- Caching responses when appropriate

## Advanced Features

### Custom Partitioning

```python
# Custom partitioning for large datasets
df = spark.read.format("news") \
    .option("query", "technology") \
    .option("max_pages", "20") \
    .load()

# Repartition by source for better performance
df.repartition(col("source_name")).write.parquet("/output/path")
```

### Sentiment Analysis Integration

```python
from pyspark.sql.functions import when, col, lower

# Simple sentiment analysis
sentiment_df = df.withColumn(
    "sentiment",
    when(col("title").contains("positive_keyword"), "positive")
    .when(col("title").contains("negative_keyword"), "negative")
    .otherwise("neutral")
)
```

### Multi-Source Comparison

```python
# Compare coverage across different source types
tech_sources = ["techcrunch", "the-verge", "wired"]
mainstream_sources = ["bbc-news", "cnn", "reuters"]

tech_df = spark.read.format("news") \
    .option("sources", ",".join(tech_sources)) \
    .load()

mainstream_df = spark.read.format("news") \
    .option("sources", ",".join(mainstream_sources)) \
    .load()

# Union and compare
combined_df = tech_df.union(mainstream_df)
combined_df.groupBy("source_name").count().show()
```

## Error Handling

The data source includes comprehensive error handling:

- **API Key Validation**: Validates API key on initialization
- **Rate Limit Handling**: Graceful handling of API rate limits
- **Network Errors**: Retry logic for network failures
- **Data Validation**: Schema validation for incoming data

## Best Practices

1. **API Key Security**: Never hardcode API keys; use environment variables
2. **Resource Management**: Always call `spark.stop()` in finally blocks
3. **Partitioning**: Use appropriate partitioning for large datasets
4. **Monitoring**: Monitor API usage to avoid rate limit issues
5. **Caching**: Cache DataFrames when performing multiple operations
6. **Error Handling**: Implement proper error handling for production use

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
- Open an issue in the repository
- Check NewsAPI.org documentation for API-related questions
- Refer to Apache Spark documentation for Spark-specific issues

---

**Note**: This is a demonstration implementation. For production use, consider additional features like:
- Advanced error handling and retry logic
- Metrics and monitoring integration
- Support for multiple news API providers
- Data quality validation
- Performance optimization
