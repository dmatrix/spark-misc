"""
REST API Data Source for PySpark using the Python Data Source API

This implementation uses the proper PySpark Data Source API introduced in Spark 4.0
to enable reading from and writing to REST APIs using the standard DataFrame format API.
With partitioning support for parallel processing.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Iterator, Tuple
import requests

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceWriter, DataSourceStreamReader, DataSourceStreamWriter, InputPartition
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql import Row

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RestApiPartition(InputPartition):
    """Input partition for REST API data source"""
    
    def __init__(self, url: str, method: str = "GET", headers: Dict[str, str] = None, 
                 params: Dict[str, str] = None, partition_id: int = 0):
        self.url = url
        self.method = method
        self.headers = headers or {}
        self.params = params or {}
        self.partition_id = partition_id


class RestApiUrlPartition(InputPartition):
    """URL-based partition for REST API data source"""
    
    def __init__(self, url: str, method: str = "GET", headers: Dict[str, str] = None, partition_id: int = 0):
        self.url = url
        self.method = method
        self.headers = headers or {}
        self.partition_id = partition_id


class RestApiPagePartition(InputPartition):
    """Page-based partition for REST API data source with pagination"""
    
    def __init__(self, base_url: str, page: int, page_size: int = 100, method: str = "GET", 
                 headers: Dict[str, str] = None, partition_id: int = 0):
        self.base_url = base_url
        self.page = page
        self.page_size = page_size
        self.method = method
        self.headers = headers or {}
        self.partition_id = partition_id
    
    @property
    def url(self) -> str:
        """Generate URL with pagination parameters"""
        separator = "&" if "?" in self.base_url else "?"
        return f"{self.base_url}{separator}page={self.page}&limit={self.page_size}"


class RestApiReader(DataSourceReader):
    """Reader for REST API data source with partition support"""
    
    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = options
        self.timeout = int(options.get("timeout", "30"))
        self._partitions = []  # Will be set by DataSource
    
    def partitions(self):
        """Return the list of partitions for parallel processing"""
        return self._partitions
        
    def _parse_headers(self, headers_str: str) -> Dict[str, str]:
        """Parse headers from JSON string"""
        try:
            return json.loads(headers_str) if headers_str else {}
        except json.JSONDecodeError:
            logger.warning(f"Invalid headers JSON: {headers_str}")
            return {}
    
    def read(self, partition):
        """Read data from REST API partition and yield tuples"""
        try:
            # Handle different partition types
            if isinstance(partition, (RestApiPartition, RestApiUrlPartition, RestApiPagePartition)):
                url = partition.url
                method = partition.method
                headers = partition.headers
                partition_id = partition.partition_id
            else:
                # Handle case where partition is just an index (for simple partitioning)
                if hasattr(self, '_partitions') and self._partitions:
                    if isinstance(partition, int) and 0 <= partition < len(self._partitions):
                        partition_obj = self._partitions[partition]
                        url = partition_obj.url
                        method = partition_obj.method
                        headers = partition_obj.headers
                        partition_id = partition_obj.partition_id
                    else:
                        # Use the first partition as fallback
                        partition_obj = self._partitions[0]
                        url = partition_obj.url
                        method = partition_obj.method
                        headers = partition_obj.headers
                        partition_id = partition_obj.partition_id
                else:
                    # Fallback to single partition mode
                    url = self.options.get("url", "")
                    method = self.options.get("method", "GET")
                    headers = self._parse_headers(self.options.get("headers", "{}"))
                    partition_id = 0
            
            logger.info(f"Partition {partition_id}: Making {method} request to {url}")
            
            # Set default headers if none provided
            if not headers:
                headers = {"Content-Type": "application/json"}
            
            # Add additional parameters for page-based partitions
            params = {}
            if isinstance(partition, RestApiPagePartition):
                params = {"page": partition.page, "limit": partition.page_size}
            
            # Make the HTTP request
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=self.timeout
            )
            
            logger.info(f"Partition {partition_id}: Response status: {response.status_code}")
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                # Response is a list of objects
                for item in data:
                    yield self._convert_to_tuple(item)
            elif isinstance(data, dict):
                # Response is a single object or paginated response
                if "data" in data and isinstance(data["data"], list):
                    # Paginated response with data key
                    for item in data["data"]:
                        yield self._convert_to_tuple(item)
                elif "items" in data and isinstance(data["items"], list):
                    # Paginated response with items key
                    for item in data["items"]:
                        yield self._convert_to_tuple(item)
                else:
                    # Single object response
                    yield self._convert_to_tuple(data)
            else:
                logger.warning(f"Unexpected response format: {type(data)}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Partition {partition_id}: Request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Partition {partition_id}: Failed to parse JSON response: {e}")
            raise
        except Exception as e:
            logger.error(f"Partition {partition_id}: Unexpected error: {e}")
            raise
    
    def _convert_to_tuple(self, item: Dict[str, Any]) -> Tuple:
        """Convert dictionary to tuple matching schema"""
        if not isinstance(item, dict):
            # If item is not a dict, convert to string
            return (str(item),) * len(self.schema.fields)
        
        values = []
        for field in self.schema.fields:
            value = item.get(field.name)
            if value is None:
                values.append(None)
            elif isinstance(value, (dict, list)):
                # Convert complex types to JSON string
                values.append(json.dumps(value))
            else:
                # Keep the original data type to match schema
                field_type = field.dataType
                if isinstance(field_type, IntegerType):
                    try:
                        values.append(int(value))
                    except (ValueError, TypeError):
                        values.append(None)
                elif isinstance(field_type, DoubleType):
                    try:
                        values.append(float(value))
                    except (ValueError, TypeError):
                        values.append(None)
                elif isinstance(field_type, BooleanType):
                    values.append(bool(value))
                else:
                    # Default to string
                    values.append(str(value))
        return tuple(values)


class RestApiWriter(DataSourceWriter):
    """Writer for REST API data source"""
    
    def __init__(self, options: Dict[str, str], schema: StructType):
        self.options = options
        self.schema = schema
        self.url = options.get("url", "")
        self.method = options.get("method", "POST")
        self.headers = self._parse_headers(options.get("headers", "{}"))
        self.timeout = int(options.get("timeout", "30"))
        
    def _parse_headers(self, headers_str: str) -> Dict[str, str]:
        """Parse headers from JSON string"""
        try:
            return json.loads(headers_str) if headers_str else {}
        except json.JSONDecodeError:
            logger.warning(f"Invalid headers JSON: {headers_str}")
            return {}
    
    def write(self, iterator):
        """Write data to REST API"""
        try:
            # Set default headers if none provided
            if not self.headers:
                self.headers = {"Content-Type": "application/json"}
            
            # Convert rows to list of dictionaries
            rows_data = []
            for row in iterator:
                row_dict = {}
                for i, field in enumerate(self.schema.fields):
                    if i < len(row):
                        row_dict[field.name] = row[i]
                rows_data.append(row_dict)
            
            logger.info(f"Writing {len(rows_data)} rows to {self.url}")
            
            # Write each row as separate API call
            # In a real implementation, you might want to batch these
            for row_data in rows_data:
                response = requests.request(
                    method=self.method,
                    url=self.url,
                    headers=self.headers,
                    json=row_data,
                    timeout=self.timeout
                )
                
                logger.info(f"Write response status: {response.status_code}")
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Write request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during write: {e}")
            raise


class RestApiDataSource(DataSource):
    """REST API Data Source for PySpark with partition support"""
    
    @classmethod
    def name(cls) -> str:
        return "restapi"
    
    def schema(self) -> StructType:
        """Return the schema of the data source - inferred from API response"""
        url = self.options.get("url", "")
        
        # Handle URL-based partitioning
        if not url:
            urls = self.options.get("urls", "")
            if urls:
                # Use the first URL for schema inference
                url = urls.split(",")[0].strip()
            else:
                raise ValueError("Either 'url' or 'urls' option is required")
        
        headers_str = self.options.get("headers", "{}")
        try:
            headers = json.loads(headers_str) if headers_str else {}
        except json.JSONDecodeError:
            headers = {}
        
        timeout = int(self.options.get("timeout", "30"))
        
        return infer_schema_from_api(url, headers, timeout)
    
    def _create_partitions(self) -> List[InputPartition]:
        """Create partitions based on options"""
        partitions = []
        
        # Get partitioning strategy
        partition_strategy = self.options.get("partitionStrategy", "single")
        
        if partition_strategy == "urls":
            # URL-based partitioning
            urls = self.options.get("urls", "")
            if urls:
                url_list = [url.strip() for url in urls.split(",")]
                method = self.options.get("method", "GET")
                headers = self._parse_headers(self.options.get("headers", "{}"))
                
                for i, url in enumerate(url_list):
                    partitions.append(RestApiUrlPartition(url, method, headers, i))
            else:
                # Fallback to single partition
                partitions.append(self._create_single_partition())
                
        elif partition_strategy == "pages":
            # Page-based partitioning
            base_url = self.options.get("url", "")
            total_pages = int(self.options.get("totalPages", "1"))
            page_size = int(self.options.get("pageSize", "100"))
            method = self.options.get("method", "GET")
            headers = self._parse_headers(self.options.get("headers", "{}"))
            
            for page in range(1, total_pages + 1):
                partitions.append(RestApiPagePartition(base_url, page, page_size, method, headers, page - 1))
                
        else:
            # Single partition (default)
            partitions.append(self._create_single_partition())
        
        logger.info(f"Created {len(partitions)} partitions using strategy: {partition_strategy}")
        return partitions
    
    def _create_single_partition(self) -> InputPartition:
        """Create a single partition for non-partitioned reading"""
        url = self.options.get("url", "")
        method = self.options.get("method", "GET")
        headers = self._parse_headers(self.options.get("headers", "{}"))
        return RestApiPartition(url, method, headers, partition_id=0)
    
    def _parse_headers(self, headers_str: str) -> Dict[str, str]:
        """Parse headers from JSON string"""
        try:
            return json.loads(headers_str) if headers_str else {}
        except json.JSONDecodeError:
            logger.warning(f"Invalid headers JSON: {headers_str}")
            return {}
    
    def reader(self, schema: StructType) -> RestApiReader:
        """Create a reader for batch operations"""
        # Create reader with partitions
        reader = RestApiReader(schema, self.options)
        reader._partitions = self._create_partitions()
        return reader
    
    def writer(self, schema: StructType, overwrite: bool) -> RestApiWriter:
        """Create a writer for batch operations"""
        return RestApiWriter(self.options, schema)
    
    def streamReader(self, schema: StructType):
        """Create a stream reader for streaming operations
        
        Args:
            schema: The schema of the data to be read
            
        Returns:
            DataSourceStreamReader instance for streaming data from REST API
            
        Raises:
            NotImplementedError: Streaming functionality not yet implemented
        """
        raise NotImplementedError("Streaming read is not yet implemented for REST API data source. "
                                "Future implementation would support polling REST endpoints at intervals.")
    
    def streamWriter(self, schema: StructType, overwrite: bool):
        """Create a stream writer for streaming operations
        
        Args:
            schema: The schema of the data to be written
            overwrite: Whether to overwrite existing data
            
        Returns:
            DataSourceStreamWriter instance for streaming data to REST API
            
        Raises:
            NotImplementedError: Streaming functionality not yet implemented
        """
        raise NotImplementedError("Streaming write is not yet implemented for REST API data source. "
                                "Future implementation would support real-time data pushing to REST endpoints.")
    
    def simpleStreamReader(self, schema: StructType):
        """Create a simple stream reader for streaming operations
        
        Args:
            schema: The schema of the data to be read
            
        Returns:
            SimpleDataSourceStreamReader instance for streaming data from REST API
            
        Raises:
            NotImplementedError: Streaming functionality not yet implemented
        """
        raise NotImplementedError("Simple streaming read is not yet implemented for REST API data source. "
                                "Future implementation would support simple polling of REST endpoints.")


# Auto schema inference function

def infer_schema_from_api(url: str, headers: Optional[Dict[str, str]] = None, 
                         timeout: int = 30) -> StructType:
    """
    Infer schema by making a sample request to the API
    """
    try:
        if headers is None:
            headers = {"Content-Type": "application/json"}
            
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        
        # Handle different response formats
        sample_item = None
        if isinstance(data, list) and len(data) > 0:
            sample_item = data[0]
        elif isinstance(data, dict):
            # Handle paginated responses
            if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
                sample_item = data["data"][0]
            elif "items" in data and isinstance(data["items"], list) and len(data["items"]) > 0:
                sample_item = data["items"][0]
            else:
                sample_item = data
        else:
            raise ValueError(f"Cannot infer schema from response type: {type(data)}")
        
        if not isinstance(sample_item, dict):
            raise ValueError("Sample item must be a dictionary to infer schema")
        
        # Create schema fields
        fields = []
        for key, value in sample_item.items():
            if isinstance(value, bool):
                field_type = BooleanType()
            elif isinstance(value, int):
                field_type = IntegerType()
            elif isinstance(value, float):
                field_type = DoubleType()
            else:
                # Default to string for complex types or strings
                field_type = StringType()
            
            fields.append(StructField(key, field_type, True))
        
        return StructType(fields)
        
    except Exception as e:
        logger.warning(f"Could not infer schema from API: {e}")
        # Return a default schema with just one string field
        return StructType([StructField("data", StringType(), True)])


def main():
    """Example usage of the REST API data source with partitioning"""
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("REST API Data Source with Partitioning") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        logger.info("REST API data source registered successfully")
        
        # Example 1: Single partition (default)
        print("=" * 60)
        print("Example 1: Single Partition (Default)")
        print("=" * 60)
        
        df1 = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts/1") \
            .option("method", "GET") \
            .load()
        
        print("Single partition processing complete")
        df1.show(5)
        df1.printSchema()
        
        # Example 2: URL-based partitioning
        print("\n" + "=" * 60)
        print("Example 2: URL-based Partitioning")
        print("=" * 60)
        
        df2 = spark.read \
            .format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", "https://jsonplaceholder.typicode.com/posts/1,https://jsonplaceholder.typicode.com/posts/2,https://jsonplaceholder.typicode.com/posts/3") \
            .option("method", "GET") \
            .load()
        
        print("URL-based partitioning complete")
        df2.show(5)
        
        # Example 3: Page-based partitioning
        print("\n" + "=" * 60)
        print("Example 3: Page-based Partitioning")
        print("=" * 60)
        
        df3 = spark.read \
            .format("restapi") \
            .option("partitionStrategy", "pages") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("totalPages", "2") \
            .option("pageSize", "5") \
            .option("method", "GET") \
            .load()
        
        print("Page-based partitioning complete")
        print(f"Total rows: {df3.count()}")
        df3.show(5)
        
        # Example 4: Performance comparison
        print("\n" + "=" * 60)
        print("Example 4: Performance Comparison")
        print("=" * 60)
        
        import time
        
        # Single partition timing
        start_time = time.time()
        df_single = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/posts") \
            .option("method", "GET") \
            .load()
        single_count = df_single.count()
        single_time = time.time() - start_time
        
        # Multi partition timing
        start_time = time.time()
        df_multi = spark.read \
            .format("restapi") \
            .option("partitionStrategy", "urls") \
            .option("urls", "https://jsonplaceholder.typicode.com/posts/1,https://jsonplaceholder.typicode.com/posts/2,https://jsonplaceholder.typicode.com/posts/3,https://jsonplaceholder.typicode.com/posts/4") \
            .option("method", "GET") \
            .load()
        multi_count = df_multi.count()
        multi_time = time.time() - start_time
        
        print(f"Single partition: {single_count} rows in {single_time:.2f}s")
        print(f"Multi partition: {multi_count} rows in {multi_time:.2f}s")
        
        print("\nREST API Data Source with Partitioning examples completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
