"""
REST API Data Source for PySpark using the Python Data Source API

This implementation uses the proper PySpark Data Source API introduced in Spark 4.0
to enable reading from and writing to REST APIs using the standard DataFrame format API.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Iterator, Tuple
import requests

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceWriter, DataSourceStreamReader, DataSourceStreamWriter
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql import Row

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RestApiReader(DataSourceReader):
    """Reader for REST API data source"""
    
    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = options
        self.url = options.get("url", "")
        self.method = options.get("method", "GET")
        self.headers = self._parse_headers(options.get("headers", "{}"))
        self.timeout = int(options.get("timeout", "30"))
        
    def _parse_headers(self, headers_str: str) -> Dict[str, str]:
        """Parse headers from JSON string"""
        try:
            return json.loads(headers_str) if headers_str else {}
        except json.JSONDecodeError:
            logger.warning(f"Invalid headers JSON: {headers_str}")
            return {}
    
    def read(self, partition):
        """Read data from REST API and yield tuples"""
        try:
            logger.info(f"Making {self.method} request to {self.url}")
            
            # Set default headers if none provided
            if not self.headers:
                self.headers = {"Content-Type": "application/json"}
            
            response = requests.request(
                method=self.method,
                url=self.url,
                headers=self.headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Handle both single objects and arrays
            if isinstance(data, list):
                for item in data:
                    yield self._convert_to_tuple(item)
            else:
                yield self._convert_to_tuple(data)
                
        except requests.RequestException as e:
            logger.error(f"Failed to read from REST API: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading from REST API: {e}")
            raise
    
    def _convert_to_tuple(self, data: Dict[str, Any]) -> Tuple:
        """Convert API response to tuple matching the schema"""
        if isinstance(data, dict):
            # Create tuple matching schema field order
            result = []
            for field in self.schema.fields:
                value = data.get(field.name)
                if value is None:
                    result.append(None)
                elif isinstance(value, (dict, list)):
                    result.append(json.dumps(value))
                else:
                    result.append(str(value))
            return tuple(result)
        else:
            # Single value - put in first field
            return (str(data),) + (None,) * (len(self.schema.fields) - 1)


class RestApiWriter(DataSourceWriter):
    """Writer for REST API data source"""
    
    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = options
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
    
    def write(self, partition):
        """Write data to REST API"""
        try:
            # Set default headers if none provided
            if not self.headers:
                self.headers = {"Content-Type": "application/json"}
            
            for row in partition:
                # Convert row to dictionary
                data = {}
                for i, field in enumerate(self.schema.fields):
                    if i < len(row):
                        data[field.name] = row[i]
                
                logger.info(f"Sending {self.method} request to {self.url}")
                
                response = requests.request(
                    method=self.method,
                    url=self.url,
                    headers=self.headers,
                    json=data,
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                logger.info(f"Successfully sent data: {response.status_code}")
                
        except requests.RequestException as e:
            logger.error(f"Failed to write to REST API: {e}")
            raise
        except Exception as e:
            logger.error(f"Error writing to REST API: {e}")
            raise


class RestApiDataSource(DataSource):
    """REST API Data Source for PySpark"""
    
    def __init__(self, options=None):
        """Initialize the REST API data source
        
        Args:
            options: Dictionary of options passed from Spark
        """
        options = options or {}
        super().__init__(options)
        
    @classmethod
    def name(cls) -> str:
        return "restapi"
    
    def schema(self) -> str:
        """Return dynamic schema - will be inferred from data"""
        return "id string, name string, email string, phone string, website string, username string, address string, company string"
    
    def reader(self, schema: StructType) -> DataSourceReader:
        """Create a reader for batch operations
        
        Args:
            schema: The schema of the data to be read
            
        Returns:
            RestApiReader instance for reading data from REST API
            
        Raises:
            ValueError: If required options are missing
        """
        if not self.options.get("url"):
            raise ValueError("URL option is required for REST API data source")
        return RestApiReader(schema, self.options)
    
    def writer(self, schema: StructType) -> DataSourceWriter:
        """Create a writer for batch operations
        
        Args:
            schema: The schema of the data to be written
            
        Returns:
            RestApiWriter instance for writing data to REST API
            
        Raises:
            ValueError: If required options are missing
        """
        if not self.options.get("url"):
            raise ValueError("URL option is required for REST API data source")
        return RestApiWriter(schema, self.options)
    
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


def main():
    """Example usage of the REST API data source"""
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("REST API Data Source") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Register the data source
        spark.dataSource.register(RestApiDataSource)
        logger.info("REST API data source registered successfully")
        
        # Example 1: Read from JSONPlaceholder API
        print("=" * 60)
        print("Example 1: Reading from JSONPlaceholder API")
        print("=" * 60)
        
        df = spark.read \
            .format("restapi") \
            .option("url", "https://jsonplaceholder.typicode.com/users") \
            .option("method", "GET") \
            .load()
        
        df.show(5)
        df.printSchema()
        
        # Example 2: Write to an API (mock example)
        print("\n" + "=" * 60)
        print("Example 2: Creating sample data for writing")
        print("=" * 60)
        
        # Create sample data
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        schema = StructType([
            StructField("title", StringType(), True),
            StructField("body", StringType(), True),
            StructField("userId", IntegerType(), True)
        ])
        
        sample_data = [
            ("Test Post 1", "Content for post 1", 1),
            ("Test Post 2", "Content for post 2", 2),
            ("Test Post 3", "Content for post 3", 3)
        ]
        
        sample_df = spark.createDataFrame(sample_data, schema)
        sample_df.show()
        
        # Note: For demo purposes, we won't actually write to an API
        # In real usage, you would do:
        # sample_df.write \
        #     .format("restapi") \
        #     .option("url", "https://jsonplaceholder.typicode.com/posts") \
        #     .option("method", "POST") \
        #     .save()
        
        print("\nREST API Data Source examples completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
