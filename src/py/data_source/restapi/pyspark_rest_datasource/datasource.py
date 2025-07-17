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
            
            # Make the HTTP request
            response = requests.request(
                method=self.method,
                url=self.url,
                headers=self.headers,
                timeout=self.timeout
            )
            
            logger.info(f"Response status: {response.status_code}")
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                # Response is a list of objects
                for item in data:
                    yield self._convert_to_tuple(item)
            elif isinstance(data, dict):
                # Response is a single object
                yield self._convert_to_tuple(data)
            else:
                logger.warning(f"Unexpected response format: {type(data)}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
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
    """REST API Data Source for PySpark"""
    
    @classmethod
    def name(cls) -> str:
        return "restapi"
    
    def schema(self) -> str:
        """Return the schema of the data source - auto-inferred"""
        return "unknown"
    
    def reader(self, schema: StructType) -> RestApiReader:
        """Create a reader for batch operations"""
        return RestApiReader(schema, self.options)
    
    def writer(self, schema: StructType, overwrite: bool) -> RestApiWriter:
        """Create a writer for batch operations"""
        return RestApiWriter(self.options, schema)
    
    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        """Create a stream reader (not implemented)"""
        raise NotImplementedError("Streaming read is not yet implemented for REST API data source")
    
    def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
        """Create a stream writer (not implemented)"""
        raise NotImplementedError("Streaming write is not yet implemented for REST API data source")


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


# Enhanced DataSource class with auto schema inference
class RestApiDataSourceWithInference(DataSource):
    """REST API Data Source for PySpark with automatic schema inference"""
    
    @classmethod
    def name(cls) -> str:
        return "restapi"
    
    def schema(self) -> StructType:
        """Return the schema of the data source - inferred from API response"""
        url = self.options.get("url", "")
        if not url:
            raise ValueError("URL option is required")
        
        headers_str = self.options.get("headers", "{}")
        try:
            headers = json.loads(headers_str) if headers_str else {}
        except json.JSONDecodeError:
            headers = {}
        
        timeout = int(self.options.get("timeout", "30"))
        
        return infer_schema_from_api(url, headers, timeout)
    
    def reader(self, schema: StructType) -> RestApiReader:
        """Create a reader for batch operations"""
        return RestApiReader(schema, self.options)
    
    def writer(self, schema: StructType, overwrite: bool) -> RestApiWriter:
        """Create a writer for batch operations"""
        return RestApiWriter(self.options, schema)
    
    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        """Create a stream reader (not implemented)"""
        raise NotImplementedError("Streaming read is not yet implemented for REST API data source")
    
    def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
        """Create a stream writer (not implemented)"""
        raise NotImplementedError("Streaming write is not yet implemented for REST API data source")


# Use the enhanced version by default
RestApiDataSource = RestApiDataSourceWithInference
