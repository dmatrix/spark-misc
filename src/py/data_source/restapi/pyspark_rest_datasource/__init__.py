"""
PySpark REST API Data Source

A development-ready REST API Data Source implementation for Apache Spark 
using the Python Data Source API (Spark 4.0+). This allows you to read from 
and write to REST APIs directly using Spark's DataFrame API.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .datasource import RestApiDataSource, RestApiReader, RestApiWriter

__all__ = [
    "RestApiDataSource",
    "RestApiReader", 
    "RestApiWriter",
    "__version__"
] 