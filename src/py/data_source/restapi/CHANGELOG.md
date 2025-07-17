# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-01-01

### Added
- Initial release of PySpark REST DataSource
- Full PySpark Data Source API implementation for Spark 4.0+
- Support for reading from REST APIs using `spark.read.format("restapi")`
- Support for writing to REST APIs using `spark.write.format("restapi")`
- Automatic schema inference from API responses
- Custom authentication headers support
- Configurable timeouts and HTTP methods
- Comprehensive error handling and validation
- SQL integration with temporary views
- Development-ready logging and debugging features

### Features
- Read from any REST API that returns JSON
- Write data to REST APIs
- Automatic data type inference
- Custom HTTP headers and authentication
- Proper error handling for network issues
- Support for both single objects and arrays in API responses
- Integration with Spark SQL and DataFrame APIs

### Requirements
- Python 3.9+
- Apache Spark 4.0+
- PyArrow 10.0.0+
- Requests 2.25.0+ 