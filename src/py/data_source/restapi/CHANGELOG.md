# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2024-01-18

### Added
- **Advanced Partitioning Support** - Major new feature for parallel processing
  - Single partition mode (default)
  - URL-based partitioning (multiple endpoints processed in parallel)
  - Page-based partitioning (pagination with parallel page processing)
- **Pure DataFrame API** - Removed all RDD operations for cleaner code
- **Comprehensive Test Suite** - Full test coverage for all partitioning features
- **Single-file Architecture** - Simplified to single `restapi.py` file

### Changed
- **BREAKING:** Import statement changed from `from pyspark_rest_datasource import RestApiDataSource` to `from restapi import RestApiDataSource`
- **Package Structure:** Consolidated from package directory to single file
- **Performance:** Parallel processing now utilizes all available Spark cores
- **Maintainability:** Single source of truth eliminates code duplication

### Enhanced
- **Configuration Options:** Added `partitionStrategy`, `urls`, `totalPages`, `pageSize`
- **Error Handling:** Partition-level error isolation
- **Logging:** Detailed partition-level logging for debugging
- **Documentation:** Updated with partitioning examples and usage patterns

### Removed
- Package directory structure (`pyspark_rest_datasource/`)
- RDD API usage (replaced with DataFrame API)
- Code duplication between implementations

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