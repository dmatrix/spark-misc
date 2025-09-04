# Spark Declarative Pipelines (SDP) Examples

This directory contains example implementations of **Spark Declarative Pipelines (SDP)**, a framework for building and managing data pipelines using Apache Spark. The SDP framework enables declarative data transformations through Python decorators and SQL, providing a clean and maintainable approach to data pipeline development.

This project is structured as a **uv-managed Python package** with PySpark 4.1.0.dev1 and Spark Connect support, providing a modern development environment with dependency management and virtual environment isolation.

## Overview

The SDP directory demonstrates two complete data processing pipelines:

1. **BrickFood** - An e-commerce order processing and analytics system
2. **Oil Rigs** - An industrial IoT sensor monitoring and analysis system

Each project showcases different aspects of the SDP framework, from synthetic data generation and materialized view creation to business analytics and sensor data visualization.

## Prerequisites and Setup

### Requirements

1. **Python 3.11+**: Required for the project
2. **UV Package Manager**: For dependency management and virtual environments
3. **Spark Declarative Pipelines CLI**: Required for running the pipelines (`spark-pipelines` command)

### Installation

1. **Install UV** (if not already installed):
   ```bash
   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Windows
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

2. **Clone and setup the project**:
   ```bash
   cd /path/to/spark-misc/src/py/sdp
   
   # Install dependencies and create virtual environment
   uv sync
   
   # Activate the virtual environment (optional)
   source .venv/bin/activate
   ```

3. **Verify installation**:
   ```bash
   # Check that PySpark is available
   uv run python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
   
   # Check SDP CLI availability (required for pipelines)
   spark-pipelines --help
   ```

### Project Dependencies

The project includes the following key dependencies (managed in `pyproject.toml`):

- **PySpark 4.1.0.dev1**: Latest development version of Apache Spark
- **PySpark Connect 4.1.0.dev1**: Spark Connect client for remote cluster connectivity
- **Faker 37.6.0+**: For generating realistic synthetic data
- **Plotly 6.3.0+**: For interactive data visualizations

## Project Structure

```
sdp/
├── pyproject.toml                   # UV project configuration and dependencies
├── uv.lock                         # UV lock file for reproducible builds
├── main.py                         # CLI interface for running pipelines
├── SDP_README.md                   # This file
├── __init__.py                     # Python package initialization
├── .venv/                          # UV virtual environment (auto-generated)
├── utils/                          # Shared utilities
│   ├── __init__.py                 # Package initialization
│   └── order_gen_util.py           # Order data generation utilities
├── brickfood/                      # E-commerce order processing pipeline
│   ├── __init__.py                 # Package initialization
│   ├── pipeline.yml                # SDP pipeline configuration
│   ├── run_pipeline.sh             # Pipeline execution script
│   ├── transformations/            # Data transformation definitions
│   │   ├── orders_mv.py            # Main orders materialized view (Python)
│   │   ├── approved_orders_mv.sql  # Approved orders filter (SQL)
│   │   ├── fulfilled_orders_mv.sql # Fulfilled orders filter (SQL)
│   │   └── pending_orders_mv.sql   # Pending orders filter (SQL)
│   ├── query_tables.py             # Query and display order data
│   ├── calculate_sales_tax.py      # Sales tax calculations and analytics
│   ├── spark-warehouse/            # Generated Spark warehouse data
│   ├── metastore_db/              # Derby database files (auto-generated)
│   └── artifacts/                  # Build artifacts
└── oil_rigs/                       # Industrial sensor monitoring pipeline
    ├── __init__.py                 # Package initialization
    ├── pipeline.yml                # SDP pipeline configuration
    ├── run_pipeline.sh             # Pipeline execution script
    ├── transformations/            # Data transformation definitions
    │   ├── oil_rig_events_mv.py    # Base rig data generation (Python)
    │   ├── temperature_events_mv.sql # Temperature sensor aggregation (SQL)
    │   ├── pressure_events_mv.sql    # Pressure sensor aggregation (SQL)
    │   └── water_level_events_mv.sql # Water level sensor aggregation (SQL)
    ├── query_oil_rigs_tables.py   # Query and display sensor data
    ├── plot_temperatures.py       # Temperature data visualization
    ├── spark-warehouse/            # Generated Spark warehouse data
    └── metastore_db/              # Derby database files (auto-generated)
```

## BrickFood E-commerce Pipeline

### Purpose
Demonstrates a complete e-commerce order processing system with order lifecycle management, financial calculations, and business analytics.

### Data Model
The pipeline creates the following materialized views:

- **`orders_mv`** - Main orders table with complete order information
  - Schema: `order_id`, `order_item`, `price`, `items_ordered`, `status`, `date_ordered`
  - Generates 100 random orders with various products and statuses
  
- **`approved_orders_mv`** - Filtered view containing only approved orders
- **`fulfilled_orders_mv`** - Filtered view containing only fulfilled orders  
- **`pending_orders_mv`** - Filtered view containing only pending orders

### Key Features
- **Synthetic Data Generation**: Creates realistic order data with 20+ product types
- **Order Status Management**: Tracks orders through approval, fulfillment, and pending states
- **Financial Analytics**: Calculates total prices, 15% sales tax, and order summaries
- **Product Analytics**: Provides breakdown by product category and sales performance
- **Business Intelligence**: Generates summary statistics and reports

### Product Categories
Toys, sports equipment, electronics including: Toy Car, Basketball, Laptop, Action Figure, Tennis Racket, Smartphone, Board Game, Football, Headphones, Drone, Puzzle, Tablet, Skateboard, Camera, Video Game, Scooter, Smartwatch, Baseball Bat, VR Headset, Electric Guitar.

### Running the BrickFood Pipeline

#### Using the CLI Interface (Recommended)
```bash
# Run the complete BrickFood pipeline
uv run python main.py brickfood

# Get help
uv run python main.py --help
```

#### Manual Execution (Advanced)
```bash
cd brickfood/

# 1. Execute the SDP pipeline to create materialized views
./run_pipeline.sh

# 2. Query and display order data
uv run python query_tables.py

# 3. Calculate sales tax and generate business analytics
uv run python calculate_sales_tax.py

cd ..
```

#### Expected Output
```
🚀 Spark Declarative Pipelines (SDP) Examples
==================================================
🏪 Running BrickFood E-commerce Pipeline...
==================================================
1. Executing SDP pipeline...
2. Querying order data...
3. Calculating sales tax and analytics...
✅ BrickFood pipeline completed successfully!
```

## Oil Rigs Industrial Monitoring Pipeline

### Purpose
Simulates a comprehensive industrial IoT sensor monitoring system for oil drilling operations, tracking critical operational parameters across multiple geographic locations in Texas.

### Data Model
The pipeline creates the following materialized views:

**Base Data Sources:**
- **`permian_rig_mv`** - Sensor data from Permian Basin (Midland, Texas: 31.9973°N, -102.0779°W)
- **`eagle_ford_rig_mv`** - Sensor data from Eagle Ford Shale (Karnes City, Texas: 28.8851°N, -97.9006°W)

**Sensor-Specific Views:**
- **`temperature_events_mv`** - Temperature readings in Fahrenheit (150-350°F range)
- **`pressure_events_mv`** - Pressure readings in PSI (2000-5000 PSI range)
- **`water_level_events_mv`** - Water level readings in feet (100-500 ft range)

### Key Features
- **Multi-Location Monitoring**: Real geographic coordinates for Texas oil fields
- **High-Volume Data**: 10,000+ sensor events per rig with 15-minute intervals
- **Multi-Sensor Types**: Temperature, pressure, and water level monitoring
- **Time-Series Analysis**: Historical data tracking with timestamp precision
- **Data Visualization**: Interactive Plotly charts for temperature analysis
- **Statistical Reporting**: Min/max/average calculations by rig location
- **Real-time Simulation**: Generates sensor data for the past 2 days

### Sensor Specifications
- **Temperature**: 150-350°F operational range
- **Pressure**: 2000-5000 PSI operational range  
- **Water Level**: 100-500 feet depth range
- **Data Frequency**: Every 15 minutes
- **Geographic Coverage**: Permian Basin and Eagle Ford Shale regions

### Running the Oil Rigs Pipeline

#### Using the CLI Interface (Recommended)
```bash
# Run the complete Oil Rigs pipeline
uv run python main.py oil-rigs

# Get help
uv run python main.py --help
```

#### Manual Execution (Advanced)
```bash
cd oil_rigs/

# 1. Execute the SDP pipeline to create materialized views
./run_pipeline.sh

# 2. Query and display sensor data from all materialized views
uv run python query_oil_rigs_tables.py

# 3. Generate interactive temperature visualization
uv run python plot_temperatures.py

cd ..
```

#### Expected Output
```
🚀 Spark Declarative Pipelines (SDP) Examples
==================================================
🛢️  Running Oil Rigs Industrial Monitoring Pipeline...
==================================================
1. Executing SDP pipeline...
2. Querying sensor data...
3. Generating temperature visualizations...
✅ Oil Rigs pipeline completed successfully!
```

## SDP Framework Architecture

### Core Components

1. **Pipeline Configuration** (`pipeline.yml`)
   - Defines transformation discovery patterns
   - Includes both Python (`.py`) and SQL (`.sql`) transformations
   ```yaml
   definitions:
     - glob:
         include: transformations/**/*.py
     - glob:
         include: transformations/**/*.sql
   ```

2. **Materialized Views**
   - **Python**: Use `@sdp.materialized_view` decorator for complex data generation
   - **SQL**: Standard SQL DDL for filtering and aggregation operations

3. **Pipeline Execution**
   - Uses `spark-pipelines run` command with Hive catalog support
   - Automatically manages dependencies between materialized views
   - Stores data in local Spark warehouse directories

### Materialized View Types

**Python Materialized Views** (`@sdp.materialized_view`)
```python
@sdp.materialized_view
def my_data_view() -> DataFrame:
    # Complex data transformation logic
    return spark.createDataFrame(data, schema)
```

**SQL Materialized Views** (`.sql` files)
```sql
CREATE MATERIALIZED VIEW my_filtered_view AS
SELECT * FROM base_view
WHERE condition = 'value';
```

### Data Storage
- **Spark Warehouse**: Local file-based storage in `spark-warehouse/` directories
- **Metastore**: Derby database for metadata management (`metastore_db/`)
- **Parquet Format**: Efficient columnar storage for analytical queries

## Usage Patterns

### 1. Data Generation
Both pipelines demonstrate synthetic data generation for testing and development:
- **BrickFood**: Generates random e-commerce orders with realistic product catalogs
- **Oil Rigs**: Simulates sensor readings with realistic operational ranges

### 2. Data Transformation
Showcases the hybrid approach of Python + SQL transformations:
- **Python**: Complex business logic, data generation, schema definition
- **SQL**: Filtering, aggregation, and view creation

### 3. Analytics and Reporting
Demonstrates various analytical capabilities:
- **Business Analytics**: Sales summaries, tax calculations, product performance
- **Sensor Analytics**: Statistical analysis, time-series visualization, operational monitoring

### 4. Visualization
- **Plotly Integration**: Interactive charts with Spark DataFrame direct integration
- **Statistical Reporting**: Automated summary statistics and data quality checks

## UV Project Configuration

### Dependencies Management
The project uses UV for modern Python dependency management. All dependencies are specified in `pyproject.toml`:

```toml
[project]
name = "spark-declarative-pipelines-examples"
version = "0.1.0"
description = "Example implementations of Spark Declarative Pipelines (SDP) with PySpark 4.1.0.dev1 and Spark Connect"
requires-python = ">=3.11"

dependencies = [
    "faker>=37.6.0",
    "plotly>=6.3.0", 
    "pyspark==4.1.0.dev1",
    "pyspark-connect==4.1.0.dev1",
]
```

### Key Features
- **Pinned PySpark Version**: Uses exact version `4.1.0.dev1` for consistency
- **Spark Connect Support**: Includes `pyspark-connect` for remote cluster connectivity
- **Development Dependencies**: Optional dev dependencies for testing and linting
- **Virtual Environment**: Automatic isolation with `.venv/` directory
- **Lock File**: `uv.lock` ensures reproducible builds across environments

### UV Commands
```bash
# Install dependencies and sync environment
uv sync

# Add a new dependency
uv add package-name

# Remove a dependency
uv remove package-name

# Run commands in the virtual environment
uv run python script.py

# Show project info
uv show

# Update dependencies
uv lock --upgrade
```

## Dependencies

### Core Dependencies
- **PySpark 4.1.0.dev1**: Latest development version of Apache Spark
- **PySpark Connect 4.1.0.dev1**: Spark Connect client for remote clusters
- **Faker 37.6.0+**: Realistic synthetic data generation
- **Plotly 6.3.0+**: Interactive data visualizations

### System Requirements
- **Python 3.11+**: Required minimum Python version
- **Spark Declarative Pipelines CLI**: External tool for pipeline execution
- **Java 11+**: Required by PySpark (automatically handled by Spark)

### Spark Configuration
- **Catalog**: Hive metastore support required
- **Warehouse**: Local file system storage
- **Backend**: Plotly integration for visualization

## Development Workflow

1. **Define Transformations**: Create materialized views in `transformations/` directory
2. **Configure Pipeline**: Update `pipeline.yml` to include new transformations
3. **Execute Pipeline**: Run `./run_pipeline.sh` to build materialized views
4. **Query Data**: Use provided query scripts or create custom analytics
5. **Visualize Results**: Generate charts and reports using the analysis scripts

## Best Practices

### Code Organization
- Separate data generation from business logic
- Use SQL for simple filtering and aggregation
- Use Python for complex transformations and data generation
- Keep utility functions in shared modules

### Performance Considerations
- SDP automatically manages materialized view dependencies
- Parquet storage provides efficient analytical query performance
- Local file system suitable for development; consider distributed storage for production

### Data Quality
- Both pipelines include data validation and error handling
- Schema enforcement through Spark StructType definitions
- Realistic data ranges and constraints for synthetic data

## Getting Started

### Quick Start
1. **Install UV** and **setup the project** (see Prerequisites section above)
2. **Verify SDP CLI** is installed: `spark-pipelines --help`
3. **Run a pipeline**: `uv run python main.py brickfood` or `uv run python main.py oil-rigs`

### Step-by-Step
1. **Choose a Pipeline**: Start with either BrickFood or Oil Rigs
2. **Review Configuration**: Examine the `pipeline.yml` and transformation files
3. **Execute Pipeline**: Use the CLI interface or manual execution
4. **Explore Data**: Generated materialized views are stored in `spark-warehouse/`
5. **Analyze Results**: Review the analytics output and visualizations

### Troubleshooting

#### SDP CLI Not Available
If you see this error:
```
❌ ERROR: SDP pipeline command not available!
   This requires the Spark Declarative Pipelines CLI to be installed.
   Please install the SDP CLI before running this pipeline.
```

**Solution**: Install the Spark Declarative Pipelines CLI tool. The `spark-pipelines` command must be available in your PATH.

#### Environment Issues
```bash
# Recreate the virtual environment
uv sync --reinstall

# Check Python version
uv run python --version

# Verify PySpark installation
uv run python -c "import pyspark; print(pyspark.__version__)"
```

#### Permission Issues
```bash
# Make sure shell scripts are executable
chmod +x brickfood/run_pipeline.sh
chmod +x oil_rigs/run_pipeline.sh
```

## Example Output

### BrickFood Analytics
```
Approved Orders with Total Prices and Sales Tax (15%):
+--------------------+------------+-------+-------------+------------+----------+----------+---------------+
|order_id           |order_item  |price  |items_ordered|date_ordered|total_price|sales_tax|total_with_tax|
+--------------------+------------+-------+-------------+------------+----------+----------+---------------+
|uuid-example       |Laptop      |750.50 |2            |2024-01-15  |1501.00   |225.15   |1726.15       |
```

### Oil Rigs Sensor Data
```
Temperature Readings from All Rigs:
+--------------------+---------------+----------------+--------------+-------------+-------------------+--------+---------+
|event_id           |rig_name       |location        |region        |temperature_f|timestamp          |latitude|longitude|
+--------------------+---------------+----------------+--------------+-------------+-------------------+--------+---------+
|uuid-example       |permian_rig    |Midland, Texas  |Permian Basin |245.67       |2024-01-15 10:30:00|31.9973|-102.0779|
```

## Notes

### File Structure
- **Derby Database**: The `metastore_db/` directories contain auto-generated Derby database files for Spark's Hive metastore. These should not be modified manually.
- **Warehouse Data**: The `spark-warehouse/` directories contain the actual data files in Parquet format.
- **Virtual Environment**: The `.venv/` directory is automatically created by UV and should not be committed to version control.
- **Lock File**: The `uv.lock` file should be committed to ensure reproducible builds.

### Development Focus
- **Local Development**: These examples are designed for local development and learning
- **UV Integration**: Modern Python package management with automatic dependency resolution
- **Production Ready**: The UV project structure is suitable for production deployment
- **Spark Connect**: Supports both local and remote Spark cluster execution

### CLI Interface
The project includes a comprehensive CLI interface (`main.py`) that:
- Provides user-friendly pipeline execution
- Handles errors gracefully with clear messages
- Supports both pipelines with consistent interface
- Exits with proper error codes for scripting integration

This SDP implementation provides a solid foundation for understanding declarative data pipeline development with Apache Spark, combining the power of Python's flexibility with SQL's simplicity, all managed through modern UV tooling for comprehensive data processing workflows.