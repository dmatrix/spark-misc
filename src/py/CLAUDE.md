# Claude Code Configuration

This document provides configuration and context information for Claude Code to work effectively with this Spark data generation project.

## Project Overview

This project contains data generators and pipelines for creating realistic datasets:
- **Oil Rig Data**: Industrial IoT sensor data (temperature, pressure, water_level)
- **Order Data**: E-commerce transaction data
- **Databricks Integration**: LDP (Logical Data Pipelines) examples

## Key Components

### Data Generators
- `generators/generator_utils.py` - Main utility classes for data generation
  - `OilRigGenerator` - Creates oil rig entities and sensor data
  - `OrderGenerator` - Creates e-commerce order data
  - `BatchManager` - Handles file operations and batch numbering
  - `ScheduledRunner` - Manages scheduled data generation
- `generators/general_oil_rig_data_generator.py` - CLI for oil rig data (default: 100 records)
- `generators/general_order_generator.py` - CLI for order data (default: 100 records)

### Project Structure
```
src/py/
├── generators/                    # Data generation utilities
│   ├── generator_utils.py         # Core generator classes
│   ├── general_oil_rig_data_generator.py  # Oil rig CLI
│   └── general_order_generator.py # Order CLI
├── ldp/                           # Logical Data Pipelines
│   ├── README.md
│   ├── pyproject.toml
│   ├── oil_rigs/                  # DLT pipeline
│   └── utils/                     # Legacy (artifacts only)
└── sdp/                           # Spark Delta Pipelines
    └── utils/                     # SDP-specific utilities
```

## Common Tasks

### Generate Data
```bash
# Oil rig data (100 records default)
cd generators && python general_oil_rig_data_generator.py --interval 1 --end 1

# Order data (100 records default)  
cd generators && python general_order_generator.py --interval 1 --end 1

# Custom batch sizes
cd generators && python general_oil_rig_data_generator.py --batch-size 50 --interval 1 --end 1
```

### Test with Spark
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Read generated data
spark = SparkSession.builder.appName('DataTest').getOrCreate()
df = spark.read.option('multiLine', 'true').json('oil_rig_batches/oil_rig_batch_1.json')
df.show()
```

## Code Standards

### Data Generation
- Use `SENSOR_RANGES` constant for sensor value ranges
- Oil rig sensor types: `temperature`, `pressure`, `water_level`
- Batch files stored in respective directories (e.g., `oil_rig_batches/`)
- Batch numbering managed by `.counter` files

### Testing
- Always test data generators after changes
- Verify Spark DataFrame schema compatibility
- Check sensor value ranges are within expected bounds
- Confirm batch numbering works correctly

## Dependencies

### Python Packages
- `pyspark` - Spark processing
- `faker` - Data generation (in some components)
- Standard library: `json`, `logging`, `random`, `uuid`, `datetime`

### File Operations
- JSON batch files for data storage
- Counter files for batch numbering
- Log files for execution tracking

## Development Notes

### Recent Refactoring
- Consolidated data generation into `generators/generator_utils.py` classes
- Removed redundant `ldp/utils/oil_gen_util.py` and `ldp/utils/order_gen_util.py`
- Updated default batch sizes to 100 records
- Enhanced oil rig data with integrated sensor readings

### Sensor Data Integration
- Oil rig entities now include `sensor_type` and `sensor_value` fields
- Random sensor type selection per rig record
- Sensor values within realistic operational ranges
- Maintains backward compatibility with existing LDP pipelines

#### Sensor Measurement Units
Oil rig sensor data uses industry-standard units for offshore drilling operations:

- **Temperature**: Fahrenheit (°F)
  - Range: 150-350°F
  - Represents equipment/drilling temperatures
- **Pressure**: PSI (Pounds per Square Inch)  
  - Range: 2,000-5,000 PSI
  - Represents drilling/hydraulic system pressure
- **Water Level**: Feet
  - Range: 100-500 feet
  - Represents depth measurements or water table levels

These ranges reflect realistic operational parameters for industrial IoT monitoring in offshore drilling environments.

## Build & Test Commands

```bash
# Test oil rig generator
cd generators && python general_oil_rig_data_generator.py --interval 1 --end 1

# Test order generator
cd generators && python general_order_generator.py --interval 1 --end 1

# Clean batch directories for fresh testing
rm -rf oil_rig_batches/ batch_orders/
```

## Context for Claude Code

When working with this project:
1. **Data Generation**: Use the main generator classes, not individual utility files
2. **Testing**: Always verify generated JSON can be read as Spark DataFrames
3. **Batch Management**: Batch numbering is automatic via counter files
4. **Sensor Data**: Oil rig data includes realistic IoT sensor readings
5. **File Structure**: LDP and SDP directories contain pipeline-specific code

The project focuses on creating realistic datasets for Spark and Databricks pipeline development and testing.