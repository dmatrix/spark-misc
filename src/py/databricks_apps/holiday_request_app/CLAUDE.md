# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Holiday Request Manager - a Databricks App that demonstrates the integration of Databricks Lakebase (PostgreSQL) with Databricks Apps runtime. It's a Streamlit-based web application for managing employee holiday requests with approval/decline functionality.

## Technology Stack

- **Frontend**: Streamlit (Python web framework)
- **Database**: Databricks Lakebase (managed PostgreSQL)
- **Authentication**: Databricks OAuth tokens
- **ORM**: SQLAlchemy with psycopg driver
- **Runtime**: Databricks Apps (serverless)

## Development Commands

### Environment Setup

#### Using uv (Recommended)
```bash
# Initialize a new uv project (if starting fresh)
uv init

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # On macOS/Linux
# or .venv\Scripts\activate  # On Windows

# Install dependencies using uv
uv pip install -r requirements.txt

# Or install in development mode with optional dev dependencies
uv pip install -e ".[dev]"

# Add new dependencies with uv
uv add streamlit pandas databricks-sdk

# Add development dependencies
uv add --dev pytest black ruff
```

#### Alternative: Using pip
```bash
# Activate virtual environment (already created)
source .venv/bin/activate

# Install dependencies using pip
pip install -r requirements.txt

# Or install in development mode with pip
pip install -e ".[dev]"
```

### Running the Application
```bash
# Run locally (for development)
streamlit run app.py --server.port=8000 --server.address=0.0.0.0

# The app.yaml defines the production runtime configuration
```

### Code Quality
```bash
# Format code with Black
black app.py

# Lint with Ruff
ruff check app.py

# Run both linting and formatting
ruff check app.py && black app.py
```

### Testing
```bash
# Run tests (if any are added)
pytest
```

## Architecture

### Database Connection Pattern
The app uses a sophisticated OAuth-based PostgreSQL connection:
- `WorkspaceClient` provides OAuth tokens automatically
- SQLAlchemy engine uses `@event.listens_for` to inject OAuth tokens
- Connection pooling with `pool_pre_ping=True` for reliability

### Key Components

**app.py:95-213** - Main Streamlit application with:
- Session state management for request selection
- Dynamic table rendering with radio button selection
- Real-time status updates with color coding
- Manager comment functionality

**Database Schema** (create_tables_and_schema.sql):
- `holidays.holiday_requests` table with SERIAL primary key
- OAuth client ID-based permissions

### Environment Variables
- `PGHOST` - PostgreSQL host (set by Databricks Apps runtime)
- Database connection uses `app_config.client_id` as username

## File Structure

```
holiday_request_app/
├── app.py                          # Main Streamlit application (lines 95-213 contain core logic)
├── app.yaml                        # Databricks App configuration  
├── pyproject.toml                  # Python project configuration
├── requirements.txt                # Production dependencies
├── create_tables_and_schema.sql    # Database initialization script
└── README.md                       # Comprehensive documentation
```

### Key Files Description

**app.py** - Main Streamlit application featuring:
- Session state management for request selection
- Dynamic table rendering with radio button selection
- Real-time status updates with color coding (🟡 Pending, 🟢 Approved, 🔴 Declined)
- Manager comment functionality
- OAuth token authentication with automatic refresh

**app.yaml** - Databricks App configuration:
- Specifies Streamlit runtime on port 8000
- Server address binding to 0.0.0.0 for proper networking
- App metadata and deployment settings

**create_tables_and_schema.sql** - Database setup:
- Creates `holidays` schema and `holiday_requests` table
- Inserts sample data for team members (Andre, Carly, Daniel, etc.)
- Configures proper permissions for app client ID

**pyproject.toml** - Project configuration:
- Defines optional development dependencies
- Project metadata and build configuration

## Database Operations

### Core Functions
- `get_holiday_requests()` - Fetch all requests
- `update_request_status(request_id, status, comment)` - Update request with manager decision
- `get_available_schemas()` - Schema introspection
- `get_tables_in_schema(schema_name)` - Table discovery
- `get_table_data(schema_name, table_name, limit)` - Data browsing

### Database Setup
Run `create_tables_and_schema.sql` to:
1. Create `holidays` schema
2. Create `holiday_requests` table with SERIAL primary key
3. Insert sample data for team members (Andre, Carly, Daniel, Denny, Elise, Holly, Jenni, Jules, Lizzie, Nick, Oleksandra, Robert, and Torey)
4. Grant permissions to app client ID

**Sample Data**: All team members request December 1-12, 2025 vacation period with "Pending" status - perfect for testing the approval workflow.

## Development Notes

### OAuth Token Management
The app automatically refreshes OAuth tokens through `WorkspaceClient`. No manual token management required.

### Streamlit Session State
Key session variables:
- `selected_request_id` - Currently selected holiday request
- Uses `st.rerun()` for immediate UI updates after database changes

### Error Handling
Comprehensive try-catch blocks with user-friendly error messages for:
- Database connection issues
- SQL query failures
- Missing tables/schemas

## Deployment

### Databricks Apps Deployment
This app deploys to Databricks Apps serverless runtime. The deployment process:

1. **Create Databricks App** in your workspace
2. **Add Database resource** and select your Lakebase instance  
3. **Upload code** or clone from repository
4. **Deploy** with one-click deployment

The `app.yaml` configuration specifies:
- Streamlit command with port 8000
- Server address binding to 0.0.0.0 for proper networking
- App metadata and runtime settings

### Prerequisites
- Databricks workspace with Apps enabled
- Lakebase instance (managed PostgreSQL)
- Proper permissions for Unity Catalog integration

## Dependencies

### Core Production Dependencies
The app uses a carefully curated selection of packages (see requirements.txt):

- **`streamlit==1.39.0`** - Beautiful web UI framework for data applications
- **`pandas==2.3.1`** - Data manipulation and analysis library
- **`databricks-sdk==0.57.0`** - Seamless Databricks platform integration and OAuth
- **`sqlalchemy==2.0.41`** - Elegant database ORM for SQL operations
- **`psycopg[binary]==3.2.9`** - High-performance PostgreSQL adapter

### Development Dependencies (Optional)
Defined in pyproject.toml for enhanced development experience:

- **`pytest>=7.0.0`** - Testing framework for unit and integration tests
- **`black>=23.0.0`** - Opinionated code formatter for consistent style
- **`ruff>=0.1.0`** - Fast Python linter and formatter

### Installing Dependencies

```bash
# Install production dependencies only
uv pip install -r requirements.txt

# Install with development dependencies
uv pip install -e ".[dev]"

# Add new dependencies
uv add package-name
uv add --dev dev-package-name
```