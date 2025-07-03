# Weather MCP Server

A Model Context Protocol (MCP) server that provides real-time weather information and alerts using the National Weather Service (NWS) API.

## Overview

Weather is a custom MCP server that allows Cursor (and other MCP-compatible clients) to access current weather forecasts and alerts for locations within the United States. It integrates with the National Weather Service API to provide accurate, up-to-date weather information directly within your development environment.

## Features

- **Weather Alerts**: Get active weather alerts for any US state
- **Weather Forecasts**: Retrieve detailed weather forecasts for specific coordinates
- **Real-time Data**: Uses the official National Weather Service API for accurate information
- **Easy Integration**: Seamlessly integrates with Cursor and other MCP-compatible tools
- **No API Keys Required**: Uses the free, public NWS API

## Prerequisites

- Python 3.13 or higher
- Cursor editor (or other MCP-compatible client)
- Internet connection (for API access)

## Installation

1. **Navigate to the project directory**:
   ```bash
   cd /Users/jules/git-repos/spark-misc/src/py/mcp/weather
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

## Configuration

### Cursor MCP Configuration

Add the following to your Cursor MCP configuration file (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "weather": {
      "command": "/Users/jules/git-repos/spark-misc/src/py/mcp/weather/weather.py"
    }
  }
}
```

## Usage

### Starting the Server

The server runs automatically when Cursor starts (if configured in `mcp.json`). You can also run it manually:

```bash
python3 weather.py
```

### Using in Cursor

Once configured, you can use the Weather MCP server in Cursor by:

1. **Getting weather alerts**: Ask about active weather alerts for any US state
2. **Checking forecasts**: Get weather forecasts for specific locations
3. **Weather monitoring**: Stay informed about weather conditions in your area

### Example Queries

- "What are the current weather alerts in California?"
- "Get the weather forecast for coordinates 40.7128, -74.0060"
- "Are there any severe weather alerts in Texas?"
- "What's the weather forecast for New York City?"

## API Reference

### MCP Tool: `get_alerts`

Retrieves active weather alerts for a specific US state.

**Parameters:**
- `state` (str): Two-letter US state code (e.g., "CA", "NY", "TX")

**Returns:**
- Formatted string containing all active weather alerts for the specified state

**Example:**
```python
# Get alerts for California
alerts = await get_alerts("CA")
```

### MCP Tool: `get_forecast`

Retrieves weather forecast for a specific location using latitude and longitude coordinates.

**Parameters:**
- `latitude` (float): Latitude of the location (e.g., 40.7128)
- `longitude` (float): Longitude of the location (e.g., -74.0060)

**Returns:**
- Formatted string containing the weather forecast for the next 5 periods

**Example:**
```python
# Get forecast for New York City
forecast = await get_forecast(40.7128, -74.0060)
```

## Project Structure

```
weather/
├── weather.py           # Main MCP server implementation
├── main.py              # Simple main function (placeholder)
├── pyproject.toml       # Project dependencies and metadata
├── README.md           # This file
├── uv.lock             # Locked dependencies
├── .python-version     # Python version specification
└── .venv/              # Virtual environment (created by uv)
```

## Data Sources

This MCP server uses the **National Weather Service (NWS) API**, which provides:

- **Free Access**: No API keys or authentication required
- **Official Data**: Direct from the National Weather Service
- **Real-time Updates**: Current weather conditions and forecasts
- **Comprehensive Coverage**: Full coverage of the United States

### API Endpoints Used

- `https://api.weather.gov/alerts/active/area/{state}` - Active weather alerts
- `https://api.weather.gov/points/{lat},{lon}` - Location metadata
- `https://api.weather.gov/gridpoints/{office}/{gridX},{gridY}/forecast` - Detailed forecasts

## Development

### Adding New Features

To extend the weather MCP server:

1. **Add new tools**: Create new functions decorated with `@mcp.tool()`
2. **Enhance formatting**: Modify the `format_alert()` function for better output
3. **Add error handling**: Improve the `make_nws_request()` function
4. **Support new data**: Add support for additional NWS API endpoints

### Dependencies

- `mcp[cli]>=1.10.1` - MCP server framework
- `httpx>=0.28.1` - HTTP client for API calls
- `nodejs>=0.1.1` - Node.js runtime (if needed for additional features)
- `npm>=0.1.1` - Node package manager
- `npx>=0.1.6` - Node package runner

## Troubleshooting

### Common Issues

1. **"Unable to fetch alerts or no alerts found"**:
   - Check your internet connection
   - Verify the state code is valid (two-letter US state code)
   - The NWS API might be temporarily unavailable

2. **"Unable to fetch forecast data for this location"**:
   - Verify the coordinates are valid
   - Ensure the location is within the United States
   - Check that the coordinates are in decimal degrees format

3. **Timeout errors**:
   - The server has a 30-second timeout for API requests
   - Check your internet connection
   - The NWS API might be experiencing high load

4. **MCP connection issues**:
   - Verify the path in your Cursor MCP configuration is correct
   - Ensure all dependencies are installed
   - Check that the weather.py file is executable

### API Limitations

- **US Coverage Only**: The NWS API only covers locations within the United States
- **Rate Limiting**: The NWS API has rate limits, though they're generous for normal usage
- **Data Availability**: Some locations may have limited forecast data available

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly with different locations and scenarios
5. Submit a pull request

## License

This project is part of the spark-misc repository. Please refer to the main repository for licensing information.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Verify the NWS API status at [weather.gov](https://www.weather.gov)
3. Open an issue in the repository

---

**Note**: This MCP server requires an active internet connection to access the National Weather Service API. All weather data is provided by the National Weather Service and is subject to their terms of service.
