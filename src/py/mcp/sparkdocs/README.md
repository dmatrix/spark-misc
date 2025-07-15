# SparkDocs MCP Server

A Model Context Protocol (MCP) server that provides access to Apache Spark documentation through intelligent web search and content extraction.

## Overview

SparkDocs is a custom MCP server that allows Cursor (and other MCP-compatible clients) to search and retrieve documentation from various Apache Spark sources. It uses the Serper API for web search and BeautifulSoup for content extraction to provide relevant Spark documentation directly within your development environment.

## Features

- **Multi-section Documentation Search**: Search across different Spark documentation sections:
  - `python-api` - Python API reference
  - `python-datasource` - Python data source tutorials
  - `pyspark` - PySpark user guide
  - `spark-sql` - Spark SQL reference

- **Intelligent Content Extraction**: Automatically extracts and formats relevant documentation content
- **Fast Search**: Uses Google Serper API for quick and accurate search results
- **MCP Integration**: Seamlessly integrates with Cursor and other MCP-compatible tools

## Prerequisites

- Python 3.13 or higher
- A Serper API key (for web search functionality)
- Cursor editor (or other MCP-compatible client)

## Installation

1. **Clone or navigate to the project directory**:
   ```bash
   cd /Users/jules/git-repos/spark-misc/src/py/mcp/sparkdocs
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Set up environment variables**:
   Create a `.env` file in the project root:
   ```bash
   SERPER_API_KEY=your_serper_api_key_here
   ```

   You can get a free Serper API key from [serper.dev](https://serper.dev)

## Configuration

### Cursor MCP Configuration

Add the following to your Cursor MCP configuration file (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "sparkdocs": {
      "command": "python3 /Users/jules/git-repos/spark-misc/src/py/mcp/sparkdocs/main.py"
    }
  }
}
```

## Usage

### Starting the Server

The server runs automatically when Cursor starts (if configured in `mcp.json`). You can also run it manually:

```bash
python3 main.py
```

### Using in Cursor

Once configured, you can use the SparkDocs MCP server in Cursor by:

1. **Asking questions about Spark**: The AI assistant can now access Spark documentation
2. **Getting code examples**: Request specific Spark code examples and implementations
3. **Troubleshooting**: Get help with Spark-related issues and errors

### Example Queries

- "How to create a Spark DataFrame from a CSV file?"
- "Show me PySpark SQL examples"
- "How to use Spark Data Source API?"
- "What are the best practices for Spark performance optimization?"

## Testing

You can verify that the MCP server and its documentation search tool are working by running a built-in test harness. This will execute a couple of example queries and print the results to your terminal.

### Run the Test Harness

From the project directory, run:

```bash
python3 main.py test
```

Or, if you are using the project's virtual environment:

```bash
.venv/bin/python main.py test
```

This will perform two example documentation queries:
- "How to use Spark SQL?" (in the `spark-sql` section)
- "How to create a DataFrame from CSV?" (in the `python-api` section)

You should see output that includes the raw text of the relevant Spark documentation pages. If there are errors (such as missing dependencies or API keys), they will be printed to the terminal.

**Note:** The test harness does not require MCP protocol clients (like Cursor) and is intended for quick verification and debugging.

## Project Structure

```
sparkdocs/
├── main.py              # Main MCP server implementation
├── pyproject.toml       # Project dependencies and metadata
├── README.md           # This file
├── uv.lock             # Locked dependencies
├── .env                # Environment variables (create this)
└── .venv/              # Virtual environment (created by uv)
```

## API Reference

### MCP Tool: `get_spark_docs`

Searches Spark documentation for a given query in a specific section.

**Parameters:**
- `query` (str): The search query (e.g., "How to use Spark SQL")
- `section` (str): The documentation section to search in

**Supported Sections:**
- `python-api` - Python API reference
- `python-datasource` - Python data source tutorials  
- `pyspark` - PySpark user guide
- `spark-sql` - Spark SQL reference

**Returns:**
- Extracted text content from relevant Spark documentation pages

## Development

### Adding New Documentation Sections

To add support for new Spark documentation sections:

1. Add the URL to the `docs_urls` dictionary in `main.py`:
   ```python
   docs_urls = {
       # ... existing sections ...
       "new-section": "https://spark.apache.org/docs/latest/new-section.html",
   }
   ```

2. Update the docstring in the `get_spark_docs` function to include the new section.

### Dependencies

- `mcp[cli]>=1.10.1` - MCP server framework
- `httpx>=0.28.1` - HTTP client for API calls
- `beautifulsoup4` - HTML parsing (via mcp dependency)
- `python-dotenv` - Environment variable loading (via mcp dependency)

## Troubleshooting

### Common Issues

1. **"No results found"**: 
   - Check your Serper API key is valid
   - Verify the query is specific enough
   - Ensure the section exists in the `docs_urls` dictionary

2. **Timeout errors**:
   - The server has a 30-second timeout for web requests
   - Check your internet connection
   - Try a more specific query

3. **MCP connection issues**:
   - Verify the path in your Cursor MCP configuration is correct
   - Ensure all dependencies are installed
   - Check that the `.env` file exists with your API key

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is part of the spark-misc repository. Please refer to the main repository for licensing information.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the Spark documentation directly
3. Open an issue in the repository

---

**Note**: This MCP server requires an active internet connection and a valid Serper API key to function properly.
