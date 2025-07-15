#
# This script implements an MCP server that answers queries by searching the web,
# focusing on Spark-related documentation. It leverages the Serper API for web search
# and BeautifulSoup for HTML parsing. The server is built using the FastMCP framework,
# and environment variables are managed with dotenv.
#
# The code is adapted and extended from a YouTube tutorial to better serve Spark documentation needs.

#
import os
import json
import httpx

from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()

USER_AGENT = "docs-app/1.0"

# serper api url for searching the web
SERPER_URL="https://google.serper.dev/search"

docs_urls = {
    "python-api": "https://spark.apache.org/docs/latest/api/python/reference/index.html",
    "python-datasource": "https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html",
    "pyspark": "https://spark.apache.org/docs/latest/api/python/user_guide/index.html",
    "spark-sql": "https://spark.apache.org/docs/latest/sql-ref.html",
    "python-datasoruce-examples": "https://github.com/allisonwang-db/pyspark-data-sources"
    # "spark-ml": "https://spark.apache.org/docs/latest/ml-guide.html",
    # "spark-streaming": "https://spark.apache.org/docs/latest/streaming-programming-guide.html",
    # "spark-mllib": "https://spark.apache.org/docs/latest/mllib-guide.html",
    # "spark-mlflow": "https://spark.apache.org/docs/latest/mlflow-tracking.html",
    # "spark-delta": "https://spark.apache.org/docs/latest/delta-intro.html",
    # "spark-delta-guide": "https://spark.apache.org/docs/latest/delta-guide.html",
    # "spark-delta-api": "https://spark.apache.org/docs/latest/api/python/reference/delta/index.html",
    # "spark-delta-sql": "https://spark.apache.org/docs/latest/sql-ref.html#delta-tables",
    # "spark-delta-streaming": "https://spark.apache.org/docs/latest/streaming-guide.html#delta-tables",
}

mcp = FastMCP("sparkdocs", user_agent=USER_AGENT)

#
# Search the web for the best answer helper function
#
async def search_spark_web(query: str) -> dict | None:
    payload = json.dumps({"q": query, "num": 2})

    headers = {
        "X-API-KEY": os.getenv("SERPER_API_KEY"),
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                SERPER_URL, headers=headers, data=payload, timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException:
            return {"organic": []}
   
#
# Fetch the url and return the text helper function
#
async def fetch_spark_url(url: str) -> str:
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=30.0)
            soup = BeautifulSoup(response.text, "html.parser")
            text = soup.get_text()
            return text
        except httpx.TimeoutException:
            return "Timeout error"
    ...

#
# Get the docs tool that will search the web for the best answer
# used my the mcp server to call this tool
#
@mcp.tool()  
async def get_spark_docs(query: str, section: str):
  """
  Search the latest docs for a given query and in the section.
  Sections supported are python-api, python-datasource, pyspark, spark-sql
  Args:
    query: The query to search for (e.g. "How to use Spark SQL")
    section: The library to search in (e.g., "spark-sql")

  Returns:
    Text from the docs
  """
  if section not in docs_urls:
    raise ValueError(f"Section {section} not supported by this tool")
  
  query = f"site:{docs_urls[section]} {query}"
  results = await search_spark_web(query)
  if len(results["organic"]) == 0:
    return "No results found"
  
  text = ""
  for result in results["organic"]:
    text += await fetch_spark_url(result["link"])
  return text


if __name__ == "__main__":
    import asyncio
    import sys
    
    def print_header(msg):
        print("\n" + "="*40)
        print(msg)
        print("="*40)

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Run a couple of test queries
        async def run_tests():
            try:
                print_header("Test 1: PySpark SQL Example")
                result1 = await get_spark_docs("How to use Spark SQL?", "spark-sql")
                print(result1[:1000] + ("..." if len(result1) > 1000 else ""))

                print_header("Test 2: DataFrame from CSV")
                result2 = await get_spark_docs("How to create a DataFrame from CSV?", "python-api")
                print(result2[:1000] + ("..." if len(result2) > 1000 else ""))
            except Exception as e:
                print(f"Test failed: {e}")
        asyncio.run(run_tests())
    else:
        mcp.run(transport="stdio")