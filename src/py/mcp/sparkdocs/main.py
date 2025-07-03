#
# This is a simple mcp server that will search the web for the best answer
# to a given query and return the text from the docs
#
# It uses the serper api to search the web and the beautifulsoup library to parse the html
#
# It uses the mcp.server.fastmcp library to run the server
#
# It uses the dotenv library to load the environment variables
# source: modified and augmented from https://www.youtube.com/watch?v=Ek8JHgZtmcI
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
    mcp.run(transport="stdio")