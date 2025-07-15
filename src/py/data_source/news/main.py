import os
from datetime import datetime, timedelta
from typing import Iterator, Optional, Any, Dict, List
import logging
import time

from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceStreamReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.datasource import InputPartition
from newsapi import NewsApiClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NewsInputPartition(InputPartition):
    """Represents a partition of news data to be processed."""
    
    def __init__(self, query: str, sources: Optional[str] = None, 
                 from_date: Optional[str] = None, to_date: Optional[str] = None,
                 page: int = 1, page_size: int = 100, sort_by: str = 'publishedAt'):
        self.query = query
        self.sources = sources
        self.from_date = from_date
        self.to_date = to_date
        self.page = page
        self.page_size = page_size
        self.sort_by = sort_by


class NewsStreamingPartition(InputPartition):
    """Represents a streaming partition with offset ranges."""
    
    def __init__(self, start_offset: int, end_offset: int, query: str, sources: Optional[str] = None):
        self.start = start_offset
        self.end = end_offset
        self.query = query
        self.sources = sources


class NewsDataSourceReader(DataSourceReader):
    """Batch reader for news data."""
    
    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = options
        self.api_key = options.get('api_key') or os.getenv('NEWSAPI_KEY')
        if not self.api_key:
            raise ValueError("API key is required. Set NEWSAPI_KEY environment variable or pass 'api_key' option")
    
    def partitions(self) -> List[NewsInputPartition]:
        """Create partitions for parallel processing."""
        query = self.options.get('query', 'technology')
        sources = self.options.get('sources')
        from_date = self.options.get('from_date')
        to_date = self.options.get('to_date', datetime.now().strftime('%Y-%m-%d'))
        page_size = int(self.options.get('page_size', '100'))
        sort_by = self.options.get('sort_by', 'publishedAt')
        
        # Create multiple partitions for parallel processing
        # Each partition handles a different page of results
        # Default to 1 page to work with NewsAPI free tier (100 results max)
        max_pages = int(self.options.get('max_pages', '1'))
        partitions = []
        
        for page in range(1, max_pages + 1):
            partition = NewsInputPartition(
                query=query,
                sources=sources,
                from_date=from_date,
                to_date=to_date,
                page=page,
                page_size=page_size,
                sort_by=sort_by
            )
            partitions.append(partition)
        
        return partitions
    
    def read(self, partition: NewsInputPartition) -> Iterator[tuple]:
        """Read news data from a specific partition."""
        try:
            # Import inside the method to ensure it's available on worker nodes
            from newsapi import NewsApiClient
            
            client = NewsApiClient(api_key=self.api_key)
            
            # Build query parameters
            params = {
                'q': partition.query,
                'page': partition.page,
                'page_size': partition.page_size,
                'sort_by': partition.sort_by
            }
            
            if partition.sources:
                params['sources'] = partition.sources
            if partition.from_date:
                params['from_param'] = partition.from_date
            if partition.to_date:
                params['to'] = partition.to_date
                
            # Fetch news articles
            response = client.get_everything(**params)
            
            if response['status'] == 'ok':
                articles = response.get('articles', [])
                logger.info(f"Fetched {len(articles)} articles from page {partition.page}")
                
                for article in articles:
                    yield self._process_article(article)
            else:
                logger.error(f"API error: {response.get('message', 'Unknown error')}")
                
        except Exception as e:
            error_msg = str(e)
            if 'maximumResultsReached' in error_msg:
                logger.warning(f"API limit reached for page {partition.page}. This is normal for free tier NewsAPI accounts.")
                # Don't yield any results for this partition, but don't crash
                return
            else:
                logger.error(f"Error reading partition {partition.page}: {e}")
                raise
    
    def _process_article(self, article: Dict[str, Any]) -> tuple:
        """Process a single article into a tuple matching our schema."""
        # Parse published date
        published_at = article.get('publishedAt')
        if published_at:
            try:
                published_at = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            except:
                published_at = None
        
        return (
            article.get('source', {}).get('id'),
            article.get('source', {}).get('name'),
            article.get('title'),
            article.get('description'),
            article.get('url'),
            article.get('urlToImage'),
            published_at,
            article.get('content'),
            article.get('author')
        )


class NewsDataSourceStreamReader(DataSourceStreamReader):
    """Streaming reader for real-time news updates."""
    
    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = options
        self.api_key = options.get('api_key') or os.getenv('NEWSAPI_KEY')
        if not self.api_key:
            raise ValueError("API key is required. Set NEWSAPI_KEY environment variable or pass 'api_key' option")
    
    def partitions(self, start: dict, end: dict) -> List[NewsStreamingPartition]:
        """Create partitions for streaming based on offset ranges."""
        query = self.options.get('query', 'breaking news')
        sources = self.options.get('sources')
        
        return [NewsStreamingPartition(
            start_offset=start["offset"],
            end_offset=end["offset"],
            query=query,
            sources=sources
        )]
    
    def latestOffset(self) -> dict:
        """Return the latest offset for streaming."""
        # For news streaming, we increment by batch size each time
        # This ensures we don't re-read the same data
        if not hasattr(self, '_current_offset'):
            self._current_offset = 0
        
        batch_size = int(self.options.get('batch_size', '10'))
        self._current_offset += batch_size
        return {"offset": self._current_offset}
    
    def initialOffset(self) -> dict:
        """Return the initial offset for streaming."""
        # Start from 0 for the first batch
        return {"offset": 0}
    
    def read(self, partition: NewsStreamingPartition) -> Iterator[tuple]:
        """Read streaming news data for a specific offset range."""
        try:
            from newsapi import NewsApiClient
            
            client = NewsApiClient(api_key=self.api_key)
            
            # Calculate how many articles to fetch based on offset range
            batch_size = partition.end - partition.start
            if batch_size <= 0:
                return
            
            # Fetch top headlines for this batch
            params = {
                'q': partition.query,
                'page_size': min(batch_size, 100),  # NewsAPI max page size is 100
                'page': 1
            }
            
            if partition.sources:
                params['sources'] = partition.sources
            
            logger.info(f"Fetching {batch_size} articles from NewsAPI for streaming")
            response = client.get_top_headlines(**params)
            
            if response['status'] == 'ok':
                articles = response.get('articles', [])
                logger.info(f"Retrieved {len(articles)} articles for streaming batch")
                
                # Yield articles up to the batch size
                for i, article in enumerate(articles[:batch_size]):
                    yield self._process_article(article)
            else:
                logger.error(f"API error: {response.get('message', 'Unknown error')}")
                
        except Exception as e:
            error_msg = str(e)
            if 'maximumResultsReached' in error_msg:
                logger.warning("API limit reached. This is normal for free tier NewsAPI accounts.")
            else:
                logger.error(f"Error reading streaming partition: {e}")
                raise
    
    def commit(self, end: dict) -> None:
        """Called when the query has finished processing data before end offset."""
        logger.info(f"Committed streaming data up to offset {end['offset']}")
        pass
    
    def _process_article(self, article: Dict[str, Any]) -> tuple:
        """Process a single article into a tuple matching our schema."""
        # Parse published date
        published_at = article.get('publishedAt')
        if published_at:
            try:
                published_at = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            except:
                published_at = None
        
        return (
            article.get('source', {}).get('id'),
            article.get('source', {}).get('name'),
            article.get('title'),
            article.get('description'),
            article.get('url'),
            article.get('urlToImage'),
            published_at,
            article.get('content'),
            article.get('author')
        )


class NewsDataSource(DataSource):
    """
    Apache Spark News Data Source using the Python Data Source API.
    
    This data source provides access to news articles from NewsAPI.org.
    
    Options:
    - query: Search query (default: 'technology')
    - sources: Comma-separated list of news sources (optional)
    - from_date: Start date in YYYY-MM-DD format (optional)
    - to_date: End date in YYYY-MM-DD format (optional)
    - page_size: Number of articles per page (default: 100)
    - max_pages: Maximum number of pages to fetch (default: 1 for free tier compatibility)
    - sort_by: Sort order - publishedAt, relevancy, popularity (default: publishedAt)
    - api_key: NewsAPI key (can also use NEWSAPI_KEY environment variable)
    - interval: Streaming interval in seconds (default: 300)
    """
    
    @classmethod
    def name(cls) -> str:
        return "news"
    
    def schema(self) -> str:
        return """
            source_id string,
            source_name string,
            title string,
            description string,
            url string,
            url_to_image string,
            published_at timestamp,
            content string,
            author string
        """
    
    def reader(self, schema: StructType) -> NewsDataSourceReader:
        """Create a batch reader for news data."""
        return NewsDataSourceReader(schema, self.options)
    
    def streamReader(self, schema: StructType) -> NewsDataSourceStreamReader:
        """Create a streaming reader for news data."""
        return NewsDataSourceStreamReader(schema, self.options)


def main():
    """Main function to demonstrate the news data source."""
    print("News Data Source for Apache Spark")
    print("This data source provides access to news articles from NewsAPI.org")
    print("Set NEWSAPI_KEY environment variable or pass 'api_key' option to use.")
    
    # Check if API key is available
    api_key = os.getenv('NEWSAPI_KEY')
    if not api_key:
        print("Warning: NEWSAPI_KEY environment variable not set!")
        print("Get your free API key at: https://newsapi.org/register")
    else:
        print(f"API key found: {api_key[:8]}...")


if __name__ == "__main__":
    main()
