import random
import pandas as pd

 # Generate 1,000 valid URLs
def generate_valid_urls(num_urls=1000):
     # Common domain names
    domains = [
        "google.com", "wikipedia.org", "amazon.com", "python.org", "github.com",
        "stackoverflow.com", "nytimes.com", "bbc.co.uk", "cnn.com", "nasa.gov",
        "microsoft.com", "apple.com", "linkedin.com", "twitter.com", "facebook.com",
        "medium.com", "quora.com", "yahoo.com", "bing.com", "reddit.com"
    ]

    # Common paths
    paths = [
        "/", "/about", "/contact", "/faq", "/blog", "/news", "/products", 
        "/services", "/team", "/privacy-policy", "/terms-of-service", "/careers",
        "/login", "/signup", "/support", "/help", "/documentation", "/portfolio",
        "/events", "/articles", "/tutorials"
    ]

    # Generate random URLs
    urls = []
    for _ in range(num_urls):
        domain = random.choice(domains)
        path = random.choice(paths)
        url = f"https://{domain}{path}"
        urls.append(url)
    return urls