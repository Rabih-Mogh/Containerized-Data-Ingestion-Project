# Scrapy settings for scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os
from dotenv import load_dotenv

import sys
import logging
import json

import re

from pathlib import Path

# Load environment variables from the .env file located at the project root
# Adjust the path inside find_dotenv() if your .env is located elsewhere
load_dotenv()

BOT_NAME = "legal_Doc_scraper"

SPIDER_MODULES = ["scraper.spiders"]
NEWSPIDER_MODULE = "scraper.spiders"

ADDONS = {}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "scraper (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Concurrency and throttling settings dynamically pulled from environment
CONCURRENT_REQUESTS_PER_DOMAIN = int(os.environ.get("SCRAPY_CONCURRENCY_PER_DOMAIN", 1))
DOWNLOAD_DELAY = int(os.environ.get("SCRAPY_DOWNLOAD_DELAY", 1))

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    # Disable Scrapy's default UserAgent middleware to prevent conflicts
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    
    # Enable your custom RandomHeaderMiddleware
    'scraper.middlewares.RandomHeaderMiddleware': 400,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "scraper.middlewares.ScraperDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "scraper.pipelines.StoragePipeline": 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"

# ==============================================================================
# ==============================================================================

class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        message = record.getMessage()
        structured_item = None
        
        # 1. Intercept Scrapy's native dictionary arguments before stringification
        if isinstance(record.args, dict) and "item" in record.args:
            try:
                structured_item = dict(record.args["item"])
                structured_item.pop("file_bytes", None)
                message = message.splitlines()[0].strip()
            except Exception:
                pass

        # 2. Create standard structured dictionary
        message_dict = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S") + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": message
        }
        
        if structured_item:
            message_dict["item"] = structured_item
            
        # 3. Explicit operational state variables
        state_fields = [
            "start_date", "end_date", "partition_len", "body_id", 
            "failed_url", "error_msg", "identifier", "run_statistics"
        ]
        for field in state_fields:
            if hasattr(record, field):
                message_dict[field] = getattr(record, field)
                
        # 4. Internal fields to explicitly skip
        ignored_keys = {
            "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs", "msg", "name",
            "pathname", "process", "processName", "relativeCreated", "stack_info", "thread", 
            "threadName", "spider"
        }
        
        # 5. Capture miscellaneous context parameters
        for key, val in record.__dict__.items():
            if key == "file_bytes":
                continue
            if key not in ignored_keys and key not in message_dict:
                message_dict[key] = val
                    
        return json.dumps(message_dict, default=str)

# ==============================================================================
# LOGGING INTEGRATION (JSON STDOUT & MONGODB)
# ==============================================================================

data_worker_path = Path(__file__).resolve().parents[2]
if str(data_worker_path) not in sys.path:
    sys.path.insert(0, str(data_worker_path))

root_logger = logging.getLogger()

# 1. Activate the JsonLogFormatter for Standard Output (Console)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(JsonLogFormatter())
root_logger.addHandler(console_handler)

# 2. Prevent Scrapy from duplicating logs with its default text handler
# (This tells Scrapy's engine to respect the root logger we just configured)
LOG_FORMAT = None 

try:
    from mongo_logger import MongoHandler
    # 3. Attach the MongoDB Handler
    root_logger.addHandler(MongoHandler())
except Exception as e:
    logging.getLogger(__name__).error(f"Failed to initialize MongoHandler for Scrapy: {e}")