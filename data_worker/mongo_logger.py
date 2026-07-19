import os
import sys
import logging
from datetime import datetime, timezone
from pymongo import MongoClient

class PyMongoFilter(logging.Filter):
    def filter(self, record):
        return not record.name.startswith('pymongo')

class MongoHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        host = os.environ.get("MONGO_HOST")
        port = os.environ.get("MONGO_PORT")
        user = os.environ.get("MONGO_ROOT_USER")
        password = os.environ.get("MONGO_ROOT_PASSWORD")
        db_name = os.environ.get("MONGO_DB_NAME")
        collection_name = os.environ.get("MONGO_LOGS_COLLECTION")

        uri = f"mongodb://{user}:{password}@{host}:{port}/"
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.addFilter(PyMongoFilter())

    def emit(self, record):
        try:
            message_str = record.getMessage()
            structured_item = None

            # Intercept Scrapy's native dictionary arguments before stringification
            if isinstance(record.args, dict) and "item" in record.args:
                try:
                    structured_item = dict(record.args["item"])
                    
                    # Safely drop the massive binary payload natively from the dict
                    structured_item.pop("file_bytes", None)
                    
                    # Isolate the core string message (e.g., "Dropped: Duplicate...") 
                    # and discard the stringified dict that usually follows on new lines
                    message_str = message_str.splitlines()[0].strip()
                except Exception:
                    pass # Fallback to standard logging if dict conversion fails

            log_document = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": message_str,
            }
            
            # Embed the native dictionary into the MongoDB document
            if structured_item:
                log_document["item"] = structured_item
            
            ignored_keys = {
                "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs", "msg", "name",
                "pathname", "process", "processName", "relativeCreated", "stack_info", "thread", 
                "threadName", "spider"
            }
            
            for key, val in record.__dict__.items():
                if key == "file_bytes":
                    continue

                if key not in ignored_keys and key not in log_document:
                    if isinstance(val, (int, float, bool, str, dict, list, type(None))):
                        log_document[key] = val
                    else:
                        log_document[key] = str(val)

            if record.exc_info:
                log_document["exception"] = self.formatter.formatException(record.exc_info) if self.formatter else str(record.exc_info)

            self.collection.insert_one(log_document)
        except Exception as e:
            print(f"CRITICAL: MongoLogger failed to connect or write: {e}", file=sys.stderr)
            self.handleError(record)