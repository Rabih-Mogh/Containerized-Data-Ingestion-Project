# File: scraper/scraper/pipelines.py
import os
import io
import re
import hashlib
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

logger = logging.getLogger(__name__)

class StoragePipeline:
    """
    Bronze (Landing) layer pipeline.
    Implements strict idempotency via MongoDB lookups before executing storage 
    transactions, guaranteeing an immutable, append-only ledger.
    """

    def open_spider(self, spider):
        """Initializes raw infrastructure connections when the spider boots."""
        self._init_mongo()
        self._init_minio()
        
        # Central metric registry tracking metrics throughout execution
        spider.metrics_discovered = 0
        spider.metrics_processed = 0

    def close_spider(self, spider):
        """Tears down external state resource blocks cleanly upon spider termination."""
        if hasattr(self, "mongo_client"):
            self.mongo_client.close()
            
        # Compile total run metrics for the final summary block
        discovered = getattr(spider, 'metrics_discovered', 0)
        processed = getattr(spider, 'metrics_processed', 0)
        
        summary_ctx = {
            "start_date": getattr(spider, 'from_date', None),
            "end_date": getattr(spider, 'to_date', None),
            "partition_len": getattr(spider, 'calculated_partition_len', None),
            "run_statistics": {
                "total_records_discovered": discovered,
                "total_records_processed_successfully": processed,
                "total_records_failed": max(0, discovered - processed)
            }
        }
        # Output final JSON summary block containing overall run statistics
        logger.info("Spider execution completed. Operational run summary block finalized.", extra=summary_ctx)

    def process_item(self, item, spider):
        item_adapter = ItemAdapter(item)
        
        # Track metrics: Discovered a new record
        spider.metrics_discovered += 1

        # Base operational state logging fields
        ctx = {
            "start_date": getattr(spider, 'from_date', None),
            "end_date": getattr(spider, 'to_date', None),
            "partition_len": getattr(spider, 'calculated_partition_len', None),
            "body_id": item_adapter.get("body_id")
        }

        # 1. Generate an immutable UTC ISO operational timeline anchor
        item_adapter["scraped_at"] = datetime.now(timezone.utc).isoformat()
        
        # Pull payload retrieved asynchronously by the spider
        file_bytes = item_adapter.get("file_bytes")
        link_to_doc = item_adapter.get("link_to_doc")
        
        # If no file payload exists, persist structural metadata only
        if not file_bytes or not link_to_doc:
            self._save_to_mongo(item_adapter, spider, ctx)
            return item

        try:
            content_type = item_adapter.get("content_type")

            # 2. Extract standard extensions from resource trailing path strings
            parsed_url = urlparse(link_to_doc)
            path_lower = parsed_url.path.lower()
            if path_lower.endswith(".pdf"):
                ext = "pdf"
            elif path_lower.endswith(".docx"):
                ext = "docx"
            else:
                ext = "html"

            # 3. Strip HTML comments if and only if the file is an HTML document
            if ext == "html":
                file_bytes = re.sub(b"<!--.*?-->", b"", file_bytes, flags=re.DOTALL)
                logger.debug(f"Sanitization Guard: Stripped HTML comments from {item_adapter.get('identifier')}", extra=ctx)

            # 4. Compute static cryptographic checksum footprint
            file_hash = hashlib.sha256(file_bytes).hexdigest()
            item_adapter["file_hash"] = file_hash

            # 5. Strict Idempotency Check
            identifier = item_adapter.get("identifier")
            if self._is_duplicate(identifier, file_hash, ctx):
                logger.info(f"Idempotency Guard: Document {identifier} already exists in Bronze layer. Dropping item.", extra=ctx)
                raise DropItem(f"Duplicate unchanged file detected for identifier: {identifier}")

            # 6. Bind parameters to shape a partition-aligned storage pattern
            body_id = item_adapter.get("body_id", "unknown_body")
            partition_date = item_adapter.get("partition_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
            
            minio_object_name = f"{body_id}/{partition_date}/{identifier}_{file_hash}.{ext}"
            item_adapter["file_path"] = minio_object_name

            # 7. Route payload cleanly straight into our MinIO object store layer
            data_stream = io.BytesIO(file_bytes)
            self.minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=minio_object_name,
                data=data_stream,
                length=len(file_bytes),
                content_type=content_type if ext != "html" else "text/html"
            )
            logger.info(f"Stored raw document file object inside MinIO -> {self.minio_bucket}/{minio_object_name}", extra=ctx)

        except DropItem:
            raise
        except Exception as e:
            error_ctx = dict(ctx)
            error_ctx.update({
                "failed_url": link_to_doc,
                "error_msg": str(e),
                "identifier": item_adapter.get("identifier")
            })
            logger.error(f"File storage operational asset transaction halted: {str(e)}", extra=error_ctx)
            return item

        # 8. Clean up binary payload from item state prior to BSON serialization
        if "file_bytes" in item_adapter:
            del item_adapter["file_bytes"]
        if "content_type" in item_adapter:
            del item_adapter["content_type"]

        # 9. Synchronize unstructured relational metadata states to NoSQL document store
        self._save_to_mongo(item_adapter, spider, ctx)
        return item

    def _is_duplicate(self, identifier, file_hash, ctx):
        """Checks the landing collection to see if this specific file version exists."""
        try:
            query = {"identifier": identifier, "file_hash": file_hash}
            existing_record = self.mongo_collection.find_one(query, projection={"_id": 1})
            return existing_record is not None
        except Exception as e:
            logger.error(f"Idempotency verification database lookup failed: {e}", extra=ctx)
            return False

    def _save_to_mongo(self, item_adapter, spider, ctx):
        """Persists item schemas into the configured landing cluster zone collection."""
        try:
            payload = item_adapter.asdict()
            self.mongo_collection.insert_one(payload)
            logger.debug(f"Document entry successfully persisted to MongoDB: {payload.get('identifier')}", extra=ctx)
            
            # Track metrics: Successfully processed record
            spider.metrics_processed += 1
        except Exception as e:
            # Database insertion failure block
            error_ctx = dict(ctx)
            error_ctx.update({
                "failed_url": item_adapter.get("link_to_doc"),
                "error_msg": f"MongoDB Error: {str(e)}",
                "identifier": item_adapter.get("identifier")
            })
            logger.error(f"Failed writing database metadata node to landing engine collection: {e}", extra=error_ctx)

    def _init_mongo(self):                          # TODO: remove default alternate keys and die instead on error
        from pymongo import MongoClient
        host = os.environ["MONGO_HOST"]
        port = os.environ["MONGO_PORT"]
        user = os.environ["MONGO_ROOT_USER"]
        password = os.environ["MONGO_ROOT_PASSWORD"]
        db_name = os.environ["MONGO_DB_NAME"]
        collection_name = os.environ["MONGO_LANDING_COLLECTION"]
        uri = f"mongodb://{user}:{password}@{host}:{port}/"
        self.mongo_client = MongoClient(uri)
        self.mongo_collection = self.mongo_client[db_name][collection_name]

    def _init_minio(self):                          # TODO: remove default alternate keys and die instead on error
        from minio import Minio
        host = os.environ["MINIO_HOST"]
        port = os.environ["MINIO_API_PORT"]
        access_key = os.environ["MINIO_ROOT_USER"]
        secret_key = os.environ["MINIO_ROOT_PASSWORD"]
        bucket_name = os.environ["MINIO_LANDING_BUCKET"]
        self.minio_client = Minio(f"{host}:{port}", access_key=access_key, secret_key=secret_key, secure=False)
        self.minio_bucket = bucket_name
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Failed to verify or create MinIO bucket {bucket_name}: {e}")