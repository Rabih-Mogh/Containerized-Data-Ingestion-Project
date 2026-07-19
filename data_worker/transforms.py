import os
import sys
import argparse
import hashlib
import io
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from pymongo import MongoClient
from minio import Minio
from bs4 import BeautifulSoup

# Dynamically import site configurations from the Scrapy project directory
try:
    from site_configs import SITE_CONFIGS
except ImportError:
    # TODO: Die instead >:) !!!
    sys.exit("CRITICAL: Failed to import SITE_CONFIGS. Shutting down.")

try:
    from mongo_logger import MongoHandler
except ImportError:
    MongoHandler = None

# Configure structured JSON-friendly logging output
logger = logging.getLogger("silver_transformer")
logger.setLevel(logging.INFO)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(logging.Formatter('{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}'))
logger.addHandler(stdout_handler)

if MongoHandler:
    try:
        logger.addHandler(MongoHandler())
    except Exception as e:
        logger.error(f"Failed to initialize MongoHandler: {e}")

def execute_transformation(start_date: str, end_date: str, mongo_db, minio_client, config_env: dict):
    """
    Core transformation engine. Natively processes Bronze data to Silver layers.
    Accepts explicit infrastructure instances for clean integration with Dagster Resources.
    """
    logger.info(f"START: Transformation pipeline initiated for window: {start_date} to {end_date}")

    bronze_coll = mongo_db[config_env["MONGO_LANDING_COLLECTION"]]
    silver_coll = mongo_db[config_env["MONGO_TRANSFORMED_COLLECTION"]]
    bronze_bucket = config_env["MINIO_LANDING_BUCKET"]
    silver_bucket = config_env["MINIO_TRANSFORMED_BUCKET"]

    # Ensure Silver target bucket exists in object storage
    if not minio_client.bucket_exists(silver_bucket):
        minio_client.make_bucket(silver_bucket)
        logger.info(f"Initialized target Silver storage container: {silver_bucket}")

    # 1. Query all metadata records within the execution timeframe
    start_iso = datetime.strptime(start_date, "%d/%m/%Y").strftime("%Y-%m-%d")
    end_iso = datetime.strptime(end_date, "%d/%m/%Y").strftime("%Y-%m-%d")

    query = {"partition_date": {"$gte": start_iso, "$lte": end_iso}}
    all_records = list(bronze_coll.find(query))
    
    if not all_records:
        logger.info(f"No documents discovered matching window: {start_date} to {end_date}")
        logger.info(f"END: Transformation pipeline completed for window: {start_date} to {end_date} (0 records)")
        return

    logger.info(f"Extracted {len(all_records)} raw records from Bronze ledger.")

    # 2. Reconcile historical variations (Group by identifier, sort chronologically by scraped_at)
    all_records.sort(key=lambda r: r.get("scraped_at", ""))
    latest_state_snapshots = {}
    for record in all_records:
        identifier = record.get("identifier")
        if identifier:
            latest_state_snapshots[identifier] = record

    logger.info(f"Deduplication step complete: Isolated {len(latest_state_snapshots)} unique 'Latest State' profiles.")

    # 3. Iterate through deduplicated snapshots and download raw files
    for identifier, record in latest_state_snapshots.items():
        file_path = record.get("file_path")
        site_key = record.get("site")

        if not file_path:
            logger.warning(f"Record {identifier} contains no valid object path string. Skipping.")
            continue

        ext = file_path.split(".")[-1].lower()
        
        try:
            # Download target payload from Bronze Landing Bucket
            response = minio_client.get_object(bronze_bucket, file_path)
            raw_bytes = response.read()
            response.close()
            response.release_conn()

            transformed_bytes = raw_bytes
            content_type = "application/octet-stream"

            # 4. Apply DOM selector cleanup rules exclusively to HTML targets
            if ext == "html":
                content_type = "text/html"
                
                # Dynamic lookups: Consult the record context to pull matching site configurations
                if site_key and site_key in SITE_CONFIGS and "content_selector" in SITE_CONFIGS[site_key]:
                    target_selector = SITE_CONFIGS[site_key]["content_selector"]
                else:
                    raise ValueError(f"CRITICAL: Metadata site key '{site_key}' missing or unconfigured for {identifier}.")

                html_content = raw_bytes.decode("utf-8", errors="ignore")
                soup = BeautifulSoup(html_content, "html.parser")
                content_div = soup.select_one(target_selector)

                if content_div:
                    # Extract, clean, and isolate strictly the target inner content body
                    inner_html = content_div.decode_contents().strip()
                    transformed_bytes = inner_html.encode("utf-8")
                else:
                    raise ValueError(f"CRITICAL: Dynamic selector '{target_selector}' not found in asset {identifier}.")

            elif ext == "pdf":
                content_type = "application/pdf"
            elif ext == "docx":
                content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

            # 5. Compute cryptographic signatures and construct normalized name layout
            new_file_hash = hashlib.sha256(transformed_bytes).hexdigest()
            silver_object_name = f"{identifier}.{ext}"

            # Stream clean file directly to the Silver zone bucket 
            minio_client.put_object(
                bucket_name=silver_bucket,
                object_name=silver_object_name,
                data=io.BytesIO(transformed_bytes),
                length=len(transformed_bytes),
                content_type=content_type
            )

            # 6. Format and upsert new metadata states to the distinct Silver collection
            silver_metadata = dict(record)
            silver_metadata.pop("_id", None)
            silver_metadata.update({
                "file_path": silver_object_name,
                "file_hash": new_file_hash,
                "transformed_at": datetime.now(timezone.utc).isoformat(),
                "transformation_status": "SUCCESS"
            })

            silver_coll.update_one(
                {"identifier": identifier},
                {"$set": silver_metadata},
                upsert=True
            )
            logger.info(f"Successfully migrated layout asset to Silver: {silver_object_name}")

        except Exception as e:
            logger.error(f"Asset transformation pipeline run suspended for {identifier}: {str(e)}", extra={"identifier": identifier, "step": "transformation"})

    logger.info(f"END: Transformation pipeline completed for window: {start_date} to {end_date}. Processed target records.", extra={"processed_count": len(latest_state_snapshots)})


if __name__ == "__main__":
    # Standalone execution setup via CLI arguments
    load_dotenv()

    parser = argparse.ArgumentParser(description="WRC Core Ingestion Transformation Utility")
    parser.add_argument("--start-date", required=True, help="Start partition timeline parameter (D/M/YYYY)")  
    parser.add_argument("--end-date", required=True, help="End partition timeline parameter (D/M/YYYY)")      
    args = parser.parse_args()

    # Map parameters smoothly from project environment variables
    env_params = {
        "MONGO_LANDING_COLLECTION": os.environ["MONGO_LANDING_COLLECTION"],            
        "MONGO_TRANSFORMED_COLLECTION": os.environ["MONGO_TRANSFORMED_COLLECTION"],    
        "MINIO_LANDING_BUCKET": os.environ["MINIO_LANDING_BUCKET"],              
        "MINIO_TRANSFORMED_BUCKET": os.environ["MINIO_TRANSFORMED_BUCKET"]       
    }

    # Initialize raw connection layer environments
    client = MongoClient(f"mongodb://{os.environ['MONGO_ROOT_USER']}:{os.environ['MONGO_ROOT_PASSWORD']}@{os.environ['MONGO_HOST']}:{os.environ['MONGO_PORT']}/")  
    db = client[os.environ["MONGO_DB_NAME"]]  

    minio = Minio(
        f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_API_PORT']}",  
        access_key=os.environ['MINIO_ROOT_USER'],                                
        secret_key=os.environ['MINIO_ROOT_PASSWORD'],                        
        secure=False  
    )

    execute_transformation(
        start_date=args.start_date,
        end_date=args.end_date,
        mongo_db=db,
        minio_client=minio,
        config_env=env_params
    )
    logger.info("Transformation asset execution pipeline finalized.")