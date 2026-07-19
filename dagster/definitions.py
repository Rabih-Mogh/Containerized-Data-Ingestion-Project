import os
import docker
import dagster as dg
from dagster_docker import PipesDockerClient

_WORKER_ENV_KEYS = [
    "MONGO_HOST", "MONGO_PORT", "MONGO_ROOT_USER", "MONGO_ROOT_PASSWORD",
    "MONGO_DB_NAME", "MONGO_LANDING_COLLECTION", "MONGO_TRANSFORMED_COLLECTION", "MONGO_LOGS_COLLECTION",
    "MINIO_HOST", "MINIO_API_PORT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
    "MINIO_LANDING_BUCKET", "MINIO_TRANSFORMED_BUCKET",
]

# Pull Docker configs from env
WORKER_IMAGE_NAME = os.environ["WORKER_IMAGE_NAME"]
WORKER_NETWORK_NAME = os.environ["WORKER_NETWORK_NAME"]

def _worker_env() -> dict:
    return {k: os.environ.get(k, "") for k in _WORKER_ENV_KEYS}

class IngestionConfig(dg.Config):
    # START_DATE, END_DATE, and PARTITION_LEN are removed.
    # Concurrency is now handled natively by Dagster's run queue.
    target_body_id: str = "1,2,3,15376"

# ------------------------------------------------------------------
# Configuration & Partitions
# ------------------------------------------------------------------

# 1. READ CONFIG FROM ENV
# We read these at startup. To "change" them from the UI/Host, 
# simply update your environment variables and restart the Dagster service.
HISTORICAL_PARTITION_START_DATE = os.environ["HISTORICAL_PARTITION_START_DATE"]
MAX_PARTITION_DAYS = int(os.environ["MAX_PARTITION_DAYS"])

# 2. DEFINE DYNAMIC PARTITIONS
# We use TimeWindowPartitionsDefinition with a custom timedelta for the "Partition Length"
wrc_partitions_def = dg.TimeWindowPartitionsDefinition(
    cron_schedule=f"0 0 */{MAX_PARTITION_DAYS} * *", # This approximates the interval
    start=HISTORICAL_PARTITION_START_DATE,
    fmt="%Y-%m-%d"
)

# ------------------------------------------------------------------
# Assets
# ------------------------------------------------------------------

@dg.asset(
    partitions_def=wrc_partitions_def,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=10)
)
def data_plane_ingestion(
    context: dg.AssetExecutionContext,
    config: IngestionConfig,
    docker_pipes: PipesDockerClient,
):
    """
    Spins up an ephemeral Docker container to handle data ingestion for a single partition.
    """    
    docker_client = docker.from_env()
    
    # Extract the precise start and end dates for this specific partition
    time_window = context.partition_time_window
    sub_start = time_window.start.strftime("%d/%m/%Y")
    sub_end = time_window.end.strftime("%d/%m/%Y")

    context.log.info(f"Initiating extraction for window: {sub_start} to {sub_end}")

    command = [
        "/bin/sh",
        "-c",
        f"cd /opt/data_worker/scraper && scrapy crawl legal_records_spider "
        f"-a site=workplacerelations "
        f"-a body='{config.target_body_id}' "
        f"-a from_date='{sub_start}' "
        f"-a to_date='{sub_end}'"
    ]

    # Use the partition_key (e.g., '2026-07-01') to uniquely label this container
    safe_partition_key = context.partition_key.replace("-", "")
    unique_run_label = f"dataworker_ing_{context.run_id}_{safe_partition_key}"

    try:
        docker_pipes.run(
            context=context,
            image=WORKER_IMAGE_NAME,
            command=command,
            container_kwargs={
                "name": unique_run_label,  # <-- Added custom container name here
                "network": WORKER_NETWORK_NAME,
                "environment": _worker_env(),
                "labels": {"wrc_teardown_target": unique_run_label}
            }
        )
    finally:
        context.log.info(f"Executing teardown for container labeled: {unique_run_label}")
        try:
            containers = docker_client.containers.list(
                all=True, 
                filters={"label": f"wrc_teardown_target={unique_run_label}"}
            )
            for c in containers:
                c.remove(force=True, v=True)
                context.log.info(f"Successfully removed ephemeral container: {c.id[:10]}")
        except Exception as e:
            context.log.warning(f"Failed to remove container during teardown: {e}")

    return dg.MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "partition_start": sub_start,
            "partition_end": sub_end,
            "target_body_id": config.target_body_id
        }
    )


@dg.asset(
    deps=[data_plane_ingestion],
    partitions_def=wrc_partitions_def
)
def data_plane_transformation(
    context: dg.AssetExecutionContext,
    config: IngestionConfig,
    docker_pipes: PipesDockerClient,
):
    """
    Spins up an ephemeral Docker container to handle the Silver layer transformation for a single partition.
    """
    docker_client = docker.from_env()

    time_window = context.partition_time_window
    sub_start = time_window.start.strftime("%d/%m/%Y")
    sub_end = time_window.end.strftime("%d/%m/%Y")

    context.log.info(f"Initiating transformation for window: {sub_start} to {sub_end}")

    command = [
        "/bin/sh",
        "-c",
        f"python /opt/data_worker/transforms.py "
        f"--start-date '{sub_start}' "
        f"--end-date '{sub_end}'"
    ]

    safe_partition_key = context.partition_key.replace("-", "")
    unique_run_label = f"dataworker_tfrm_{context.run_id}_{safe_partition_key}"

    try:
        docker_pipes.run(
            context=context,
            image=WORKER_IMAGE_NAME, 
            command=command,
            container_kwargs={
                "name": unique_run_label,  # <-- Added custom container name here
                "network": WORKER_NETWORK_NAME, 
                "environment": _worker_env(),
                "labels": {"wrc_teardown_target": unique_run_label}
            }
        )
    finally:
        context.log.info(f"Executing teardown for container labeled: {unique_run_label}")
        try:
            containers = docker_client.containers.list(
                all=True, 
                filters={"label": f"wrc_teardown_target={unique_run_label}"}
            )
            for c in containers:
                c.remove(force=True, v=True)
                context.log.info(f"Successfully removed ephemeral container: {c.id[:10]}")
        except Exception as e:
            context.log.warning(f"Failed to remove container during teardown: {e}")

    return dg.MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "partition_start": sub_start,
            "partition_end": sub_end,
        }
    )

# ------------------------------------------------------------------
# Jobs & Definitions Registry
# ------------------------------------------------------------------

wrc_pipeline_job = dg.define_asset_job(
    name="wrc_ingestion_and_transformation_job",
    selection=["data_plane_ingestion", "data_plane_transformation"],
    partitions_def=wrc_partitions_def # The job inherits the partition mapping
)

defs = dg.Definitions(
    assets=[data_plane_ingestion, data_plane_transformation],
    jobs=[wrc_pipeline_job],
    resources={
        "docker_pipes": PipesDockerClient()
    }
)