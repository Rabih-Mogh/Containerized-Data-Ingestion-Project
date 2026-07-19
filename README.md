# Legal Text Scraping Pipeline

An enterprise-grade, containerized data ingestion and transformation pipeline designed to scrape, clean, and store legal texts from the Workplace Relations Commission (WRC).

---

## Tech Stack
* **Orchestration**: Dagster (PostgreSQL-backed)
* **Compute Layer**: Docker & Dagster Pipes (Ephemeral Containers)
* **Extraction Engine**: Scrapy & BeautifulSoup4
* **Storage Layer**: MongoDB (Metadata/State) & MinIO (Object Storage)
* **Database UI**: Mongo Express

---

## Prerequisites
* Docker & Docker Compose
* Python 3.11+ (for local debugging and environment management)

---

## Environment Configuration

Create a `.env` file in the root directory of the project. The pipeline strictly enforces the presence of these configurations and will fail fast if they are omitted. 

### .env example (usable)

    # MongoDB 
    MONGO_HOST=localhost
    MONGO_PORT=27017
    MONGO_ROOT_USER=admin
    MONGO_ROOT_PASSWORD=changeit
    MONGO_UI_USER=admin
    MONGO_UI_PASSWORD=adminpassword
    MONGO_DB_NAME=wrc_scraper
    MONGO_LANDING_COLLECTION=records_bronze 
    MONGO_TRANSFORMED_COLLECTION=records_silver
    MONGO_LOGS_COLLECTION=app_logs

    # MinIO
    MINIO_HOST=localhost
    MINIO_API_PORT=9000
    MINIO_CONSOLE_PORT=9001
    MINIO_ROOT_USER=rabih
    MINIO_ROOT_PASSWORD=rabih12345
    MINIO_LANDING_BUCKET=legal-records-bronze
    MINIO_TRANSFORMED_BUCKET=legal-records-silver

    # Dagster Storage & Webserver
    DAGSTER_POSTGRES_USER=dagster
    DAGSTER_POSTGRES_PASSWORD=dagster_password
    DAGSTER_POSTGRES_DB=dagster
    DAGSTER_WEBSERVER_PORT=3000

    # Dagster Partitioning Configuration
    HISTORICAL_PARTITION_START_DATE=2026-07-01
    MAX_PARTITION_DAYS=7

    # Docker Infrastructure Configuration
    WORKER_IMAGE_NAME=wrc_worker_image:latest
    WORKER_NETWORK_NAME=wrc_network

    # Scrapy Defaults & Throttling
    DEFAULT_SITE_KEY=workplacerelations
    DEFAULT_TARGET_BODY_ID="1,2,3,15376"
    SCRAPY_DOWNLOAD_DELAY=1 # polite :)
    SCRAPY_CONCURRENCY_PER_DOMAIN=1

---

| Variable | Purpose |
| :--- | :--- |
| `MONGO_*` | MongoDB instance configuration, credentials, and collection naming. |
| `MINIO_*` | MinIO object storage connection details and bucket naming. |
| `DAGSTER_POSTGRES_*` | Database configuration for Dagster's metadata storage. |
| `DAGSTER_WEBSERVER_PORT` | The local port mapped for the Dagster UI. |
| `HISTORICAL_PARTITION_START_DATE` | The starting date for data ingestion partitions. |
| `MAX_PARTITION_DAYS` | Defines the size of the temporal chunking window. |
| `WORKER_IMAGE_NAME` | The Docker tag for ephemeral worker containers. |
| `WORKER_NETWORK_NAME` | The internal bridge network used by the containers. |
| `DEFAULT_SITE_KEY` | Identifier for the active scraper configuration. |
| `DEFAULT_TARGET_BODY_ID` | Internal IDs for specific site sections to scrape. |
| `SCRAPY_DOWNLOAD_DELAY` | Seconds to wait between requests to ensure polite scraping. |
| `SCRAPY_CONCURRENCY_PER_DOMAIN` | Limit on concurrent requests per domain. |

## Setup & Execution

### **1. Build the Worker Image**
The orchestration layer relies on Dagster Pipes to spin up ephemeral worker containers. You must build this image locally before launching the pipeline:

    docker build -t wrc_worker_image:latest -f Dockerfile.worker . 
    
make sure the image's name in the above command match **WORKER_IMAGE_NAME** in .env

### **2. Launch the Infrastructure**
Spin up the decoupled storage, orchestration, and developer tooling:

    docker-compose up -d --build

### **3. Accessing Services**
* **Dagster UI**: Navigate to `http://localhost:3000`.
* **Mongo Express**: Navigate to `http://localhost:8081` to view and manage your MongoDB collections.

### **4. Trigger the Orchestrator**
* Navigate to `http://localhost:3000`.
* Go to **Overview > Jobs** and select `wrc_ingestion_and_transformation_job`.
* Click the **Materialize all** button (or edit the run's config via **Open Launchpad**), select your target Date Partition, and execute.
* Dagster will orchestrate in parallel the Bronze landing extraction via Scrapy and seamlessly trigger the Silver transformation script upon completion for each partition independently.