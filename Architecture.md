# System Architecture & Engineering Rationale

This pipeline implements a Medallion Architecture (Bronze/Silver) using containerized compute and decoupled storage. The design prioritizes scalability, data immutability, and strict operational idempotency.

---

## 1. Orchestration & Compute (Dagster Pipes)
**Design Choice**: Rather than running extraction scripts directly within the orchestrator's environment, this architecture utilizes **Dagster Pipes** to spin up ephemeral Docker containers (`wrc_worker_image:latest`) for distinct tasks.
**Valorization**: 
* **Dependency Isolation**: The Scrapy engine and transformation scripts have entirely separate lifecycles and dependency trees from the orchestrator.
* **Resource Optimization**: Compute resources are only consumed during active execution. Once a partition is scraped, the container is destroyed, preventing memory leaks common in long-running Python scrapers.
* **Horizontal Scalability**: This design allows a seamless future transition from local Docker daemons to Kubernetes orchestration without rewriting the core extraction logic.

## 2. Anti-Blocking & Evasion Strategy
**Design Choice**: To avoid detection and IP-based blocking by target web application firewalls, the Scrapy engine utilizes a custom `RandomHeaderMiddleware`.
**Valorization**: 
* **Header Randomization**: Before every request, the middleware dynamically injects a randomized `User-Agent` and `Accept-Language` header. This mimics legitimate browser traffic patterns and prevents rudimentary bot-detection algorithms from fingerprinting and blacklisting the scraper.
* **Throttling**: The architecture enforces a `DOWNLOAD_DELAY` via environment configuration, ensuring requests are sent at a polite cadence. This minimizes the risk of overloading the target server and significantly reduces the likelihood of triggering rate-limiting tripwires.

## 3. Developer Tooling: Mongo Express
**Design Choice**: Integration of `mongo-express` as a sidecar container in the `docker-compose` stack.
**Valorization**: Provides an intuitive, web-based UI to browse MongoDB collections, execute ad-hoc queries, and verify document structures without manual CLI interaction. Because scraping raw HTML often yields volatile schema structures during the initial Bronze layer ingestion, having an immediate visual feedback loop drastically accelerates pipeline debugging and data mapping validation.

## 4. Decoupled Storage Strategy
**Design Choice**: Utilizing MongoDB for metadata and MinIO (S3-compatible) for binary object storage.
**Valorization**: Relational databases degrade rapidly when storing large BLOBs (PDFs, heavy HTML). By decoupling the metadata (Mongo) from the binary payloads (MinIO), we ensure rapid querying of state and historical snapshots without incurring heavy I/O penalties. This precisely mirrors enterprise cloud environments (AWS S3 + DynamoDB).

## 5. Partitioning Strategy 
**Design Choice**: Data is logically chunked into overlapping time windows (configurable via `MAX_PARTITION_DAYS` in the `.env` file), injecting strict `start_date` and `end_date` bounds into the Scrapy engine dynamically.
**Valorization**: Time-based partitioning creates deterministic execution boundaries. Observing the target website's publication volume reveals approximately 300 legal records in a given 30-day window. Given this metric, a **7-day default partitioning window** chunks the workload into highly manageable batches (~70 records per run). If a specific week of data ingestion fails due to a network timeout, only that minimal partition needs to be re-run, preventing massive, monolithic "scrape everything" jobs.

---

## 6. Data Consistency & Hashing Strategy
**Design Choice**: Pre-processing raw HTML during ingestion to deliberately strip server-injected execution comments (e.g., `<!-- took 0.9 ms -->`).
**Valorization**: While strict Medallion Architecture dictates that the Bronze layer should be an entirely unmodified ledger, a deliberate design choice was made to strip specific HTML comments prior to hashing and storage. Because the target server dynamically injects rendering execution times into the HTML of every request, identical documents would generate entirely different SHA-256 hashes on subsequent downloads. Stripping these dynamic artifacts is required to maintain consistent hash values, which is the foundational mechanism for pipeline idempotency and deduplication.

---

## 7. Current Technical Debt & Remediation

### Anti-Pattern: Eager Downloading for Idempotency
* **The Issue**: To satisfy the "do not re-download unchanged files" rule, the pipeline currently downloads the file to compute a SHA-256 hash, checks it against MongoDB, and drops it if it exists. Downloading the payload just to verify if it needs to be downloaded is computationally wasteful.
* **The Solution**: Use the currently retreived `publish_date`, or in more advanced approuch, Transition to HTTP `HEAD` requests (Prior to yielding the binary stream request, the pipeline should issue a `HEAD` request to evaluate the `ETag`, `Content-Length`, or `Last-Modified` headers).  If these match our database records, we skip the `GET` request entirely, saving bandwidth.

---

## 7. Scaling to 50+ Sources (1000x Volume) 

This architecture was explicitly designed with multi-source scaling in mind. 
By abstracting the DOM selectors, base URLs, and parsing rules into configuration dictionaries (`site-config.py`), the Scrapy/Transformer pairing acts as a **universal engine**. Granted that newly onboarded sources share a relatively common site architecture (e.g., paginated list views, predictable HTML table structures, or standard REST API endpoints), scaling to new legal domains merely requires injecting a new dictionary mapping rather than engineering a new spider.

Meanwhile, volume scaling is inherently solved by the horizontal scaling principles already implemented on a small scale via Docker Compose. To push this architecture to handle 50+ global legal domains and exponential data growth, the following evolutions would be seamlessly integrated:

* **Distributed Compute Cluster**: Migrate `PipesDockerClient` to `PipesK8sClient`. The orchestrator will dynamically provision worker pods across a scalable Kubernetes cluster, allowing massive parallel scraping of distinct legal bodies simultaneously.
* **Dynamic Asset Partitions**: Transition Dagster's rigid time-window partitions to `DynamicPartitionsDefinition`. This allows the pipeline to dynamically track and schedule partitions by *Source Domain + Identifier*, rather than overarching time blocks.
* **Proxy Pools & IP Rotation**: Integrate residential proxy networks (e.g., Bright Data, Zyte) natively into Scrapy's middleware to distribute request origins, circumventing localized IP bans across diverse Web Application Firewalls.