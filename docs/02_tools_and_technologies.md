# 02. Tools & Technologies Stack

## 1. Complete Technology Matrix

Every tool in this project is selected to mirror how modern platform teams build Uber-scale Lakehouse systems on Databricks using Python and PySpark.

| Domain | Technology Used | Version / Runtime | Why It Is Used |
| :--- | :--- | :--- | :--- |
| **Compute Engine** | **Apache Spark (PySpark)** | Spark 3.5+ (DBR 14.3 LTS / 15.x / Serverless) | Distributed batch & stream processing, Catalyst query optimizer, Photon vectorized execution |
| **Storage Format** | **Delta Lake** | Delta 3.x | ACID transactions, schema enforcement/evolution, `MERGE` upserts, Time Travel, Liquid Clustering, Change Data Feed |
| **Data Governance** | **Databricks Unity Catalog** | 3-level namespace (`uber_lakehouse.*.*`) | Fine-grained access control, UC Volumes for unstructured/raw landing files, automated table & column lineage |
| **Streaming Ingestion** | **Spark Structured Streaming & Auto Loader (`cloudFiles`)** | Built-in DBR | Incremental file discovery without directory listing bottlenecks, schema checkpointing, `_rescued_data` quarantine |
| **Declarative ETL** | **Lakeflow Declarative Pipelines (Delta Live Tables / DLT)** | Built-in DBR | Declarative `@dlt.table` and `@dlt.expect` data quality enforcement with automatic lineage and retry |
| **Geospatial Engine** | **Uber H3** (`pyspark.databricks.sql.functions.h3_*` / `h3-py` / Fallback) | H3 v4 / Built-in Photon H3 | Hexagonal spatial indexing at Resolution 7 & 8, k-ring spatial smoothing, Haversine distance validation |
| **Data Warehousing** | **Databricks SQL (Serverless)** | Photon SQL Warehouse | Kimball Star Schema analytics, `CLUSTER BY` (Liquid Clustering), PK/FK informational constraints for query optimization |
| **Machine Learning** | **PySpark MLlib (`pyspark.ml`) + MLflow** | MLflow 2.12+ | Distributed feature pipelines (`StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `GBTRegressor`, `GBTClassifier`), experiment tracking, model registry |
| **CI/CD & IaC** | **Databricks Asset Bundles (DABs)** | `databricks.yml` CLI v0.218+ | Infrastructure-as-Code deployment of Notebooks, Jobs/Workflows, and DLT Pipelines across `dev` and `prod` |
| **Public Data APIs** | **NYC TLC CloudFront S3** + **Open-Meteo API** | Parquet (PyArrow) + REST JSON | Real-world Uber (`HV0003`) trip records, 265 NYC Taxi Zones, and real hourly weather without API keys |

---

## 2. Public Free Datasets Used (Zero API Keys Required)

### 2.1 NYC TLC High-Volume For-Hire Vehicle (HVFHV) Trip Records
- **Source URL**: `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet`
- **Why this dataset is gold for an Uber project**:
  - `hvfhs_license_num = 'HV0003'` is **literally Uber** (regulated by the NYC Taxi & Limousine Commission; `'HV0005'` is Lyft, `'HV0004'` is Via).
  - Contains **four granular marketplace timestamps** per trip:
    1. `request_datetime` — Rider taps "Request Uber"
    2. `on_scene_datetime` — Driver arrives at pickup curb
    3. `pickup_datetime` — Rider enters vehicle & trip begins
    4. `dropoff_datetime` — Trip completes
  - Contains **full two-sided marketplace economics**:
    - Rider side: `base_passenger_fare`, `tolls`, `bcf` (Black Car Fund), `sales_tax`, `congestion_surcharge`, `airport_fee`, `tips`
    - Driver side: `driver_pay` (enabling exact **Uber Platform Net Revenue / Take-Rate** calculation!)
  - Contains **product-type flags**: `shared_request_flag`, `shared_match_flag` (UberX Share), `wav_request_flag`, `wav_match_flag` (UberWAV).

### 2.2 NYC Taxi Zone Reference Dataset
- **Source URL**: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Maps `LocationID` (`1` to `265`) to `Borough` (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR), `Zone` (e.g., *JFK Airport*, *Midtown Center*, *Times Sq/Theatre District*), and `service_zone`.
- Enriched in `src/uber_platform/geospatial.py` with accurate **WGS84 `(latitude, longitude)` zone centroids** + realistic street-level jitter so every trip maps cleanly to **Uber H3 cells**.

### 2.3 Open-Meteo Historical Weather Archive API
- **Source URL**: `https://archive-api.open-meteo.com/v1/archive`
- Fetches real hourly NYC (`40.7829° N, -73.9654° W`) observations for January 2024: `temperature_2m`, `precipitation`, `rain`, `snowfall`, `wind_speed_10m`, and WMO `weather_code`.
- Used in both **Gold Star Schema (`dim_weather`)** and **ML Surge / ETA models** (rain and snow dramatically spike Uber surge multipliers and slow down trip speeds).

---

## 3. Databricks Compute & Optimization Features Leveraged

1. **Liquid Clustering (`CLUSTER BY (pickup_date, pickup_h3_r7)`)**:
   - Replaces rigid Hive-style `PARTITIONED BY` + `ZORDER BY`, avoiding the small-file problem on streaming appends while giving sub-second data skipping on both temporal and geospatial filters.
2. **Delta Change Data Feed (`delta.enableChangeDataFeed = true`)**:
   - Enabled on Silver and Gold tables so downstream consumers (feature tables, incremental marts) only process changed rows (`_change_type IN ('insert', 'update_postimage')`).
3. **Auto Optimize (`delta.autoOptimize.optimizeWrite = true`, `autoCompact = true`)**:
   - Automatically coalesces small streaming micro-batch files into optimal 128 MB–256 MB Parquet files inside Delta tables.
4. **`trigger(availableNow=True)` for Cost-Efficient Streaming Tests**:
   - Processes all newly landed streaming files in bounded micro-batches with full Spark Structured Streaming checkpointing (`checkpointLocation`), then cleanly shuts down the stream so you never burn idle cluster credits while testing on Databricks! (Can be switched to `trigger(processingTime="10 seconds")` via a single config flag for 24/7 continuous mode.)
