# 05. Data Transformations, Business Rules & Data Quality Engineering

## 1. Medallion Transformation Pipeline Overview

Each Medallion hop has strict responsibilities, contracts, and SLAs:

| Layer | Input | Transformations Applied | Output Tables |
| :--- | :--- | :--- | :--- |
| **Bronze** | Parquet (`fhvhv`), CSV (`taxi_zones`), JSON (`weather`, `gps_pings`, `trip_events`, `cdc`) | Audit columns (`_ingest_ts`, `_source_file`, `_batch_id`), zero destructive filtering, `_rescued_data` capture | `bronze.fhvhv_trips_raw`, `bronze.taxi_zones_raw`, `bronze.hourly_weather_raw`, `bronze.driver_pings_stream`, `bronze.trip_lifecycle_stream`, `bronze.driver_cdc_raw` |
| **Silver** | Bronze Delta tables | Filter to Uber (`HV0003`), deterministic `trip_id` hashing, DQ validation & quarantine split, NYC zone coordinate mapping, **Uber H3 Res 7 & 8 indexing**, Marketplace SLA metrics, SCD Type 2 `MERGE` | `silver.trips_enriched`, `silver.trips_quarantine`, `silver.dim_driver_scd2`, `silver.dim_rider_scd2`, `silver.weather_hourly`, `silver.hex_telemetry_5m` |
| **Gold** | Silver Delta tables | Kimball surrogate key lookups, point-in-time SCD2 joins, 5-minute H3 demand/supply ratio & dynamic surge pricing curves, daily driver earnings rollups | `gold.fact_trips`, `gold.fact_hex_supply_demand_5m`, `gold.fact_driver_daily_summary`, `gold.dim_zone`, `gold.dim_date`, `gold.dim_time`, `gold.dim_weather` |

---

## 2. Core Uber Business Logic & Derived Metrics

### 2.1 Deterministic Idempotent `trip_id`
Public NYC TLC HVFHV files do not expose Uber's internal UUID. To make `MERGE INTO` 100% deterministic and idempotent across re-runs:
```sql
sha2(concat_ws('||', hvfhs_license_num, dispatching_base_num,
               cast(pickup_datetime as string), cast(dropoff_datetime as string),
               cast(PULocationID as string), cast(DOLocationID as string),
               cast(trip_miles as string), cast(base_passenger_fare as string)), 256)
```

### 2.2 Uber Product Classification Logic
Derived from NYC TLC regulatory flags and fare structure:
- `wav_request_flag = 'Y'` $\rightarrow$ **`UberWAV`** (Wheelchair Accessible)
- `shared_request_flag = 'Y'` $\rightarrow$ **`UberX Share`**
- `base_passenger_fare / max(trip_miles, 0.5) >= 8.5` and `trip_miles >= 2.0` $\rightarrow$ **`Uber Black /XL`**
- `base_passenger_fare / max(trip_miles, 0.5) >= 5.5` $\rightarrow$ **`Uber Comfort`**
- Otherwise $\rightarrow$ **`UberX`**

### 2.3 Two-Sided Marketplace Financial Equations
- **Gross Rider Bookings (`total_rider_amount`)**:
  $$\text{Total Rider Amount} = \text{base\_passenger\_fare} + \text{tolls} + \text{bcf} + \text{sales\_tax} + \text{congestion\_surcharge} + \text{airport\_fee} + \text{tips}$$
- **Pass-Through Regulatory & Tip Items**:
  $$\text{Pass-Through} = \text{tolls} + \text{bcf} + \text{sales\_tax} + \text{congestion\_surcharge} + \text{airport\_fee} + \text{tips}$$
- **Uber Platform Net Margin (`uber_gross_margin`)**:
  $$\text{Uber Margin} = \text{base\_passenger\_fare} - (\text{driver\_pay} - \text{tips}^*)$$
  *(Note: NYC TLC `driver_pay` excludes `tips`, so `base_passenger_fare - driver_pay` directly measures platform margin net of driver incentives! When driver incentives exceed rider base fare during cold-start supply boosts, margin can legitimately be negative—a real phenomenon in rideshare economics!)*
- **Marketplace Surge Multiplier Estimation (`surge_multiplier`)**:
  Computed against the expected baseline NYC TLC minimum time-and-distance fare formula:
  $$\text{Expected Base Fare} = \max\left(8.00,\; 2.50 + 1.75 \times \text{trip\_miles} + 0.58 \times \frac{\text{trip\_duration\_sec}}{60}\right)$$
  $$\text{Implied Surge Multiplier} = \text{clip}\left(\frac{\text{base\_passenger\_fare}}{\text{Expected Base Fare}},\; 1.0,\; 4.0\right)$$

---

## 3. Data Quality (DQ) Quarantine & Lakeflow / DLT Expectations

Rather than silently dropping bad rows (`WHERE trip_miles > 0`)—which hides upstream mobile SDK regressions—we implement the **Quarantine / Dead-Letter Table Pattern**:

| Rule ID | Data Quality Rule | Threshold / Expression | Action on Failure |
| :--- | :--- | :--- | :--- |
| `DQ_01` | Valid Uber License | `hvfhs_license_num IS NOT NULL` | Route to `silver.trips_quarantine` |
| `DQ_02` | Chronological Timestamps | `dropoff_datetime > pickup_datetime` | Route to `silver.trips_quarantine` |
| `DQ_03` | Realistic Trip Distance | `trip_miles > 0.05 AND trip_miles <= 250.0` | Route to `silver.trips_quarantine` |
| `DQ_04` | Realistic Trip Duration | `trip_time >= 30 AND trip_time <= 21600` (30s to 6h) | Route to `silver.trips_quarantine` |
| `DQ_05` | Non-Negative Fare | `base_passenger_fare >= 0.0 AND driver_pay >= 0.0` | Route to `silver.trips_quarantine` |
| `DQ_06` | Physically Plausible Speed | `(trip_miles / (trip_time / 3600.0)) <= 110.0` mph | Route to `silver.trips_quarantine` |
| `DQ_07` | Valid NYC Location IDs | `PULocationID BETWEEN 1 AND 265 AND DOLocationID BETWEEN 1 AND 265` | Route to `silver.trips_quarantine` |

Every quarantined record preserves the full raw payload **plus** an array column `dq_failed_rules` (e.g., `["DQ_03_INVALID_MILES", "DQ_06_IMPOSSIBLE_SPEED"]`) so Data Engineering on-call teams can query root causes immediately.

In `pipelines/dlt_uber_medallion_pipeline.py`, these exact same rules are also expressed declaratively via `@dlt.expect_all_or_drop` and quarantine views for **Databricks Lakeflow / Delta Live Tables**!
