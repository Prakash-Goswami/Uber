# 06. Data Warehousing, Semantic Layer & Analytical KPIs

## 1. Databricks Lakehouse Warehousing Best Practices

Our Gold layer (`uber_lakehouse.gold.*`) acts as a high-concurrency **Enterprise Data Warehouse** served directly via **Databricks SQL Serverless** without copying data out to an external proprietary warehouse:

1. **Liquid Clustering (`CLUSTER BY`)**:
   - `gold.fact_trips`: `CLUSTER BY (pickup_date_key, pickup_h3_r7)` — accelerates both date-range financial queries and geospatial H3 marketplace heatmaps.
   - `gold.fact_hex_supply_demand_5m`: `CLUSTER BY (window_start_ts, h3_r7)` — accelerates real-time surge & SLA monitoring.
2. **Primary Key & Foreign Key Informational Constraints (`RELY`)**:
   - Declared in `sql/01_ddl_gold_star_schema.sql` (`PRIMARY KEY (trip_id) RELY`, `FOREIGN KEY (pu_location_id) REFERENCES dim_zone(location_id)`).
   - Enables the **Databricks Photon / Catalyst Optimizer** to eliminate redundant dimension joins when dimension attributes are not projected!
3. **Rich Table & Column Metadata Comments for Databricks AI/BI Genie**:
   - Every table and key metric includes business definitions in `COMMENT` clauses so natural-language queries in **Databricks Genie** (*"What was Uber's take rate at JFK during rainy hours?"*) generate accurate SQL automatically.

---

## 2. Executive & Marketplace KPI Catalog (`sql/02_analytical_warehouse_queries.sql`)

| # | Analytical Domain | KPI / Query Name | Business Question Answered |
| :--- | :--- | :--- | :--- |
| **Q1** | Marketplace Economics | **Gross Bookings, Net Revenue & Take-Rate by Product** | Which product (`UberX`, `Comfort`, `UberX Share`, `UberWAV`) drives the highest volume vs. platform margin? |
| **Q2** | Geospatial Surge | **H3 Hexagon Supply-Demand Deficit & Surge Efficiency** | Which H3 Resolution-7 cells suffer chronic driver undersupply during AM/PM rush hours? |
| **Q3** | Weather Elasticity | **Adverse Weather Impact on ETA, Surge & Cancellations** | How much do rain/snow storms increase rider wait time, surge multipliers, and driver hourly earnings? |
| **Q4** | Airport Operations | **JFK / LGA / EWR Airport Funnel & Curb Wait SLA** | What is the p50/p90/p95 rider wait time and airport fee revenue across NYC's three major airports? |
| **Q5** | Shared Mobility | **UberX Share Match Rate & Unit Economics** | When riders request a shared trip (`shared_request_flag='Y'`), how often do we successfully match a co-rider (`shared_match_flag='Y'`) and how does margin shift? |
| **Q6** | Driver Retention | **Driver Tier Progression & Hourly Earnings Utilization** | Do `Diamond` and `Platinum` EV drivers earn significantly more per active hour than `Blue` tier drivers? |
| **Q7** | Rider Loyalty | **Uber One Membership Tip Behavior & Frequency** | How does `Uber_One` membership impact trip frequency, average fare, and tip generosity (using point-in-time SCD2 joins)? |
| **Q8** | Route Corridors | **Top 20 O-D (Origin-Destination) Zone Corridors** | Which neighborhood-to-neighborhood pairs represent the highest recurring revenue corridors? |
| **Q9** | Operational Quality | **Data Quality Quarantine Audit Summary** | What percentage of ingested trips failed each DQ rule (`DQ_01`..`DQ_07`) by dispatching base? |
| **Q10** | Time-Series Anomaly | **Hour-over-Hour Marketplace Demand Shock Detector** | SQL window functions (`LAG`, rolling Z-score) flagging sudden demand spikes (>2.5$\sigma$). |
