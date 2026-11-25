-- =============================================================================
-- UBER LAKEHOUSE ON DATABRICKS — GOLD KIMBALL STAR SCHEMA DDL
-- =============================================================================
-- Reference DDL with Unity Catalog 3-level namespace, Liquid Clustering,
-- Informational PK/FK Optimizer Constraints (`RELY`), and AI/BI Genie Comments.
-- Note: Notebook `04_gold_dimensional_warehouse.py` materializes these tables
-- dynamically via PySpark; run this script anytime to inspect or pre-declare DDL.
-- =============================================================================

USE CATALOG uber_lakehouse;
CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- 1. Conformed Geospatial Zone Dimension
CREATE TABLE IF NOT EXISTS dim_zone (
    location_id INT NOT NULL COMMENT 'Official NYC TLC Taxi Zone ID (1..265)',
    borough STRING COMMENT 'NYC Borough (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)',
    zone_name STRING COMMENT 'Neighborhood or Airport name (e.g., Midtown Center, JFK Airport)',
    service_zone STRING COMMENT 'Regulatory service zone (Yellow Zone, Boro Zone, Airports, EWR)',
    centroid_lat DOUBLE COMMENT 'WGS84 latitude of zone centroid',
    centroid_lon DOUBLE COMMENT 'WGS84 longitude of zone centroid',
    centroid_h3_r7 STRING COMMENT 'Uber H3 Resolution-7 hexagon (~5.16 km2) of centroid',
    centroid_h3_r8 STRING COMMENT 'Uber H3 Resolution-8 hexagon (~0.74 km2) of centroid',
    is_airport BOOLEAN COMMENT 'True if JFK, LaGuardia (LGA), or Newark (EWR)',
    CONSTRAINT pk_dim_zone PRIMARY KEY (location_id) RELY
)
USING DELTA
CLUSTER BY (location_id)
COMMENT 'NYC 265 Taxi Zones enriched with WGS84 coordinates and Uber H3 hexagonal indices.';

-- 2. Conformed Calendar Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT NOT NULL COMMENT 'Surrogate integer key formatted as YYYYMMDD',
    full_date DATE NOT NULL COMMENT 'Calendar date',
    year INT COMMENT '4-digit calendar year',
    quarter INT COMMENT 'Calendar quarter (1..4)',
    month INT COMMENT 'Calendar month (1..12)',
    month_name STRING COMMENT 'Full month name',
    day_of_month INT COMMENT 'Day of month (1..31)',
    day_of_week INT COMMENT 'Spark day of week (1=Sun .. 7=Sat)',
    day_name STRING COMMENT 'Full weekday name',
    is_weekend BOOLEAN COMMENT 'True on Saturday and Sunday',
    is_us_holiday BOOLEAN COMMENT 'True on major US federal holidays',
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key) RELY
)
USING DELTA
CLUSTER BY (date_key)
COMMENT 'Enterprise calendar dimension for time-intelligence analytics.';

-- 3. Conformed Time-of-Day Dimension
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INT NOT NULL COMMENT 'Minute-of-day integer key formatted as HHMM (0..2359)',
    hour_24 INT COMMENT '24-hour clock hour (0..23)',
    minute INT COMMENT 'Minute within hour (0..59)',
    time_of_day_bucket STRING COMMENT 'Marketplace demand period (AM_Rush, Midday, PM_Rush, Evening, Late_Night, Early_Morning)',
    is_rush_hour BOOLEAN COMMENT 'True during AM (07:00-09:59) or PM (16:00-19:59) commute peaks',
    CONSTRAINT pk_dim_time PRIMARY KEY (time_key) RELY
)
USING DELTA
CLUSTER BY (time_key)
COMMENT 'Minute-grain time dimension with NYC commuter rush-hour buckets.';

-- 4. Conformed Hourly Weather Dimension
CREATE TABLE IF NOT EXISTS dim_weather (
    weather_hour_key STRING NOT NULL COMMENT 'Hourly UTC key formatted as YYYYMMDDHH',
    observation_hour_ts TIMESTAMP COMMENT 'Truncated UTC hour timestamp',
    temp_celsius DOUBLE COMMENT '2-meter air temperature in Celsius',
    precipitation_mm DOUBLE COMMENT 'Total hourly liquid precipitation equivalent in mm',
    rain_mm DOUBLE COMMENT 'Hourly rainfall in mm',
    snowfall_cm DOUBLE COMMENT 'Hourly snowfall in cm',
    wind_speed_kmh DOUBLE COMMENT '10-meter wind speed in km/h',
    weather_code INT COMMENT 'WMO meteorological condition code',
    weather_condition STRING COMMENT 'Derived bucket: Clear, Cloudy, Rain, Heavy_Rain, Snow, Storm',
    is_adverse_weather BOOLEAN COMMENT 'True during significant rain, snow, or high wind events',
    CONSTRAINT pk_dim_weather PRIMARY KEY (weather_hour_key) RELY
)
USING DELTA
CLUSTER BY (weather_hour_key)
COMMENT 'Hourly NYC weather observations from Open-Meteo Archive API.';

-- 5. SCD Type 2 Driver Dimension
CREATE TABLE IF NOT EXISTS dim_driver_scd2 (
    driver_sk BIGINT NOT NULL COMMENT 'Point-in-time surrogate key for SCD Type 2 version',
    driver_id STRING NOT NULL COMMENT 'Natural operational Driver ID (DRV-XXXXX)',
    home_borough STRING COMMENT 'Driver primary operating NYC borough',
    vehicle_type STRING COMMENT 'Assigned vehicle class (Sedan, SUV, EV_Sedan, EV_SUV, WAV)',
    driver_rating DOUBLE COMMENT 'Rolling 500-trip rider feedback rating (1.00 - 5.00)',
    driver_tier STRING COMMENT 'Uber Pro loyalty tier (Blue, Gold, Platinum, Diamond)',
    is_ev_vehicle BOOLEAN COMMENT 'True if Zero-Emission Electric Vehicle eligible for Green incentives',
    effective_start_ts TIMESTAMP COMMENT 'UTC timestamp when this dimension version became active',
    effective_end_ts TIMESTAMP COMMENT 'UTC timestamp when superseded (9999-12-31 23:59:59 if current)',
    is_current BOOLEAN COMMENT 'True for latest active driver profile version',
    CONSTRAINT pk_dim_driver_scd2 PRIMARY KEY (driver_sk) RELY
)
USING DELTA
CLUSTER BY (driver_id)
COMMENT 'SCD Type 2 Driver history tracking EV transitions, rating changes, and Uber Pro tier upgrades.';

-- 6. SCD Type 2 Rider Dimension
CREATE TABLE IF NOT EXISTS dim_rider_scd2 (
    rider_sk BIGINT NOT NULL COMMENT 'Point-in-time surrogate key for SCD Type 2 version',
    rider_id STRING NOT NULL COMMENT 'Natural operational Rider ID (RDR-XXXXXX)',
    membership_status STRING COMMENT 'Loyalty subscription: None, Uber_One_Monthly, Uber_One_Annual',
    rider_rating DOUBLE COMMENT 'Rider rating given by drivers (1.00 - 5.00)',
    preferred_payment STRING COMMENT 'Default payment instrument (Card, ApplePay, UberCash, Corporate)',
    effective_start_ts TIMESTAMP COMMENT 'UTC timestamp when this version became active',
    effective_end_ts TIMESTAMP COMMENT 'UTC timestamp when superseded',
    is_current BOOLEAN COMMENT 'True for latest active rider profile version',
    CONSTRAINT pk_dim_rider_scd2 PRIMARY KEY (rider_sk) RELY
)
USING DELTA
CLUSTER BY (rider_id)
COMMENT 'SCD Type 2 Rider history tracking Uber One membership upgrades.';

-- 7. Core Transaction & Milestone Fact Table: fact_trips
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id STRING NOT NULL COMMENT 'Deterministic SHA-256 trip primary key',
    driver_sk BIGINT COMMENT 'Point-in-time FK to dim_driver_scd2',
    rider_sk BIGINT COMMENT 'Point-in-time FK to dim_rider_scd2',
    driver_id STRING COMMENT 'Degenerate operational Driver ID',
    rider_id STRING COMMENT 'Degenerate operational Rider ID',
    pickup_date DATE COMMENT 'Calendar date of trip pickup',
    pickup_date_key INT COMMENT 'FK to dim_date (YYYYMMDD)',
    pickup_time_key INT COMMENT 'FK to dim_time (HHMM)',
    pu_location_id INT COMMENT 'FK to dim_zone for Pickup Location',
    do_location_id INT COMMENT 'FK to dim_zone for Dropoff Location',
    weather_hour_key STRING COMMENT 'FK to dim_weather (YYYYMMDDHH)',
    pickup_h3_r7 STRING COMMENT 'Pickup Uber H3 Resolution-7 cell (~5.16 km2)',
    pickup_h3_r8 STRING COMMENT 'Pickup Uber H3 Resolution-8 cell (~0.74 km2)',
    dropoff_h3_r7 STRING COMMENT 'Dropoff Uber H3 Resolution-7 cell',
    dropoff_h3_r8 STRING COMMENT 'Dropoff Uber H3 Resolution-8 cell',
    pickup_lat DOUBLE COMMENT 'Pickup WGS84 latitude',
    pickup_lon DOUBLE COMMENT 'Pickup WGS84 longitude',
    dropoff_lat DOUBLE COMMENT 'Dropoff WGS84 latitude',
    dropoff_lon DOUBLE COMMENT 'Dropoff WGS84 longitude',
    product_type STRING COMMENT 'Uber product: UberX, Uber Comfort, UberX Share, Uber Black/XL, UberWAV',
    request_ts TIMESTAMP COMMENT 'Timestamp when rider tapped Request Ride',
    on_scene_ts TIMESTAMP COMMENT 'Timestamp when driver arrived at pickup curb',
    pickup_ts TIMESTAMP COMMENT 'Timestamp when trip started',
    dropoff_ts TIMESTAMP COMMENT 'Timestamp when trip completed',
    wait_time_sec DOUBLE COMMENT 'Total rider wait seconds (pickup_ts - request_ts)',
    driver_arrival_sec DOUBLE COMMENT 'Driver dispatch-to-curb seconds (on_scene_ts - request_ts)',
    curb_wait_sec DOUBLE COMMENT 'Driver waiting at curb seconds (pickup_ts - on_scene_ts)',
    trip_duration_sec DOUBLE COMMENT 'In-vehicle travel duration seconds (dropoff_ts - pickup_ts)',
    trip_miles DOUBLE COMMENT 'Actual driven distance in miles',
    haversine_km DOUBLE COMMENT 'Great-circle straight-line distance in kilometers',
    avg_speed_mph DOUBLE COMMENT 'Average trip speed in miles per hour',
    surge_multiplier DOUBLE COMMENT 'Implied dynamic surge pricing multiplier (1.0x - 4.0x)',
    base_passenger_fare DOUBLE COMMENT 'Rider base time-and-distance fare in USD (excl. tolls/taxes/tips)',
    tolls_amount DOUBLE COMMENT 'Bridge and tunnel tolls in USD',
    congestion_surcharge DOUBLE COMMENT 'NYC Manhattan congestion surcharge in USD',
    airport_fee DOUBLE COMMENT 'NYC/NJ Airport access fee in USD',
    tips_amount DOUBLE COMMENT 'Rider tip paid 100% to driver in USD',
    taxes_and_fees DOUBLE COMMENT 'Sales tax + Black Car Fund regulatory fees in USD',
    total_rider_amount DOUBLE COMMENT 'Total Gross Bookings charged to rider in USD',
    driver_pay_amount DOUBLE COMMENT 'Base net payout to driver in USD (excl. tips)',
    uber_gross_margin DOUBLE COMMENT 'Uber platform gross margin in USD (base_passenger_fare - driver_pay_amount)',
    take_rate_pct DOUBLE COMMENT 'Platform take-rate percentage (uber_gross_margin / base_passenger_fare * 100)',
    is_airport_trip BOOLEAN COMMENT 'True if pickup or dropoff is JFK, LGA, or EWR',
    is_shared_trip BOOLEAN COMMENT 'True if rider requested UberX Share',
    is_shared_matched BOOLEAN COMMENT 'True if UberX Share trip successfully matched another co-rider',
    CONSTRAINT pk_fact_trips PRIMARY KEY (trip_id) RELY,
    CONSTRAINT fk_trips_pu_zone FOREIGN KEY (pu_location_id) REFERENCES dim_zone(location_id),
    CONSTRAINT fk_trips_do_zone FOREIGN KEY (do_location_id) REFERENCES dim_zone(location_id),
    CONSTRAINT fk_trips_date FOREIGN KEY (pickup_date_key) REFERENCES dim_date(date_key)
)
USING DELTA
CLUSTER BY (pickup_date_key, pickup_h3_r7)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Core Uber Trip Fact Table at single-trip grain, Liquid Clustered by date and H3 hexagon.';
