-- =============================================================================
-- UBER LAKEHOUSE ON DATABRICKS — 10 EXECUTIVE & MARKETPLACE SQL ANALYTICS QUERIES
-- =============================================================================
-- Run directly in Databricks SQL Editor or attach to an AI/BI Dashboard.
-- =============================================================================

USE CATALOG uber_lakehouse;
USE SCHEMA gold;

-- -----------------------------------------------------------------------------
-- Q1. Two-Sided Marketplace Unit Economics & Take-Rate by Uber Product
-- -----------------------------------------------------------------------------
SELECT
    product_type,
    COUNT(*)                                                AS completed_trips,
    ROUND(SUM(total_rider_amount), 2)                       AS gross_bookings_usd,
    ROUND(SUM(base_passenger_fare), 2)                      AS net_fare_revenue_usd,
    ROUND(SUM(driver_pay_amount + tips_amount), 2)          AS total_driver_takehome_usd,
    ROUND(SUM(uber_gross_margin), 2)                        AS uber_platform_margin_usd,
    ROUND(AVG(take_rate_pct), 2)                            AS avg_take_rate_pct,
    ROUND(AVG(surge_multiplier), 2)                         AS avg_surge_multiplier,
    ROUND(AVG(wait_time_sec) / 60.0, 2)                     AS avg_rider_wait_min
FROM fact_trips
GROUP BY product_type
ORDER BY gross_bookings_usd DESC;


-- -----------------------------------------------------------------------------
-- Q2. Geospatial H3 Hexagon Supply-Demand Deficit & Surge Hotspots
-- -----------------------------------------------------------------------------
SELECT
    h.h3_r7,
    COALESCE(z.borough, 'NYC Metro')                        AS primary_borough,
    COALESCE(z.zone_name, 'Multi-Zone Hex')                 AS representative_zone,
    COUNT(*)                                                AS observed_5m_windows,
    SUM(h.ride_requests_demand)                             AS total_ride_requests,
    ROUND(AVG(h.active_drivers_supply), 1)                  AS avg_available_drivers,
    ROUND(AVG(h.demand_supply_ratio), 2)                    AS avg_demand_supply_ratio,
    ROUND(AVG(h.recommended_surge_multiplier), 2)           AS avg_surge_multiplier,
    SUM(h.unfulfilled_requests)                             AS cancelled_or_unfulfilled
FROM fact_hex_supply_demand_5m h
LEFT JOIN dim_zone z
    ON h.h3_r7 = z.centroid_h3_r7
GROUP BY h.h3_r7, COALESCE(z.borough, 'NYC Metro'), COALESCE(z.zone_name, 'Multi-Zone Hex')
HAVING SUM(h.ride_requests_demand) >= 10
ORDER BY avg_demand_supply_ratio DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- Q3. Weather Elasticity: How Rain & Snow Impact Surge, Speed & Wait Time
-- -----------------------------------------------------------------------------
SELECT
    w.weather_condition,
    w.is_adverse_weather,
    COUNT(f.trip_id)                                        AS total_trips,
    ROUND(AVG(f.surge_multiplier), 3)                       AS avg_surge_multiplier,
    ROUND(AVG(f.wait_time_sec) / 60.0, 2)                   AS avg_pickup_wait_min,
    ROUND(AVG(f.avg_speed_mph), 2)                          AS avg_traffic_speed_mph,
    ROUND(AVG(f.total_rider_amount), 2)                     AS avg_gross_fare_usd,
    ROUND(AVG(f.tips_amount), 2)                            AS avg_tip_usd
FROM fact_trips f
JOIN dim_weather w
    ON f.weather_hour_key = w.weather_hour_key
GROUP BY w.weather_condition, w.is_adverse_weather
ORDER BY avg_surge_multiplier DESC;


-- -----------------------------------------------------------------------------
-- Q4. NYC Tri-Airport Funnel (JFK, LaGuardia, Newark) — SLA & Fee Economics
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN pu.is_airport THEN CONCAT('Pickup @ ', pu.zone_name)
        WHEN do_z.is_airport THEN CONCAT('Dropoff @ ', do_z.zone_name)
    END                                                     AS airport_direction,
    COUNT(*)                                                AS trip_volume,
    ROUND(PERCENTILE_APPROX(f.wait_time_sec, 0.50) / 60.0, 2) AS p50_wait_min,
    ROUND(PERCENTILE_APPROX(f.wait_time_sec, 0.90) / 60.0, 2) AS p90_wait_min,
    ROUND(AVG(f.trip_miles), 2)                             AS avg_miles,
    ROUND(AVG(f.total_rider_amount), 2)                     AS avg_total_fare_usd,
    ROUND(SUM(f.airport_fee), 2)                            AS total_airport_fees_collected_usd
FROM fact_trips f
JOIN dim_zone pu   ON f.pu_location_id = pu.location_id
JOIN dim_zone do_z ON f.do_location_id = do_z.location_id
WHERE pu.is_airport OR do_z.is_airport
GROUP BY 1
ORDER BY trip_volume DESC;


-- -----------------------------------------------------------------------------
-- Q5. UberX Share Co-Rider Match Efficiency & Margin Comparison
-- -----------------------------------------------------------------------------
SELECT
    is_shared_matched,
    COUNT(*)                                                AS shared_requests,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)      AS pct_of_shared_requests,
    ROUND(AVG(base_passenger_fare), 2)                      AS avg_rider_base_fare_usd,
    ROUND(AVG(driver_pay_amount), 2)                        AS avg_driver_payout_usd,
    ROUND(AVG(uber_gross_margin), 2)                        AS avg_platform_margin_usd,
    ROUND(AVG(take_rate_pct), 2)                            AS avg_take_rate_pct
FROM fact_trips
WHERE is_shared_trip = TRUE
GROUP BY is_shared_matched;


-- -----------------------------------------------------------------------------
-- Q6. Driver Pro Tier & Electric Vehicle (EV) Hourly Utilization Benchmark
-- -----------------------------------------------------------------------------
SELECT
    driver_tier,
    is_ev_vehicle,
    COUNT(DISTINCT driver_id)                               AS active_drivers,
    ROUND(AVG(completed_trips), 1)                          AS avg_daily_trips,
    ROUND(AVG(total_active_hours), 2)                       AS avg_daily_active_hours,
    ROUND(AVG(total_driver_earnings_usd), 2)                AS avg_daily_earnings_usd,
    ROUND(AVG(earnings_per_active_hour_usd), 2)             AS avg_hourly_earnings_usd
FROM fact_driver_daily_summary
GROUP BY driver_tier, is_ev_vehicle
ORDER BY avg_hourly_earnings_usd DESC;


-- -----------------------------------------------------------------------------
-- Q7. Point-in-Time SCD Type 2 Analysis: Uber One Member Tip & Spend Lift
-- -----------------------------------------------------------------------------
SELECT
    r.membership_status,
    COUNT(f.trip_id)                                        AS total_trips,
    COUNT(DISTINCT f.rider_id)                              AS distinct_riders,
    ROUND(COUNT(f.trip_id) * 1.0 / COUNT(DISTINCT f.rider_id), 2) AS trips_per_rider,
    ROUND(AVG(f.total_rider_amount), 2)                     AS avg_trip_spend_usd,
    ROUND(AVG(CASE WHEN f.tips_amount > 0 THEN 100.0 ELSE 0.0 END), 2) AS tip_frequency_pct,
    ROUND(AVG(f.tips_amount), 2)                            AS avg_tip_usd
FROM fact_trips f
JOIN dim_rider_scd2 r
    ON f.rider_sk = r.rider_sk
GROUP BY r.membership_status
ORDER BY trips_per_rider DESC;


-- -----------------------------------------------------------------------------
-- Q8. Top 15 High-Revenue NYC Origin-Destination Corridors
-- -----------------------------------------------------------------------------
SELECT
    CONCAT(pu.borough, ': ', pu.zone_name, ' -> ', do_z.borough, ': ', do_z.zone_name) AS od_corridor,
    COUNT(*)                                                AS trip_count,
    ROUND(SUM(f.total_rider_amount), 2)                     AS corridor_gross_bookings_usd,
    ROUND(AVG(f.trip_miles), 2)                             AS avg_miles,
    ROUND(AVG(f.trip_duration_sec) / 60.0, 1)               AS avg_duration_min,
    ROUND(AVG(f.avg_speed_mph), 1)                          AS avg_speed_mph
FROM fact_trips f
JOIN dim_zone pu   ON f.pu_location_id = pu.location_id
JOIN dim_zone do_z ON f.do_location_id = do_z.location_id
GROUP BY 1
ORDER BY corridor_gross_bookings_usd DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- Q9. Commuter Rush-Hour vs Late-Night Marketplace Dynamics
-- -----------------------------------------------------------------------------
SELECT
    d.is_weekend,
    t.time_of_day_bucket,
    COUNT(*)                                                AS trips,
    ROUND(AVG(f.surge_multiplier), 2)                       AS avg_surge,
    ROUND(AVG(f.avg_speed_mph), 1)                          AS avg_mph,
    ROUND(AVG(f.driver_arrival_sec) / 60.0, 2)              AS avg_driver_eta_min,
    ROUND(AVG(f.total_rider_amount), 2)                     AS avg_fare_usd
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
JOIN dim_time t ON f.pickup_time_key = t.time_key
GROUP BY d.is_weekend, t.time_of_day_bucket
ORDER BY d.is_weekend, trips DESC;


-- -----------------------------------------------------------------------------
-- Q10. Trust, Safety & ML Risk Review Queue (Combining ML Predictions + Fact)
-- -----------------------------------------------------------------------------
SELECT
    r.risk_flag_reason,
    COUNT(*)                                                AS flagged_trips,
    ROUND(AVG(r.circuity_ratio), 2)                         AS avg_circuity_ratio,
    ROUND(AVG(r.base_passenger_fare), 2)                    AS avg_base_fare_usd,
    ROUND(AVG(r.fraud_anomaly_score), 2)                    AS avg_anomaly_zscore,
    ROUND(AVG(r.cancel_risk_prob), 3)                       AS avg_cancel_prob
FROM ml_trip_risk_scores r
GROUP BY r.risk_flag_reason
ORDER BY flagged_trips DESC;
