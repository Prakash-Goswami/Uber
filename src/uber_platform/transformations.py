"""
Medallion Data Transformations, Data Quality Quarantine, SCD Type 2 & Kimball Star Schema Builders.

100% Catalyst / Photon native DataFrame API (zero Python UDFs, zero `.rdd` / `sparkContext` usage),
guaranteeing full compatibility with Databricks Serverless, Free Edition, and Standard Clusters.
"""

from __future__ import annotations

from typing import Any, List, Tuple

from uber_platform.geospatial import (
    AIRPORT_LOCATION_IDS,
    add_h3_columns_spark,
    haversine_km_spark,
    spark_supports_native_h3,
)


def build_silver_weather(raw_weather_df: Any) -> Any:
    """Clean Open-Meteo hourly weather records and derive marketplace weather categories."""
    from pyspark.sql import functions as F

    obs_ts = F.to_timestamp(F.col("observation_hour_utc"))
    return (
        raw_weather_df.withColumn("observation_hour_ts", F.date_trunc("hour", obs_ts))
        .withColumn("weather_hour_key", F.date_format("observation_hour_ts", "yyyyMMddHH"))
        .withColumn("temp_celsius", F.coalesce(F.col("temp_celsius").cast("double"), F.lit(5.0)))
        .withColumn("precipitation_mm", F.coalesce(F.col("precipitation_mm").cast("double"), F.lit(0.0)))
        .withColumn("rain_mm", F.coalesce(F.col("rain_mm").cast("double"), F.lit(0.0)))
        .withColumn("snowfall_cm", F.coalesce(F.col("snowfall_cm").cast("double"), F.lit(0.0)))
        .withColumn("wind_speed_kmh", F.coalesce(F.col("wind_speed_kmh").cast("double"), F.lit(10.0)))
        .withColumn("weather_code", F.coalesce(F.col("weather_code").cast("int"), F.lit(0)))
        .withColumn(
            "weather_condition",
            F.when(F.col("snowfall_cm") > 0.2, F.lit("Snow"))
            .when(F.col("rain_mm") > 4.0, F.lit("Heavy_Rain"))
            .when(F.col("rain_mm") > 0.1, F.lit("Rain"))
            .when(F.col("weather_code").isin(95, 96, 99), F.lit("Storm"))
            .when(F.col("weather_code") >= 3, F.lit("Cloudy"))
            .otherwise(F.lit("Clear")),
        )
        .withColumn(
            "is_adverse_weather",
            (F.col("rain_mm") >= 1.5)
            | (F.col("snowfall_cm") >= 0.3)
            | (F.col("wind_speed_kmh") >= 32.0),
        )
        .dropDuplicates(["weather_hour_key"])
    )


def build_dim_zone(raw_zones_df: Any) -> Any:
    """Build Gold `dim_zone` enriched with Uber H3 Res-7 and Res-8 hexagon IDs."""
    from pyspark.sql import functions as F

    native_h3 = spark_supports_native_h3(raw_zones_df.sparkSession)
    z = (
        raw_zones_df.select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.coalesce(F.col("Borough"), F.lit("Unknown")).alias("borough"),
            F.coalesce(F.col("Zone"), F.lit("Unknown")).alias("zone_name"),
            F.coalesce(F.col("service_zone"), F.lit("Boro Zone")).alias("service_zone"),
            F.col("centroid_lat").cast("double").alias("centroid_lat"),
            F.col("centroid_lon").cast("double").alias("centroid_lon"),
        )
        .filter(F.col("location_id").isNotNull())
        .dropDuplicates(["location_id"])
    )
    z = add_h3_columns_spark(
        z, "centroid_lat", "centroid_lon", prefix="centroid", resolutions=(7, 8), native_h3=native_h3
    )
    return z.withColumn(
        "is_airport",
        F.col("location_id").isin(*AIRPORT_LOCATION_IDS) | (F.col("service_zone") == F.lit("Airports")),
    )


def clean_and_split_fhvhv_trips(
    bronze_trips_df: Any,
    dim_zone_df: Any,
    num_drivers: int = 600,
    num_riders: int = 2_500,
    uber_only: bool = True,
) -> Tuple[Any, Any]:
    """
    Transform raw NYC TLC HVFHV trips into:
      1. `silver_trips_df`: Validated, deduplicated, H3-indexed Uber trips with full marketplace economics.
      2. `quarantine_trips_df`: Corrupt/invalid trips tagged with `dq_failed_rules` array.
    """
    from pyspark.sql import functions as F

    df = bronze_trips_df
    if uber_only:
        df = df.filter(F.col("hvfhs_license_num") == F.lit("HV0003"))

    # Deterministic idempotent SHA-256 trip_id
    df = df.withColumn(
        "trip_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("hvfhs_license_num"), F.lit("")),
                F.coalesce(F.col("dispatching_base_num"), F.lit("")),
                F.coalesce(F.col("pickup_datetime").cast("string"), F.lit("")),
                F.coalesce(F.col("dropoff_datetime").cast("string"), F.lit("")),
                F.coalesce(F.col("PULocationID").cast("string"), F.lit("")),
                F.coalesce(F.col("DOLocationID").cast("string"), F.lit("")),
                F.coalesce(F.col("trip_miles").cast("string"), F.lit("")),
                F.coalesce(F.col("base_passenger_fare").cast("string"), F.lit("")),
            ),
            256,
        ),
    )

    # Standardize numeric & timestamp types
    df = (
        df.withColumn("pickup_ts", F.col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_ts", F.col("dropoff_datetime").cast("timestamp"))
        .withColumn(
            "request_ts",
            F.coalesce(
                F.col("request_datetime").cast("timestamp"),
                F.expr("pickup_datetime - INTERVAL 4 MINUTES"),
            ),
        )
        .withColumn(
            "on_scene_ts",
            F.coalesce(
                F.col("on_scene_datetime").cast("timestamp"),
                F.expr("pickup_datetime - INTERVAL 1 MINUTE"),
            ),
        )
        .withColumn("pu_location_id", F.col("PULocationID").cast("int"))
        .withColumn("do_location_id", F.col("DOLocationID").cast("int"))
        .withColumn("trip_miles", F.col("trip_miles").cast("double"))
        .withColumn(
            "trip_duration_sec",
            F.coalesce(
                F.col("trip_time").cast("double"),
                F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts"),
            ),
        )
        .withColumn("base_passenger_fare", F.col("base_passenger_fare").cast("double"))
        .withColumn("driver_pay_amount", F.col("driver_pay").cast("double"))
        .withColumn("tolls_amount", F. greatest(F.lit(0.0), F.coalesce(F.col("tolls").cast("double"), F.lit(0.0))))
        .withColumn("bcf_amount", F.greatest(F.lit(0.0), F.coalesce(F.col("bcf").cast("double"), F.lit(0.0))))
        .withColumn("sales_tax_amount", F.greatest(F.lit(0.0), F.coalesce(F.col("sales_tax").cast("double"), F.lit(0.0))))
        .withColumn(
            "congestion_surcharge",
            F.greatest(F.lit(0.0), F.coalesce(F.col("congestion_surcharge").cast("double"), F.lit(0.0))),
        )
        .withColumn("airport_fee", F.greatest(F.lit(0.0), F.coalesce(F.col("airport_fee").cast("double"), F.lit(0.0))))
        .withColumn("tips_amount", F.greatest(F.lit(0.0), F.coalesce(F.col("tips").cast("double"), F.lit(0.0))))
    )

    implied_speed_mph = F.when(
        F.col("trip_duration_sec") > 0,
        F.col("trip_miles") / (F.col("trip_duration_sec") / F.lit(3600.0)),
    ).otherwise(F.lit(999.0))

    # 7 Enterprise Data Quality Rules
    dq_rules_array = F.filter(
        F.array(
            F.when(F.col("hvfhs_license_num").isNull(), F.lit("DQ_01_MISSING_LICENSE")),
            F.when(
                F.col("pickup_ts").isNull()
                | F.col("dropoff_ts").isNull()
                | (F.col("dropoff_ts") <= F.col("pickup_ts")),
                F.lit("DQ_02_INVALID_TIMESTAMPS"),
            ),
            F.when(
                F.col("trip_miles").isNull()
                | (F.col("trip_miles") <= 0.05)
                | (F.col("trip_miles") > 250.0),
                F.lit("DQ_03_INVALID_TRIP_MILES"),
            ),
            F.when(
                F.col("trip_duration_sec").isNull()
                | (F.col("trip_duration_sec") < 30.0)
                | (F.col("trip_duration_sec") > 21600.0),
                F.lit("DQ_04_INVALID_DURATION_SEC"),
            ),
            F.when(
                F.col("base_passenger_fare").isNull()
                | (F.col("base_passenger_fare") <= 0.0)
                | F.col("driver_pay_amount").isNull()
                | (F.col("driver_pay_amount") < 0.0),
                F.lit("DQ_05_NEGATIVE_OR_ZERO_FARE"),
            ),
            F.when(implied_speed_mph > 110.0, F.lit("DQ_06_IMPOSSIBLE_SPEED_GT_110MPH")),
            F.when(
                (~F.col("pu_location_id").between(1, 265))
                | (~F.col("do_location_id").between(1, 265)),
                F.lit("DQ_07_UNKNOWN_NYC_ZONE"),
            ),
        ),
        lambda x: x.isNotNull(),
    )

    df_tagged = df.withColumn("dq_failed_rules", dq_rules_array)

    quarantine_df = (
        df_tagged.filter(F.size(F.col("dq_failed_rules")) > 0)
        .withColumn("quarantined_at_ts", F.current_timestamp())
    )

    clean_df = df_tagged.filter(F.size(F.col("dq_failed_rules")) == 0).drop("dq_failed_rules")
    clean_df = clean_df.dropDuplicates(["trip_id"])

    # Deterministically assign realistic Driver ID & Rider ID via hash modulo
    hash_num = F.abs(F.xxhash64(F.col("trip_id")))
    clean_df = clean_df.withColumn(
        "driver_id",
        F.format_string("DRV-%05d", (hash_num % F.lit(num_drivers)).cast("int") + F.lit(1)),
    ).withColumn(
        "rider_id",
        F.format_string(
            "RDR-%06d",
            ((F.abs(F.xxhash64(F.col("pickup_ts"), F.col("pu_location_id"))) % F.lit(num_riders)).cast("int") + F.lit(1)),
        ),
    )

    # Enrich with Pickup & Dropoff Zone Coordinates + street-level deterministic micro-offset
    pu_zones = dim_zone_df.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pu_borough"),
        F.col("zone_name").alias("pu_zone_name"),
        F.col("centroid_lat").alias("_pu_base_lat"),
        F.col("centroid_lon").alias("_pu_base_lon"),
        F.col("is_airport").alias("_pu_is_airport"),
    )
    do_zones = dim_zone_df.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("do_borough"),
        F.col("zone_name").alias("do_zone_name"),
        F.col("centroid_lat").alias("_do_base_lat"),
        F.col("centroid_lon").alias("_do_base_lon"),
        F.col("is_airport").alias("_do_is_airport"),
    )

    clean_df = clean_df.join(F.broadcast(pu_zones), on="pu_location_id", how="left").join(
        F.broadcast(do_zones), on="do_location_id", how="left"
    )

    # Add realistic street-block coordinate jitter (~ +/- 350m) inside each neighborhood zone
    jitter_1 = ((F.abs(F.hash(F.col("trip_id"), F.lit("lat1"))) % 1000) / 1000.0 - 0.5) * 0.007
    jitter_2 = ((F.abs(F.hash(F.col("trip_id"), F.lit("lon1"))) % 1000) / 1000.0 - 0.5) * 0.007
    jitter_3 = ((F.abs(F.hash(F.col("trip_id"), F.lit("lat2"))) % 1000) / 1000.0 - 0.5) * 0.007
    jitter_4 = ((F.abs(F.hash(F.col("trip_id"), F.lit("lon2"))) % 1000) / 1000.0 - 0.5) * 0.007

    clean_df = (
        clean_df.withColumn("pickup_lat", F.round(F.coalesce(F.col("_pu_base_lat"), F.lit(40.7580)) + jitter_1, 6))
        .withColumn("pickup_lon", F.round(F.coalesce(F.col("_pu_base_lon"), F.lit(-73.9855)) + jitter_2, 6))
        .withColumn("dropoff_lat", F.round(F.coalesce(F.col("_do_base_lat"), F.lit(40.7505)) + jitter_3, 6))
        .withColumn("dropoff_lon", F.round(F.coalesce(F.col("_do_base_lon"), F.lit(-73.9934)) + jitter_4, 6))
    )

    # Index Pickup & Dropoff into Uber H3 Resolution 7 & Resolution 8
    native_h3 = spark_supports_native_h3(clean_df.sparkSession)
    clean_df = add_h3_columns_spark(
        clean_df, "pickup_lat", "pickup_lon", prefix="pickup", resolutions=(7, 8), native_h3=native_h3
    )
    clean_df = add_h3_columns_spark(
        clean_df, "dropoff_lat", "dropoff_lon", prefix="dropoff", resolutions=(7, 8), native_h3=native_h3
    )

    # Great-circle Haversine distance & Uber Marketplace SLA + Financial Metrics
    fare_per_mile = F.col("base_passenger_fare") / F.greatest(F.col("trip_miles"), F.lit(0.5))
    expected_baseline_fare = F.greatest(
        F.lit(8.00),
        F.lit(2.50) + F.lit(1.75) * F.col("trip_miles") + F.lit(0.58) * (F.col("trip_duration_sec") / F.lit(60.0)),
    )

    clean_df = (
        clean_df.withColumn(
            "haversine_km",
            F.round(haversine_km_spark("pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon"), 3),
        )
        .withColumn(
            "wait_time_sec",
            F.greatest(F.lit(15.0), (F.unix_timestamp("pickup_ts") - F.unix_timestamp("request_ts")).cast("double")),
        )
        .withColumn(
            "driver_arrival_sec",
            F.greatest(F.lit(10.0), (F.unix_timestamp("on_scene_ts") - F.unix_timestamp("request_ts")).cast("double")),
        )
        .withColumn(
            "curb_wait_sec",
            F.greatest(F.lit(0.0), (F.unix_timestamp("pickup_ts") - F.unix_timestamp("on_scene_ts")).cast("double")),
        )
        .withColumn(
            "avg_speed_mph",
            F.round(F.col("trip_miles") / (F.col("trip_duration_sec") / F.lit(3600.0)), 2),
        )
        .withColumn(
            "product_type",
            F.when(F.col("wav_request_flag") == F.lit("Y"), F.lit("UberWAV"))
            .when(F.col("shared_request_flag") == F.lit("Y"), F.lit("UberX Share"))
            .when((fare_per_mile >= 8.5) & (F.col("trip_miles") >= 2.0), F.lit("Uber Black/XL"))
            .when(fare_per_mile >= 5.5, F.lit("Uber Comfort"))
            .otherwise(F.lit("UberX")),
        )
        .withColumn(
            "surge_multiplier",
            F.round(
                F.least(F.lit(4.0), F.greatest(F.lit(1.0), F.col("base_passenger_fare") / expected_baseline_fare)),
                2,
            ),
        )
        .withColumn("taxes_and_fees", F.round(F.col("bcf_amount") + F.col("sales_tax_amount"), 2))
        .withColumn(
            "total_rider_amount",
            F.round(
                F.col("base_passenger_fare")
                + F.col("tolls_amount")
                + F.col("bcf_amount")
                + F.col("sales_tax_amount")
                + F.col("congestion_surcharge")
                + F.col("airport_fee")
                + F.col("tips_amount"),
                2,
            ),
        )
        .withColumn(
            "uber_gross_margin",
            F.round(F.col("base_passenger_fare") - F.col("driver_pay_amount"), 2),
        )
        .withColumn(
            "take_rate_pct",
            F.round(
                (F.col("base_passenger_fare") - F.col("driver_pay_amount"))
                / F.greatest(F.col("base_passenger_fare"), F.lit(1.0))
                * F.lit(100.0),
                2,
            ),
        )
        .withColumn("is_airport_trip", F.coalesce(F.col("_pu_is_airport"), F.lit(False)) | F.coalesce(F.col("_do_is_airport"), F.lit(False)) | (F.col("airport_fee") > 0))
        .withColumn("is_shared_trip", F.col("shared_request_flag") == F.lit("Y"))
        .withColumn("is_shared_matched", F.col("shared_match_flag") == F.lit("Y"))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_date_key", F.date_format("pickup_ts", "yyyyMMdd").cast("int"))
        .withColumn("pickup_time_key", (F.hour("pickup_ts") * 100 + F.minute("pickup_ts")).cast("int"))
        .withColumn("weather_hour_key", F.date_format(F.date_trunc("hour", F.col("pickup_ts")), "yyyyMMddHH"))
        .withColumn("silver_processed_ts", F.current_timestamp())
        .drop("_pu_base_lat", "_pu_base_lon", "_do_base_lat", "_do_base_lon", "_pu_is_airport", "_do_is_airport")
    )

    return clean_df, quarantine_df


def apply_scd2_from_cdc(
    spark: Any,
    cdc_df: Any,
    target_table_fqn: str,
    natural_key: str,
    surrogate_key: str,
    tracked_columns: List[str],
) -> Any:
    """
    Build or incrementally update an SCD Type 2 Delta dimension table from CDC events.

    Uses window lead() over chronological CDC events for clean initial/full history construction
    plus Delta Lake `MERGE INTO` semantics when incrementally merging new CDC batches into an
    existing SCD Type 2 table.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Deduplicate exact duplicate CDC event deliveries
    dedup_cdc = (
        cdc_df.filter(F.col("op").isin("c", "u"))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .dropDuplicates([natural_key, "event_ts"])
    )

    if not spark.catalog.tableExists(target_table_fqn):
        w_hist = Window.partitionBy(natural_key).orderBy(F.col("event_ts").asc())
        far_future = F.to_timestamp(F.lit("9999-12-31 23:59:59"))
        next_ts = F.lead("event_ts").over(w_hist)

        scd2_init = (
            dedup_cdc.withColumn("effective_start_ts", F.col("event_ts"))
            .withColumn("effective_end_ts", F.coalesce(next_ts, far_future))
            .withColumn("is_current", next_ts.isNull())
            .withColumn(
                surrogate_key,
                F.abs(F.xxhash64(F.col(natural_key), F.col("effective_start_ts"))),
            )
            .select(
                surrogate_key,
                natural_key,
                *tracked_columns,
                "effective_start_ts",
                "effective_end_ts",
                "is_current",
            )
        )
        (
            scd2_init.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table_fqn)
        )
        return spark.table(target_table_fqn)

    # Incremental SCD Type 2 Delta MERGE for latest incoming mutation per natural key
    w_latest = Window.partitionBy(natural_key).orderBy(F.col("event_ts").desc())
    latest_incoming = (
        dedup_cdc.withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    target_current = spark.table(target_table_fqn).filter(F.col("is_current") == F.lit(True))

    change_cond = " OR ".join([f"NOT (t.`{c}` <=> s.`{c}`)" for c in tracked_columns])
    staged_updates = (
        latest_incoming.alias("s")
        .join(target_current.alias("t"), on=natural_key, how="left")
        .filter(
            F.col(f"t.{natural_key}").isNull()
            | ((F.expr(change_cond)) & (F.col("s.event_ts") > F.col("t.effective_start_ts")))
        )
        .select("s.*", F.col(f"t.{natural_key}").isNotNull().alias("_has_existing"))
    )

    # Staged union pattern for single-transaction atomic SCD Type 2 Delta MERGE:
    # Row 1 (`merge_key = natural_key`): matches active target row and closes it
    # Row 2 (`merge_key = NULL`): does not match -> inserts new open current version
    rows_to_close = staged_updates.filter(F.col("_has_existing")).withColumn(
        "_merge_key", F.col(natural_key)
    )
    rows_to_insert = staged_updates.withColumn("_merge_key", F.lit(None).cast("string"))
    staged_union = rows_to_close.unionByName(rows_to_insert)

    temp_view = f"_tmp_scd2_stage_{natural_key}"
    staged_union.createOrReplaceTempView(temp_view)

    insert_cols = ", ".join(
        [surrogate_key, natural_key] + tracked_columns + ["effective_start_ts", "effective_end_ts", "is_current"]
    )
    insert_vals = ", ".join(
        [f"abs(xxhash64(s.{natural_key}, s.event_ts))", f"s.{natural_key}"]
        + [f"s.{c}" for c in tracked_columns]
        + ["s.event_ts", " TIMESTAMP '9999-12-31 23:59:59'", "true"]
    )

    spark.sql(
        f"""
        MERGE INTO {target_table_fqn} AS t
        USING {temp_view} AS s
        ON t.{natural_key} = s._merge_key AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            t.effective_end_ts = s.event_ts,
            t.is_current = false
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
        """
    )
    return spark.table(target_table_fqn)


def build_dim_date(spark: Any, start_date: str = "2023-12-01", end_date: str = "2024-03-31") -> Any:
    """Generate Kimball `dim_date` calendar table using pure Spark SQL sequence()."""
    from pyspark.sql import functions as F

    holidays = ["2023-12-25", "2024-01-01", "2024-01-15", "2024-02-19"]
    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date"
        )
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .withColumn("is_us_holiday", F.col("full_date").cast("string").isin(*holidays))
    )


def build_dim_time(spark: Any) -> Any:
    """Generate Kimball `dim_time` grain of every minute of the 24-hour day (1,440 rows)."""
    from pyspark.sql import functions as F

    return (
        spark.range(0, 1440)
        .withColumn("hour_24", (F.col("id") / 60).cast("int"))
        .withColumn("minute", (F.col("id") % 60).cast("int"))
        .withColumn("time_key", (F.col("hour_24") * 100 + F.col("minute")).cast("int"))
        .withColumn(
            "time_of_day_bucket",
            F.when(F.col("hour_24").between(7, 9), F.lit("AM_Rush"))
            .when(F.col("hour_24").between(10, 15), F.lit("Midday"))
            .when(F.col("hour_24").between(16, 19), F.lit("PM_Rush"))
            .when(F.col("hour_24").between(20, 22), F.lit("Evening"))
            .when((F.col("hour_24") >= 23) | (F.col("hour_24") <= 3), F.lit("Late_Night"))
            .otherwise(F.lit("Early_Morning")),
        )
        .withColumn("is_rush_hour", F.col("hour_24").between(7, 9) | F.col("hour_24").between(16, 19))
        .drop("id")
    )


def build_fact_trips(silver_trips_df: Any, dim_driver_scd2_df: Any, dim_rider_scd2_df: Any) -> Any:
    """
    Build Gold `fact_trips` with point-in-time SCD Type 2 surrogate key resolution
    for both `driver_sk` and `rider_sk`.
    """
    from pyspark.sql import functions as F

    drv = dim_driver_scd2_df.select(
        F.col("driver_id").alias("_d_id"),
        F.col("driver_sk"),
        F.col("effective_start_ts").alias("_d_start"),
        F.col("effective_end_ts").alias("_d_end"),
    )
    rdr = dim_rider_scd2_df.select(
        F.col("rider_id").alias("_r_id"),
        F.col("rider_sk"),
        F.col("effective_start_ts").alias("_r_start"),
        F.col("effective_end_ts").alias("_r_end"),
    )

    joined = (
        silver_trips_df.join(
            drv,
            (silver_trips_df["driver_id"] == drv["_d_id"])
            & (silver_trips_df["pickup_ts"] >= drv["_d_start"])
            & (silver_trips_df["pickup_ts"] < drv["_d_end"]),
            how="left",
        )
        .join(
            rdr,
            (silver_trips_df["rider_id"] == rdr["_r_id"])
            & (silver_trips_df["pickup_ts"] >= rdr["_r_start"])
            & (silver_trips_df["pickup_ts"] < rdr["_r_end"]),
            how="left",
        )
        .drop("_d_id", "_d_start", "_d_end", "_r_id", "_r_start", "_r_end")
    )

    return joined.select(
        "trip_id",
        F.coalesce(F.col("driver_sk"), F.lit(-1)).alias("driver_sk"),
        F.coalesce(F.col("rider_sk"), F.lit(-1)).alias("rider_sk"),
        "driver_id",
        "rider_id",
        "pickup_date",
        "pickup_date_key",
        "pickup_time_key",
        "pu_location_id",
        "do_location_id",
        "weather_hour_key",
        "pickup_h3_r7",
        "pickup_h3_r8",
        "dropoff_h3_r7",
        "dropoff_h3_r8",
        "pickup_lat",
        "pickup_lon",
        "dropoff_lat",
        "dropoff_lon",
        "product_type",
        "request_ts",
        "on_scene_ts",
        "pickup_ts",
        "dropoff_ts",
        "wait_time_sec",
        "driver_arrival_sec",
        "curb_wait_sec",
        "trip_duration_sec",
        "trip_miles",
        "haversine_km",
        "avg_speed_mph",
        "surge_multiplier",
        "base_passenger_fare",
        "tolls_amount",
        "congestion_surcharge",
        "airport_fee",
        "tips_amount",
        "taxes_and_fees",
        "total_rider_amount",
        "driver_pay_amount",
        "uber_gross_margin",
        "take_rate_pct",
        "is_airport_trip",
        "is_shared_trip",
        "is_shared_matched",
    )


def build_fact_hex_supply_demand_5m(
    silver_trips_df: Any,
    silver_hex_pings_5m_df: Any | None = None,
    silver_lifecycle_df: Any | None = None,
) -> Any:
    """
    Build Gold `fact_hex_supply_demand_5m` periodic snapshot fact table at `(window_start_ts, h3_r7)` grain.

    Combines historical/batch Uber trip demand & active drivers in each 5-minute H3 hexagon
    with real-time streaming GPS telemetry supply & cancellation counts when available.
    """
    from pyspark.sql import functions as F

    trip_hex_5m = (
        silver_trips_df.withColumn("win", F.window(F.col("request_ts"), "5 minutes"))
        .groupBy(
            F.col("win.start").alias("window_start_ts"),
            F.col("win.end").alias("window_end_ts"),
            F.col("pickup_h3_r7").alias("h3_r7"),
        )
        .agg(
            F.count("*").alias("completed_pickups"),
            F.countDistinct("driver_id").alias("trip_active_drivers"),
            F.round(F.avg("driver_arrival_sec"), 1).alias("avg_pickup_eta_sec"),
            F.round(F.avg("surge_multiplier"), 2).alias("observed_avg_surge"),
        )
    )

    if silver_hex_pings_5m_df is not None:
        pings = silver_hex_pings_5m_df.select(
            F.col("window_start_ts"),
            F.col("window_end_ts"),
            F.col("h3_r7"),
            F.col("distinct_available_drivers").alias("ping_available_drivers"),
        )
        combined = trip_hex_5m.join(pings, on=["window_start_ts", "window_end_ts", "h3_r7"], how="full_outer")
    else:
        combined = trip_hex_5m.withColumn("ping_available_drivers", F.lit(0))

    if silver_lifecycle_df is not None:
        cancels = (
            silver_lifecycle_df.filter(F.col("event_type").startswith("CANCELLED"))
            .withColumn("win", F.window(F.col("event_ts"), "5 minutes"))
            .groupBy(
                F.col("win.start").alias("window_start_ts"),
                F.col("win.end").alias("window_end_ts"),
                F.col("pickup_h3_r7").alias("h3_r7"),
            )
            .agg(F.count("*").alias("cancelled_requests"))
        )
        combined = combined.join(cancels, on=["window_start_ts", "window_end_ts", "h3_r7"], how="left")
    else:
        combined = combined.withColumn("cancelled_requests", F.lit(0))

    completed = F.coalesce(F.col("completed_pickups"), F.lit(0))
    cancelled = F.coalesce(F.col("cancelled_requests"), F.lit(0))
    demand = completed + cancelled
    supply = F.greatest(
        F.lit(1),
        F.coalesce(F.col("ping_available_drivers"), F.lit(0))
        + F.coalesce(F.col("trip_active_drivers"), F.lit(0)),
    )
    ds_ratio = demand.cast("double") / supply.cast("double")

    # Uber marketplace equilibrium surge curve: 1.0x when balanced, scaling smoothly up to 3.5x
    rec_surge = F.round(
        F.least(
            F.lit(3.5),
            F.greatest(
                F.lit(1.0),
                F.coalesce(F.col("observed_avg_surge"), F.lit(1.0)) * F.lit(0.5)
                + (F.lit(1.0) + F.greatest(F.lit(0.0), ds_ratio - F.lit(1.0)) * F.lit(0.45)) * F.lit(0.5),
            ),
        ),
        2,
    )

    return (
        combined.withColumn("completed_pickups", completed.cast("int"))
        .withColumn("unfulfilled_requests", cancelled.cast("int"))
        .withColumn("ride_requests_demand", demand.cast("int"))
        .withColumn("active_drivers_supply", supply.cast("int"))
        .withColumn("demand_supply_ratio", F.round(ds_ratio, 3))
        .withColumn("recommended_surge_multiplier", rec_surge)
        .withColumn("avg_pickup_eta_sec", F.coalesce(F.col("avg_pickup_eta_sec"), F.lit(180.0)))
        .withColumn(
            "weather_hour_key",
            F.date_format(F.date_trunc("hour", F.col("window_start_ts")), "yyyyMMddHH"),
        )
        .withColumn(
            "window_hex_id",
            F.concat_ws("_", F.date_format("window_start_ts", "yyyyMMddHHmm"), F.col("h3_r7")),
        )
        .select(
            "window_hex_id",
            "window_start_ts",
            "window_end_ts",
            "h3_r7",
            "active_drivers_supply",
            "ride_requests_demand",
            "completed_pickups",
            "unfulfilled_requests",
            "demand_supply_ratio",
            "recommended_surge_multiplier",
            "avg_pickup_eta_sec",
            "weather_hour_key",
        )
    )


def build_fact_driver_daily_summary(fact_trips_df: Any, dim_driver_scd2_df: Any) -> Any:
    """Build Gold `fact_driver_daily_summary` periodic daily driver earnings & utilization table."""
    from pyspark.sql import functions as F

    cur_drv = dim_driver_scd2_df.filter(F.col("is_current") == F.lit(True)).select(
        "driver_id", "home_borough", "driver_tier", "vehicle_type", "is_ev_vehicle"
    )
    agg = (
        fact_trips_df.groupBy("pickup_date_key", "pickup_date", "driver_id")
        .agg(
            F.count("*").alias("completed_trips"),
            F.round(F.sum("trip_miles"), 2).alias("total_miles_driven"),
            F.round(F.sum("trip_duration_sec") / 3600.0, 2).alias("engaged_trip_hours"),
            F.round(F.sum(F.col("trip_duration_sec") + F.col("driver_arrival_sec")) / 3600.0, 2).alias(
                "total_active_hours"
            ),
            F.round(F.sum("driver_pay_amount"), 2).alias("base_driver_pay_usd"),
            F.round(F.sum("tips_amount"), 2).alias("total_tips_usd"),
            F.round(F.sum(F.col("driver_pay_amount") + F.col("tips_amount")), 2).alias("total_driver_earnings_usd"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge_captured"),
            F.sum(F.when(F.col("is_airport_trip"), F.lit(1)).otherwise(F.lit(0))).alias("airport_trips"),
        )
        .withColumn(
            "earnings_per_active_hour_usd",
            F.round(
                F.col("total_driver_earnings_usd") / F.greatest(F.col("total_active_hours"), F.lit(0.25)),
                2,
            ),
        )
    )
    return agg.join(cur_drv, on="driver_id", how="left")
