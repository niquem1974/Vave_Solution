from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Step 1: Create a temporary view with transformations and data quality filters
@dp.temporary_view()
@dp.expect_all_or_drop({
    "valid_latitude": "latitude BETWEEN -90 AND 90",
    "valid_longitude": "longitude BETWEEN -180 AND 180",
    "valid_risk_score": "risk_score IS NOT NULL",
    "valid_event_id": "event_id IS NOT NULL",
    "valid_h3_index": "h3_ix IS NOT NULL"
})
@dp.expect("valid_event_timestamp", "event_timestamp IS NOT NULL")
def silver_vave_eventsfeed_source():
    return (
        spark.readStream.table("vave.bronze.vave_eventsfeed")
        .select(
            "event_id",
            "event_timestamp",
            F.col("event_timestamp").cast("date").alias("event_dt"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("risk_score").cast("double").alias("risk_score"),
            "source_file_name",
            "ingestion_ts",
            F.expr("h3_longlatash3string(longitude, latitude, 8)").alias("h3_ix")
        )
    )

# Step 2: Create the target streaming table
dp.create_streaming_table(
    name="vave.silver.vave_eventsfeed",
    comment="Silver layer - Cleaned and enriched event data with H3 geospatial indexing (latest per event_id)",
    partition_cols=["event_dt"]
)

# Step 3: Apply CDC to keep only the latest record per event_id
dp.create_auto_cdc_flow(
    target="vave.silver.vave_eventsfeed",
    source="silver_vave_eventsfeed_source",
    keys=["event_id"],
    sequence_by="ingestion_ts",
    stored_as_scd_type=1  # SCD Type 1: Keep only the latest version
)