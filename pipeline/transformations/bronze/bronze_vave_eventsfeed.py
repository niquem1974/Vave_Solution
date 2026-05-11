from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="vave.bronze.vave_eventsfeed",
    comment="Bronze layer - Raw event data ingested from CSV files via Auto Loader"
)
def vave_eventsfeed():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/vave/_raw/external_data/vave_raw")
        .withColumn("source_file_name", F.col("_metadata.file_path"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )
