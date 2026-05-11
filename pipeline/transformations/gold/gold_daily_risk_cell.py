from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="vave.gold.daily_risk_cell",
    comment="Gold layer - Daily risk aggregation by H3 cell",
    partition_cols=["event_dt"]
)
def daily_risk_cell():
    return (
        spark.read.table("vave.silver.vave_eventsfeed")
        .groupBy("event_dt", "h3_ix")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("risk_score").alias("total_risk_score"),
            F.avg("risk_score").alias("avg_risk_score")
        )
    )