from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="vave.gold.daily_risk_zone",
    comment="Gold layer - Daily risk aggregation by zone (H3 cell + neighbors)",
    partition_cols=["event_dt"]
)
def daily_risk_zone():
    # Read the daily risk by cell
    risk_cell = spark.read.table("vave.gold.daily_risk_cell")
    
    # Expand each cell to include its neighbors (k-ring of 1)
    cells_with_neighbors = (
        risk_cell
        .select("event_dt", "h3_ix")
        .withColumn("member_h3", F.explode(F.expr("h3_kring(h3_ix, 1)")))
    )
    
    # Join back to get risk metrics for neighboring cells
    zone_aggregation = (
        cells_with_neighbors.alias("c")
        .join(
            risk_cell.alias("r"),
            (F.col("c.event_dt") == F.col("r.event_dt")) & (F.col("c.member_h3") == F.col("r.h3_ix")),
            "left"
        )
        .groupBy(F.col("c.event_dt"), F.col("c.h3_ix").alias("zone_id"))
        .agg(
            F.count("*").alias("contributing_cells"),
            F.sum("r.event_count").alias("event_count"),
            F.sum("r.total_risk_score").alias("total_risk_score"),
            F.avg("r.avg_risk_score").alias("avg_cell_risk_score")
        )
    )
    
    return zone_aggregation