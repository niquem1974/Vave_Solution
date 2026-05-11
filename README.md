# Vave Geospatial Risk Pipeline

A scalable, medallion-architecture data pipeline for processing and analyzing geospatial risk events using Databricks Lakehouse Platform with H3 spatial indexing.

## Overview

The Vave pipeline ingests, enriches, and aggregates risk event data with geographic coordinates, transforming raw event streams into actionable intelligence for risk analysis and monitoring. The pipeline leverages H3 hexagonal hierarchical spatial indexing for efficient geospatial queries and aggregations.

## Architecture

The pipeline implements a **Medallion Architecture** with three distinct layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA FLOW                                │
└─────────────────────────────────────────────────────────────────┘

  📁 Source Files (Volume)              🔄 Auto Loader
        │                                      │
        │                                      │
        ▼                                      ▼
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER - Raw Data Ingestion                               │
│  ─────────────────────────────────────────────────────────────  │
│  Table: main.bronze.vave_eventsfeed                              │
│                                                                   │
│  • Incremental file ingestion with Auto Loader                   │
│  • Schema evolution support                                      │
│  • File metadata tracking (_metadata columns)                    │
│  • Preserves raw source data (append-only)                       │
│                                                                   │
│  Columns:                                                        │
│    - event_id (STRING): Unique event identifier                  │
│    - event_timestamp (TIMESTAMP): Event occurrence time          │
│    - latitude (DOUBLE): Geographic latitude                      │
│    - longitude (DOUBLE): Geographic longitude                    │
│    - risk_score (DOUBLE): Risk assessment score                  │
│    - _metadata: Auto Loader file tracking metadata               │
└──────────────────────────────────────────────────────────────────┘
        │
        │  Deduplication + H3 Enrichment
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  SILVER LAYER - Enriched & Cleansed Data                         │
│  ─────────────────────────────────────────────────────────────  │
│  Table: main.silver.vave_eventsfeed                              │
│                                                                   │
│  • Event deduplication (MERGE on event_id)                       │
│  • H3 geospatial index generation (resolution 7)                 │
│  • Date partitioning (event_dt)                                  │
│  • Data quality validation                                       │
│                                                                   │
│  Columns:                                                        │
│    - event_id (STRING): Unique event identifier                  │
│    - event_timestamp (TIMESTAMP): Event occurrence time          │
│    - latitude (DOUBLE): Geographic latitude                      │
│    - longitude (DOUBLE): Geographic longitude                    │
│    - risk_score (DOUBLE): Risk assessment score                  │
│    - h3_ix (STRING): H3 hexagonal spatial index (resolution 7)   │
│    - event_dt (DATE): Event date (for partitioning)              │
└──────────────────────────────────────────────────────────────────┘
        │
        │  Aggregation & Analytics
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  GOLD LAYER - Aggregated Analytics                               │
│  ─────────────────────────────────────────────────────────────  │
│  Materialized Views (Auto-Refresh)                               │
│                                                                   │
│  1. main.gold.daily_risk_cell                                    │
│     • Daily aggregations by H3 cell                              │
│     • Granular geographic risk analysis                          │
│     • Refresh: TRIGGER ON UPDATE                                 │
│                                                                   │
│     Columns:                                                     │
│       - h3_ix (STRING): H3 cell identifier                       │
│       - event_dt (DATE): Aggregation date                        │
│       - total_events (BIGINT): Event count in cell               │
│       - total_risk_score (DOUBLE): Sum of risk scores            │
│       - avg_risk_score (DOUBLE): Average risk score              │
│       - max_risk_score (DOUBLE): Peak risk in cell               │
│       - min_risk_score (DOUBLE): Minimum risk in cell            │
│                                                                   │
│  2. main.gold.daily_risk_zone                                    │
│     • Daily zone-level aggregations                              │
│     • High-level risk trend monitoring                           │
│     • Refresh: TRIGGER ON UPDATE                                 │
│                                                                   │
│     Columns:                                                     │
│       - event_dt (DATE): Aggregation date                        │
│       - total_events (BIGINT): Total event count                 │
│       - total_risk_score (DOUBLE): Sum of all risk scores        │
│       - avg_risk_score (DOUBLE): Average risk score              │
│       - unique_h3_cells (BIGINT): Number of active cells         │
│       - max_cell_risk (DOUBLE): Highest cell risk score          │
└──────────────────────────────────────────────────────────────────┘
```

## Pipeline Components

### 1. Data Ingestion (Bronze Layer)

**Object:** `main.bronze.vave_eventsfeed`  
**Type:** Delta Table (Streaming)  
**Technology:** Auto Loader

**Purpose:**  
Incremental ingestion of raw event files from Unity Catalog Volume (`/Volumes/main/bronze/raw_vave_files/`). Maintains full history with file metadata for auditability.

**Key Features:**
* Automatic schema inference and evolution
* Checkpoint-based incremental processing
* File-level metadata tracking
* Append-only writes for data lineage

### 2. Data Enrichment (Silver Layer)

**Object:** `main.silver.vave_eventsfeed`  
**Type:** Delta Table (MERGE operations)  
**Technology:** Spark SQL + H3 UDFs

**Purpose:**  
Deduplicates events and enriches with H3 geospatial indexes for efficient spatial queries and aggregations.

**Key Features:**
* Event deduplication via MERGE (upsert on `event_id`)
* H3 spatial indexing at resolution 7 (~5km² cells)
* Date partitioning for query optimization
* Data quality validation (null checks, coordinate validation)

**H3 Spatial Index Benefits:**
* Hierarchical hexagonal grid system
* Efficient spatial joins and proximity queries
* Uniform cell area (unlike lat/lon grids)
* Resolution 7 provides ~5km² cells for city-scale analysis

### 3. Analytics Aggregations (Gold Layer)

#### 3.1 Daily Risk by Cell

**Object:** `main.gold.daily_risk_cell`  
**Type:** Materialized View (Auto-Refresh)  
**Refresh Strategy:** TRIGGER ON UPDATE

**Purpose:**  
Provides granular daily risk metrics aggregated by H3 cell for heatmap visualizations and geographic hotspot detection.

**Use Cases:**
* Geographic risk heatmaps
* Cell-level trend analysis
* Hotspot identification
* Spatial pattern detection

#### 3.2 Daily Risk Zone Summary

**Object:** `main.gold.daily_risk_zone`  
**Type:** Materialized View (Auto-Refresh)  
**Refresh Strategy:** TRIGGER ON UPDATE

**Purpose:**  
Provides high-level daily risk summaries across all zones for trend monitoring and executive dashboards.

**Use Cases:**
* Daily risk trend dashboards
* Executive reporting
* Alerting on risk threshold breaches
* Geographic coverage monitoring (active cells)

## Data Flow

### Ingestion Flow

1. **File Upload:** CSV files uploaded to `/Volumes/main/bronze/raw_vave_files/`
2. **Auto Loader:** Detects new files, reads with schema inference
3. **Bronze Write:** Appends to `main.bronze.vave_eventsfeed` with metadata
4. **Silver MERGE:** Reads bronze incremental changes, deduplicates, enriches with H3
5. **Gold Refresh:** Materialized views auto-refresh when silver updates

### Data Transformation Details

```sql
-- Bronze → Silver Transformation
1. SELECT DISTINCT event_id (deduplication)
2. h3_latlng_to_cell(latitude, longitude, 7) AS h3_ix
3. DATE(event_timestamp) AS event_dt
4. MERGE INTO silver ON event_id

-- Silver → Gold Transformations
1. daily_risk_cell:
   GROUP BY h3_ix, event_dt
   Metrics: COUNT(*), SUM(risk_score), AVG/MIN/MAX(risk_score)

2. daily_risk_zone:
   GROUP BY event_dt
   Metrics: COUNT(*), COUNT(DISTINCT h3_ix), aggregate risk metrics
```

## Data Quality & Monitoring

**Data Quality Notebook:** `Vave_Data_Quality.ipynb`

### Quality Checks

1. **File Upload Verification**
   * Ingestion timeline tracking
   * Missing file detection
   * File metadata validation

2. **Bronze Layer Validation**
   * Duplicate detection
   * Null value checks (event_id, coordinates, risk_score)
   * Coordinate range validation (-90/90, -180/180)
   * Daily ingestion volume monitoring

3. **Silver Layer Validation**
   * H3 index population rate (target: >99%)
   * Bronze-to-Silver reconciliation
   * Event_dt population check
   * H3 cell distribution analysis

4. **Gold Layer Verification**
   * Materialized view refresh status
   * Aggregation accuracy (Silver vs Gold comparison)
   * Data freshness checks (lag detection)

5. **End-to-End Flow Validation**
   * Cross-layer record count reconciliation
   * Event ID traceability
   * Data freshness across layers

6. **Quality Metrics Dashboard**
   * Pipeline health summary
   * Quality score card by layer
   * Top 10 data quality issues
   * Daily quality trends

## Key Technologies

* **Databricks Lakehouse Platform:** Unified analytics platform
* **Delta Lake:** ACID transactions, time travel, schema evolution
* **Auto Loader:** Incremental file ingestion with schema inference
* **Spark SQL:** Distributed data processing
* **Materialized Views:** Auto-refreshing aggregations (TRIGGER ON UPDATE)
* **H3 Spatial Index:** Uber's hexagonal hierarchical spatial index
* **Unity Catalog:** Unified governance (catalog: `main`)

## Schema Structure

```
main (Unity Catalog)
├── bronze (Schema)
│   └── vave_eventsfeed (Delta Table - Streaming)
│       └── Source: /Volumes/main/bronze/raw_vave_files/
│
├── silver (Schema)
│   └── vave_eventsfeed (Delta Table - MERGE)
│       └── Source: main.bronze.vave_eventsfeed
│
└── gold (Schema)
    ├── daily_risk_cell (Materialized View)
    │   └── Source: main.silver.vave_eventsfeed
    └── daily_risk_zone (Materialized View)
        └── Source: main.silver.vave_eventsfeed
```

## Performance Optimization

### Bronze Layer
* **Auto Loader checkpointing:** Incremental processing without reprocessing
* **Schema inference:** Automatic detection of schema changes
* **Append-only writes:** Optimized for streaming ingestion

### Silver Layer
* **MERGE operations:** Efficient upserts with predicate pushdown
* **H3 indexing:** Pre-computed spatial index for fast lookups
* **Date partitioning:** Partition pruning for date-filtered queries
* **Deduplication:** Single source of truth for each event

### Gold Layer
* **Materialized Views:** Pre-aggregated data for instant queries
* **TRIGGER ON UPDATE:** Automatic refresh eliminates manual scheduling
* **Granular aggregations:** Separate cell-level and zone-level views

## Query Patterns

### Geographic Hotspot Detection
```sql
SELECT h3_ix, AVG(total_risk_score) AS avg_risk
FROM main.gold.daily_risk_cell
WHERE event_dt BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY h3_ix
HAVING avg_risk > 100
ORDER BY avg_risk DESC;
```

### Daily Risk Trends
```sql
SELECT event_dt, total_risk_score, unique_h3_cells
FROM main.gold.daily_risk_zone
ORDER BY event_dt DESC
LIMIT 30;
```

### Cell-Level Event Detail
```sql
SELECT event_id, event_timestamp, risk_score, h3_ix
FROM main.silver.vave_eventsfeed
WHERE h3_ix = '8730062a9a7ffff'
  AND event_dt = '2025-05-11'
ORDER BY risk_score DESC;
```

## Setup & Configuration

### Prerequisites
* Databricks workspace with Unity Catalog enabled
* Catalog `main` with schemas: `bronze`, `silver`, `gold`
* Volume created: `/Volumes/main/bronze/raw_vave_files/`
* H3 library available (included in DBR 11.0+)

### Deployment Steps

1. **Create Unity Catalog Objects**
   ```sql
   CREATE CATALOG IF NOT EXISTS main;
   CREATE SCHEMA IF NOT EXISTS main.bronze;
   CREATE SCHEMA IF NOT EXISTS main.silver;
   CREATE SCHEMA IF NOT EXISTS main.gold;
   CREATE VOLUME IF NOT EXISTS main.bronze.raw_vave_files;
   ```

2. **Deploy Bronze Layer** (Streaming Table)
   * Run bronze layer creation SQL
   * Verify Auto Loader checkpoint location

3. **Deploy Silver Layer** (MERGE logic)
   * Run silver layer creation SQL
   * Verify H3 UDF availability

4. **Deploy Gold Layer** (Materialized Views)
   * Create `daily_risk_cell` materialized view
   * Create `daily_risk_zone` materialized view
   * Verify auto-refresh triggers

5. **Upload Test Data**
   * Upload sample CSV to volume
   * Validate end-to-end flow
   * Run data quality notebook

## Monitoring & Operations

### Health Checks
* Run `Vave_Data_Quality.ipynb` notebook daily
* Monitor materialized view refresh status
* Track ingestion lag (Bronze → Silver → Gold)
* Alert on data quality score drops

### Maintenance
* **OPTIMIZE:** Run weekly on silver/gold tables for performance
* **VACUUM:** Retain 7 days, run monthly for cost optimization
* **Materialized Views:** Automatic - no manual refresh needed

### Troubleshooting
* **Missing H3 indexes:** Check coordinate null rates in bronze
* **Stale Gold data:** Verify materialized view refresh status
* **Ingestion delays:** Check Auto Loader checkpoint health
* **Duplicate events:** Review silver MERGE predicate logic

## Project Structure

```
Vave_Solution/
├── README.md (this file)
├── notebooks/
│   ├── Vave_Data_Quality.ipynb       # Data quality validation
│   └── SETUP_GUIDE.ipynb             # Pipeline setup instructions
└── (SQL scripts for table creation)
```

## Future Enhancements

* **Real-time Alerting:** Integrate with Databricks SQL alerts for threshold breaches
* **Machine Learning:** Train risk prediction models on historical patterns
* **Advanced Spatial Analytics:** H3 neighbor analysis, radius queries
* **Dashboard Integration:** Lakeview dashboards with geographic visualizations
* **Data Retention Policies:** Automated archival of aged data
* **Multi-Resolution H3:** Support multiple H3 resolutions for zoom levels

## Contact & Support

For questions or issues with the Vave pipeline, please refer to the SETUP_GUIDE notebook or data quality validation results.

---

**Last Updated:** 2026-05-11  
**Pipeline Version:** 1.0  
**Databricks Runtime:** DBR 11.0+ (H3 support required)