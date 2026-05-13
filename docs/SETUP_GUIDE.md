---
jupyter:
  application/vnd.databricks.v1+notebook:
    language: python
    notebookMetadata:
      mostRecentlyExecutedCommandWithImplicitDF:
        commandId: 8816845156231689
        dataframes:
        - \_sqldf
      pythonIndentUnit: 4
    notebookName: SETUP_GUIDE
    widgets: {}
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
  language_info:
    name: python
  nbformat: 4
  nbformat_minor: 0
---

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"1b4c27de-636a-4160-bde7-ec225209e424\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
# Vave Pipeline - Setup Guide

## Overview

This guide explains how to set up and run the Vave Geospatial Risk Aggregation Pipeline.

## Prerequisites

1.  Unity Catalog `main` with appropriate permissions
2.  Access to `/Workspace/Repos/vave` Git repository
3.  Sample CSV files from `sample_data` folder in Git repo
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"4d3413ee-2758-44fd-a532-cd6001bd2eac\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
## Step 2: Upload Sample CSV Files

**IMPORTANT**: Files must be **manually uploaded** from the Git repository.

### File Location in Git Repo

repo_root/ `<br>`{=html}
└── sample_data/ `<br>`{=html}

- ├── vave_events_2026-04-20.csv
- ├── vave_events_2026-05-01.csv
- └── vave_events_2026-05-05.csv

### Upload Destination

**Volume Path**: `/Volumes/main/_raw/external_data/vave_raw/`

### Upload Method

**Databricks UI**:

1.  Navigate to **Data** → **Volumes**
2.  Browse to `main` → `_raw` → `external_data` → `vave_raw`
3.  Click **Upload**
4.  Select all 3 CSV files from `sample_data/` folder
5.  Confirm upload

Expected CSV Format:
event_id,event_timestamp,latitude,longitude,risk_score`<br>`{=html}
EVT001,2026-04-20 08:15:23,51.5074,-0.1278,45.3`<br>`{=html}
EVT002,2026-04-20 09:22:11,51.5155,-0.1426,62.8
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"232404fb-3905-49bd-90ba-33ef63a29ffa\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
## Step 3: Verify File Upload

**Action**: Run this command to verify files are in the volume:
:::

::: {.cell .code execution_count="0" application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"0575f16f-916c-45b1-9efb-de5c26acce45\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
``` python
# In Databricks notebook
display(dbutils.fs.ls("/Volumes/main/_raw/external_data/vave_raw/"))
```
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"7808cf1c-7106-4e81-a957-1f7ed04b7747\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
Expected output: 3 CSV files listed
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"96891635-c13d-45a1-8248-082ffeb2aa7d\",\"showTitle\":true,\"tableResultSettingsMap\":{},\"title\":\"Step 4: Run Bronze Ingestion\"}"}
## Step 4: Run Bronze Ingestion

**Notebook**: `Vave_Bronze_dbt`

**Purpose**: Processes CSV files from volume into Bronze Delta table.

**Action**:

1.  Open `Vave_Bronze_dbt` notebook
2.  Set widget parameter: `fil_pth_str` = Comma-separated file paths

**Example**:`<br>`{=html}
/Volumes/main/\_raw/external_data/vave_raw/vave_events_2026-04-20.csv,`<br>`{=html}/Volumes/main/\_raw/external_data/vave_raw/vave_events_2026-05-01.csv,`<br>`{=html}/Volumes/main/\_raw/external_data/vave_raw/vave_events_2026-05-05.csv

1.  Click **Run All**
2.  Verify records inserted into `main.bronze.vave_eventsfeed`

**Expected Output**:
=== Bronze Ingestion - dbt Triggered === `<br>`{=html}📊 Target Table: main.bronze.vave_eventsfeed `<br>`{=html}📝 File Type: csv `<br>`{=html}📁 Files to Process: 3 `<br>`{=html}✓ Inserted 190 records `<br>`{=html}✓ Processed 3 files

**Note**: The notebook tracks processed files in `main.control.processed_files` to prevent duplicate processing.
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"827c9e11-7cdf-46d5-aaea-6a568be842ff\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
## Step 5: Transform to Silver Layer

**Purpose**: Enrich Bronze data with H3 geospatial indexing.

**Action**: Run this SQL query in a notebook or SQL editor:
:::

::: {.cell .code execution_count="0" application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"60a2ff3e-4cb0-4cee-8001-476dc1047f90\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
``` python
%sql
INSERT INTO main.silver.vave_eventsfeed
SELECT
  event_id,
  event_timestamp,
  latitude,
  longitude,
  risk_score,
  h3_latlng_to_cell_string(latitude, longitude, 8) AS h3_ix,
  DATE(event_timestamp) AS event_dt,
  source_file_name,
  ingestion_ts
FROM main.bronze.vave_eventsfeed
WHERE event_id NOT IN (SELECT event_id FROM main.silver.vave_eventsfeed);
```
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"c3bf854a-e0d1-4918-9772-98d232356cc2\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
What This Does:

- Adds H3 geospatial index at resolution 8 (\~0.46 km hexagon edge)
- Extracts date from timestamp for partitioning
- Prevents duplicate records

Verify:
:::

::: {.cell .code execution_count="0" application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"1ba7717e-9bbc-4432-be8e-04615c744207\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
``` python
%sql
SELECT COUNT(*) FROM main.silver.vave_eventsfeed;
-- Expected: 190 records

SELECT DISTINCT h3_ix FROM main.silver.vave_eventsfeed;
-- Expected: ~57 unique H3 cells
```
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"182b8391-1c0d-41ce-b5a5-b571f7bc2e62\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
## Step 6: Refresh Gold Materialized Views

**Purpose**: Aggregate Silver data into analytics-ready Gold tables.

**Action**:
:::

::: {.cell .code execution_count="0" application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"055b28b8-30ba-4031-a659-207fb52b88cf\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
``` python
%sql
-- Refresh both gold materialized views
REFRESH MATERIALIZED VIEW main.gold.daily_risk_cell;
REFRESH MATERIALIZED VIEW main.gold.daily_risk_zone;
```
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"c01b076f-d865-4826-8cf8-b4cfc1d8c8e4\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
Verify:
:::

::: {.cell .code execution_count="0" application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"77728ae8-ddfa-4aa5-99eb-2edf06afed5c\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
``` python
%sql
SELECT * FROM main.gold.daily_risk_cell
ORDER BY event_dt DESC, total_risk_score DESC
LIMIT 10;
```
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"9a0fc65e-99d1-4b0e-b892-5bd3c7706fb1\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
Expected Output:

- Daily risk aggregations by H3 cell
- Multiple dates from sample data (April 20 - May 9, 2026)
- 57 unique H3 cells in London area
- Total of 190 events
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"43f9b17a-624c-4df5-857c-a599c16d17f7\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
## Step 7: View Dashboard

**Dashboard**: Vave Geospatial Risk Analysis NM

**Widgets**:

1.  **Total Events** counter: 190
2.  **Unique H3 Cells** counter: 57
3.  **Average Risk Score** counter: 47.32
4.  **Risk Trend Over Time**: Line chart showing daily total risk scores
5.  **Top 10 Highest Risk Cells**: Bar chart of H3 cells with highest cumulative risk
6.  **Top 20 Risk Records**: Table showing detailed records

**Data Sources**:

- `main.gold.daily_risk_cell`
- `main.gold.daily_risk_zone`

**Access**: Navigate to Dashboards in workspace or use published dashboard URL
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"446fe235-4a12-48eb-9f41-5d8d7abb4686\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
## Troubleshooting

### Files Not Processing

**Issue**: Files uploaded but not appearing in Bronze table

**Solution**:

1.  Verify files in volume:
:::

::: {.cell .code execution_count="0" application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"dd0b8d79-374b-4995-a542-e2d9259fb199\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
``` python
dbutils.fs.ls("/Volumes/main/_raw/external_data/vave_raw/")
```
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"adcb84e8-452c-446f-a140-3d6680701b61\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
2 .Check widget parameter `fil_pth_str` is set correctly in Bronze notebook
`<br>`{=html}

1.  Verify files not already in control table:
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"e7ffbcb9-20d7-47d9-94d0-ebf20a9f0044\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
# Vave Pipeline - Setup Guide {#vave-pipeline---setup-guide}

## H3 Function Not Found

Issue: `h3_latlng_to_cell_string` function error

Solution: Ensure Databricks Runtime 13.3 LTS or higher (H3 functions built-in)

## Gold Views Empty

Issue: Gold materialized views show no data
Solution:

1.  Verify Silver table has data
:::

::: {.cell .code execution_count="0" application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"ee537f31-635d-46aa-b87c-d79bced9f0a5\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
``` python
%sql
SELECT COUNT(*) FROM main.silver.vave_eventsfeed;
```
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"a5373cce-d9fa-4e75-a741-ee7c5991e826\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
1.  Refresh materialized views manually
2.  Check H3 index column populated:
:::

::: {.cell .code execution_count="0" application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"676dd608-858b-48f1-b8e6-80a2d08523b1\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
``` python
%sql
SELECT h3_ix FROM main.silver.vave_eventsfeed WHERE h3_ix IS NULL;
```
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"cb172607-a781-4464-b56b-c97a7d828b42\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
## Dashboard Shows Old Data

Issue: Dashboard not reflecting recent data

Solution:

1.  Refresh materialized views
2.  Refresh dashboard page
:::

::: {.cell .markdown application/vnd.databricks.v1+cell="{\"cellMetadata\":{},\"inputWidgets\":{},\"nuid\":\"e0de0f45-209f-4c90-ae15-65d05b51b296\",\"showTitle\":false,\"tableResultSettingsMap\":{},\"title\":\"\"}"}
## Daily Production Workflow

**Batch Schedule** (Recommended: Daily at 2 AM):

1.  **File Arrival**: CSV files land in volume throughout previous day
2.  **Bronze Ingestion**: Scheduled job runs `Vave_Bronze_dbt` notebook
    - Discovers new files
    - Processes only unprocessed files (idempotent)
    - Updates control table
3.  **Silver Transformation**: Scheduled job enriches with H3 indexing
4.  **Gold Refresh**: Materialized views refresh
5.  **Dashboard Update**: Dashboard displays updated metrics

**Orchestration Options**:

- **Databricks Jobs** (recommended) - Native scheduling
- **dbt Cloud** - Data transformation orchestration
- **Apache Airflow** - Complex workflow management
- **Azure Data Factory** - Azure-native orchestration

**No Auto-Trigger**: Files landing in volume do NOT automatically trigger pipeline. Manual or scheduled execution required.

------------------------------------------------------------------------

## Migration to Vave Workspace

**Target Structure**: `/Workspace/Repos/vave/notebooks/`

**Essential Files**:

1.  `Vave_Config_Params.py` - Shared configuration
2.  `Vave_Setup.py` - Infrastructure setup
3.  `Vave_Bronze_Ingestion.py` - Main ingestion notebook

**Migration Steps**:

1.  Export all 3 notebooks as `.py` source files
2.  Commit to Git repository at `/Workspace/Repos/vave/notebooks/`
3.  Update `%run` paths to relative paths (e.g., `./Vave_Config_Params`)
4.  Run setup notebook in new workspace
5.  Test with sample files

**Dashboard Migration**:

- Export as JSON from current workspace
- Import to Vave workspace
- Reconnect datasets to Unity Catalog tables
:::
