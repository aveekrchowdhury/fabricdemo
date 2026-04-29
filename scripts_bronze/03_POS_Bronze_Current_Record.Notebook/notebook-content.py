# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1d610ecb-aa55-4c4e-be76-2335a5a874fb",
# META       "default_lakehouse_name": "bronzelh",
# META       "default_lakehouse_workspace_id": "f3a3d640-f8f5-4bf5-b208-eabda21ba6bb",
# META       "known_lakehouses": [
# META         {
# META           "id": "1d610ecb-aa55-4c4e-be76-2335a5a874fb"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 03 · POS Bronze Current Record Indicator — Manifest Table
# 
# **Purpose:** When a file is loaded more than once for the same batch date (accidentally or
# intentionally), mark the **latest load** as `current_record = 1`.
# 
# This implements the *Current Record Indicator* described in the CRG DnA ETL specification:
# 
# > *"Baird will use a 'Current Record Indicator' to identify the latest load of a file if it was
# > loaded accidentally or more than once for a batch load date."*
# 
# ### Outputs — `pos_bronze_manifest`
# | Column | Description |
# |--------|-------------|
# | `file_name` | Source filename |
# | `batch_id` | Batch identifier (derived from filename date) |
# | `file_date` | Date embedded in the file header |
# | `ingest_ts` | When this load was ingested |
# | `data_row_count` | Number of DATA rows in this load |
# | `footer_expected_rows` | Row count declared in footer |
# | `rowcount_delta` | Difference between actual and expected |
# | `current_record` | `1` = latest load · `0` = superseded load |
# | `rowcount_warning` | `True` if footer vs actual discrepancy exists |


# CELL ********************

# ── PARAMETERS ──────────────────────────────────────────────────────────────────
bronze_raw_table: str   = "pos_bronze_raw"
manifest_table:   str   = "pos_bronze_manifest"

# Resolve "current" by max ingest_ts (True) or lexicographic max batch_id (False)
use_ingest_ts: bool = True


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── BUILD MANIFEST ───────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window

raw = spark.table(bronze_raw_table)

# ── Per-load summary ─────────────────────────────────────────────────────────────
per_load = raw.groupBy("file_path", "file_name", "batch_id", "file_date", "load_type").agg(
    F.max("ingest_ts").alias("ingest_ts"),
    F.sum(F.when(F.col("section") == "DATA",   F.lit(1)).otherwise(F.lit(0))).alias("data_row_count"),
    F.sum(F.when(F.col("section") == "HEADER", F.lit(1)).otherwise(F.lit(0))).alias("header_count"),
    F.sum(F.when(F.col("section") == "FOOTER", F.lit(1)).otherwise(F.lit(0))).alias("footer_count"),
)

# ── Extract footer expected row count ────────────────────────────────────────────
footer_data = raw.filter(F.col("section") == "FOOTER").withColumn(
    "_ffields", F.split("line_text", r"\|")
).withColumn(
    "footer_expected_rows", F.element_at(F.col("_ffields"), 10).cast("int")
).select("file_path", "batch_id", "footer_expected_rows")

per_load = per_load.join(footer_data, on=["file_path", "batch_id"], how="left")

per_load = per_load.withColumn(
    "rowcount_delta", F.col("data_row_count")+2 - F.col("footer_expected_rows")
).withColumn(
    "rowcount_warning", F.col("rowcount_delta") != 0
)

# ── Apply Current Record Indicator ───────────────────────────────────────────────
if use_ingest_ts:
    w_cur = Window.partitionBy("file_name").orderBy(F.col("ingest_ts").desc())
else:
    w_cur = Window.partitionBy("file_name").orderBy(F.col("batch_id").desc())

manifest = per_load.withColumn(
    "current_record", (F.row_number().over(w_cur) == 1).cast("int")
)

# ── Write ─────────────────────────────────────────────────────────────────────────
(
    manifest
    .select(
        "file_name", "batch_id", "file_date", "load_type",
        "ingest_ts", "data_row_count", "footer_expected_rows",
        "rowcount_delta", "rowcount_warning", "current_record",
        "header_count", "footer_count"
    )
    .write.format("delta").mode("overwrite").saveAsTable(manifest_table)
)

print(f"✅  Manifest written to '{manifest_table}'")

# ── Display ───────────────────────────────────────────────────────────────────────
spark.table(manifest_table).orderBy("file_name", "ingest_ts").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── SURFACE WARNINGS ─────────────────────────────────────────────────────────────
manifest_df = spark.table(manifest_table)

dup_loads = manifest_df.filter(F.col("current_record") == 0)
if dup_loads.count() > 0:
    print("⚠️  Superseded (duplicate) loads detected:")
    dup_loads.select("file_name","batch_id","ingest_ts","data_row_count","current_record").show(False)
else:
    print("✅  No duplicate loads detected.")

rowcount_warnings = manifest_df.filter(F.col("rowcount_warning") == True)
if rowcount_warnings.count() > 0:
    print("⚠️  Files with footer row-count discrepancy (warning — not a pipeline failure):")
    rowcount_warnings.select(
        "file_name","batch_id","footer_expected_rows","data_row_count","rowcount_delta"
    ).show(False)
else:
    print("✅  All footer row counts match actual data row counts.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
