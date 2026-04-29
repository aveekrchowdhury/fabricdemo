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

# # 01 · POS Bronze Ingest — Landing → Bronze Delta Table
# 
# **Purpose:** Read the raw POS pipe-delimited file(s) from the landing zone and write every line,
# unchanged, into a Bronze Delta table.  The table is append-only so it acts as an immutable audit
# record of exactly what was received.
# 
# ### POS File Structure (from `POS_20260309.csv`)
# | Row | Marker | Description |
# |-----|--------|-------------|
# | 1 (HEADER) | `+` | `+\|1\|1\|<expected_col_count>\|*\|<file_date>\|<load_type>\|RWB\|1` |
# | 2 … N-1 (DATA) | `A/R/M/F/…` | 48 fields · 47 pipes (standard) or 49 fields · 48 pipes (extra-date variant) |
# | N (FOOTER) | `+` | `+\|9\|9999999\|<expected_col_count>\|9\|<file_date>\|<load_type>\|RWB\|1\|<expected_rows>\|<processed>\|<flag1>\|<flag2>` |
# 
# > **Design principle:** Bronze never fixes or rejects data. Every line is stored as received.
# > Duplicate loads are supported — the Current Record Indicator notebook (NB 03) resolves them.


# PARAMETERS CELL ********************

file_name=''
name_filter= 'POS'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── PARAMETERS ──────────────────────────────────────────────────────────────────
# Set these via Fabric Pipeline parameters when running in production.

# Full path to the landing folder or single file in OneLake / ADLS shortcut.
# Examples:
#   "Files/landing/beta/pos/2026/03/09/"
#   "Files/landing/beta/pos/POS_20260309.csv"
landing_path: str = "Files/bairddemofiles/landing/beta/pos/"+file_name         # REQUIRED

# Optional: only process files whose name contains this string
name_filter: str = "POS"

# Bronze Delta table name (written to the attached Lakehouse)
bronze_raw_table: str = "pos_bronze_raw"

# Delimiter
delimiter: str = "|"

# If True the first line is HEADER and the last line is FOOTER (Beta standard)
assume_header_footer: bool = True

# Append (True = audit-friendly, recommended) or Overwrite
append_mode: bool = True

# Override batch_id (leave blank → auto-generated from filename date or timestamp)
batch_id_override: str = ""

print("landing_path  :", landing_path)
print("bronze_table  :", bronze_raw_table)
print("append_mode   :", append_mode)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── FILE DISCOVERY ──────────────────────────────────────────────────────────────
import re

def list_landing_files(path: str, name_filter: str) -> list:
    """Return a list of file paths under *path* that match *name_filter*."""
    try:
        items = mssparkutils.fs.ls(path)
        paths = [i.path for i in items if not i.isDir]
    except Exception:
        # Fallback: treat landing_path as a single file path
        paths = [path]
    if name_filter:
        paths = [p for p in paths if name_filter in p]
    return paths

files = list_landing_files(landing_path, name_filter)

if not files or files == [""]:
    raise ValueError(f"No files found at '{landing_path}'. Set landing_path correctly.")

print(f"Files to ingest ({len(files)}):")
for f in files:
    print(" →", f)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── INGEST RAW LINES ────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def extract_batch_id(file_path: str, override: str) -> str:
    """Derive batch_id from filename date (POS_YYYYMMDD.csv) or fall back to timestamp."""
    if override:
        return override
    m = re.search(r"(\d{8})", file_path)
    return m.group(1) if m else datetime.utcnow().strftime("%Y%m%d%H%M%S")

from datetime import datetime

all_dfs = []
for fp in files:
    bid = extract_batch_id(fp, batch_id_override)
    fname = fp.split("/")[-1]

    raw = (
        spark.read.text(fp)
        .withColumnRenamed("value", "line_text")
        .withColumn("file_path",  F.lit(fp))
        .withColumn("file_name",  F.lit(fname))
        .withColumn("batch_id",   F.lit(bid))
        .withColumn("ingest_ts",  F.current_timestamp())
        .withColumn("_row_idx",   F.monotonically_increasing_id())
    )
    all_dfs.append(raw)

combined = all_dfs[0]
for df in all_dfs[1:]:
    combined = combined.unionByName(df)

# ── Assign stable row numbers per file ──────────────────────────────────────────
w_file = Window.partitionBy("file_path", "batch_id").orderBy("_row_idx")
combined = combined.withColumn("row_num", F.row_number().over(w_file))

# ── Total rows per file (to identify last row = FOOTER) ─────────────────────────
w_cnt = Window.partitionBy("file_path", "batch_id")
combined = combined.withColumn("total_lines", F.max("row_num").over(w_cnt))

# ── Tag HEADER / FOOTER / DATA ───────────────────────────────────────────────────
if assume_header_footer:
    combined = combined.withColumn(
        "section",
        F.when(F.col("row_num") == 1,                          F.lit("HEADER"))
         .when(F.col("row_num") == F.col("total_lines"),       F.lit("FOOTER"))
         .otherwise(                                            F.lit("DATA"))
    )
else:
    combined = combined.withColumn("section", F.lit("DATA"))

# ── Pipe count per line (key validation signal) ──────────────────────────────────
combined = combined.withColumn(
    "pipe_count",
    F.length("line_text") - F.length(F.regexp_replace("line_text", r"\|", ""))
)

# ── Parse file_date and load_type from HEADER line ──────────────────────────────
# Header format: +|1|1|<col_count>|*|<YYYYMMDD>|<load_type>|RWB|1
header_df = (
    combined.filter(F.col("section") == "HEADER")
    .withColumn("_hfields", F.split("line_text", r"\|"))
    .withColumn("file_date",  F.element_at(F.col("_hfields"), 6))   # index 5 (1-based = 6)
    .withColumn("load_type",  F.element_at(F.col("_hfields"), 7))   # index 6 (1-based = 7)
    .select("file_path", "batch_id", "file_date", "load_type")
)

bronze_df = (
    combined
    .join(header_df, on=["file_path", "batch_id"], how="left")
    .select(
        "file_path", "file_name", "batch_id", "file_date", "load_type",
        "ingest_ts", "row_num", "section", "pipe_count", "line_text"
    )
)

print(f"Total lines staged: {bronze_df.count()}")
bronze_df.groupBy("section").count().show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── WRITE TO BRONZE DELTA TABLE ─────────────────────────────────────────────────
write_mode = "append" if append_mode else "overwrite"

(
    bronze_df
    .write
    .format("delta")
    .mode(write_mode)
    .saveAsTable(bronze_raw_table)
)

print(f"✅  Written to '{bronze_raw_table}' (mode={write_mode})")


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
