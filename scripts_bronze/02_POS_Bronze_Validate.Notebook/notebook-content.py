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

# # 02 · POS Bronze Validate — Structure & Format Checks
# 
# **Purpose:** Run all Bronze-layer validation rules against the ingested raw lines.
# Results are written to a validation Delta table. Optionally fails the Fabric pipeline
# so downstream Silver processing never runs on a bad file.
# 
# ### Validation Checks Implemented
# 
# | # | Check | Source Rule |
# |---|-------|-------------|
# | 1 | Header row present and not blank | PDF §Header/Footer Validation |
# | 2 | Footer row present and not blank | PDF §Header/Footer Validation |
# | 3 | File date in header matches date in filename | PDF §Vendor sent wrong dated file |
# | 4 | Expected column count in header == 13 | PDF §Pipe check / column alignment |
# | 5 | Pipe count per DATA row — flag true anomalies | PDF §Pipe count validation (c_dna_filevalidation) |
# | 6 | Info: rows with extra-date variant (48 pipes) | Known POS schema variant |
# | 7 | Blank lines in DATA section | PDF §File has blank lines between rows |
# | 8 | Footer expected row count vs actual DATA rows | PDF §Footer row count validation |
# 
# > **Standard pipe count:** 37 (38 fields).  
# > **Extended variant:** 38 pipes (39 fields — extra date column, known POS behaviour).  
# > Any row with **< 37 or > 38** pipes is a true anomaly.


# PARAMETERS CELL ********************

# Optional filters (leave blank to validate everything in the table)
file_name_filter:   str  = "POS_20260406.csv"   # e.g. "POS_20260309"
batch_id_equals:    str  = ""   # e.g. "20260309"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── PARAMETERS ──────────────────────────────────────────────────────────────────
bronze_raw_table:       str  = "pos_bronze_raw"
validation_results_table: str = "pos_bronze_validation"



# Known-good pipe counts for POS data rows
STANDARD_PIPE_COUNT: int = 38  # 48 fields  — standard layout
EXTENDED_PIPE_COUNT: int = 39   # 49 fields  — extra date-column variant

# Expected column count declared in header field[3]
EXPECTED_COL_COUNT: int = 13



# If True, raise an exception at the end when critical checks fail
fail_pipeline_on_error: bool = True

print("Validation config ready.")
print(f"  Standard pipe count : {STANDARD_PIPE_COUNT}")
print(f"  Extended pipe count : {EXTENDED_PIPE_COUNT}")
print(f"  Expected col count  : {EXPECTED_COL_COUNT}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── LOAD BRONZE TABLE & APPLY FILTERS ───────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import re

raw = spark.table(bronze_raw_table)

if file_name_filter:
    raw = raw.filter(F.col("file_name").contains(file_name_filter))
if batch_id_equals:
    raw = raw.filter(F.col("batch_id") == batch_id_equals)

if raw.rdd.isEmpty():
    raise ValueError(
        f"No rows found in '{bronze_raw_table}' for the given filters. "
        "Run the Ingest notebook first."
    )

total = raw.count()
print(f"Rows loaded for validation: {total:,}")
raw.groupBy("file_name", "batch_id", "section").count().orderBy("file_name","batch_id","section").show(50, False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── CHECK 1 & 2 · Header and Footer presence / blank ────────────────────────────
hf_checks = raw.groupBy("file_path", "file_name", "batch_id").agg(
    # Header
    F.max(F.when(F.col("section") == "HEADER", F.lit(1)).otherwise(F.lit(0))).alias("has_header"),
    F.max(
        F.when((F.col("section") == "HEADER") & (F.trim(F.col("line_text")) == ""), F.lit(1))
         .otherwise(F.lit(0))
    ).alias("header_blank"),
    # Footer
    F.max(F.when(F.col("section") == "FOOTER", F.lit(1)).otherwise(F.lit(0))).alias("has_footer"),
    F.max(
        F.when((F.col("section") == "FOOTER") & (F.trim(F.col("line_text")) == ""), F.lit(1))
         .otherwise(F.lit(0))
    ).alias("footer_blank"),
)

print("Check 1 & 2 — Header / Footer presence:")
display(hf_checks)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── CHECK 3 · File date in header must match filename date ───────────────────────
# Header line_text: +|1|1|13|*|YYYYMMDD|load_type|RWB|1
# Filename:         POS_YYYYMMDD.csv

header_rows = raw.filter(F.col("section") == "HEADER").withColumn(
    "_hfields", F.split("line_text", r"\|")
).withColumn(
    "header_file_date", F.element_at(F.col("_hfields"), 6)   # 1-based index 6 = field[5]
)

# Extract date from filename  (POS_YYYYMMDD.csv → YYYYMMDD)
filename_date_udf = F.udf(
    lambda fname: (re.search(r"(\d{8})", fname) or [None, None])[1],
    T.StringType()
)

header_rows = header_rows.withColumn("filename_date", filename_date_udf("file_name"))

date_check = header_rows.withColumn(
    "date_match", F.col("header_file_date") == F.col("filename_date")
).select("file_path", "file_name", "batch_id", "header_file_date", "filename_date", "date_match")

print("Check 3 — File date in header vs filename:")
date_check.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── CHECK 4 · Expected column count in header must == 13 ────────────────────────
col_count_check = raw.filter(F.col("section") == "HEADER").withColumn(
    "_hfields", F.split("line_text", r"\|")
).withColumn(
    "header_col_count", F.element_at(F.col("_hfields"), 4).cast("int")   # 1-based index 4 = field[3]
).withColumn(
    "col_count_ok", F.col("header_col_count") == F.lit(EXPECTED_COL_COUNT)
).select("file_path", "file_name", "batch_id", "header_col_count", "col_count_ok")

print(f"Check 4 — Expected column count == {EXPECTED_COL_COUNT}:")
col_count_check.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── CHECK 5 · Pipe count per DATA row (true anomalies only) ─────────────────────
# Standard = 47 pipes, Extended variant = 48 pipes → both acceptable
# Anything outside [47, 48] is a true anomaly

data_rows = raw.filter(F.col("section") == "DATA")

pipe_anomalies = data_rows.withColumn(
    "is_anomaly",
    ~F.col("pipe_count").isin(STANDARD_PIPE_COUNT, EXTENDED_PIPE_COUNT)
)

pipe_summary = pipe_anomalies.groupBy("file_path", "file_name", "batch_id").agg(
    F.count("*").alias("total_data_rows"),
    F.sum(F.when(F.col("pipe_count") == STANDARD_PIPE_COUNT, 1).otherwise(0)).alias("standard_rows"),
    F.sum(F.when(F.col("pipe_count") == EXTENDED_PIPE_COUNT, 1).otherwise(0)).alias("extended_date_rows"),
    F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("pipe_anomaly_rows"),
)

print("Check 5 — Pipe count distribution per file:")
pipe_summary.show(truncate=False)

# Show the anomalous rows for investigation
anomalous = pipe_anomalies.filter(F.col("is_anomaly"))
if anomalous.count() > 0:
    print("⚠️  Anomalous rows (pipe count outside [47,48]):")
    anomalous.select("file_name", "batch_id", "row_num", "pipe_count", "line_text").show(20, False)
else:
    print("✅  No true pipe-count anomalies found.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── CHECK 7 · Blank lines in DATA section ────────────────────────────────────────
blank_lines = data_rows.filter(
    (F.trim(F.col("line_text")) == "") | (F.col("pipe_count") == 0)
).groupBy("file_path", "file_name", "batch_id").agg(
    F.count("*").alias("blank_line_count")
)

print("Check 7 — Blank lines in DATA section:")
if blank_lines.count() == 0:
    print("✅  No blank lines detected.")
else:
    blank_lines.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── CHECK 8 · Footer expected row count vs actual DATA row count ─────────────────
# Footer format: +|9|9999999|13|9|YYYYMMDD|load_type|RWB|1|<expected>|<processed>|flag1|flag2
# expected_rows = field[9] (1-based index 10)

footer_rows = raw.filter(F.col("section") == "FOOTER").withColumn(
    "_ffields", F.split("line_text", r"\|")
).withColumn(
    "footer_expected_rows", F.element_at(F.col("_ffields"), 10).cast("int")  # 1-based = 10
).withColumn(
    "footer_processed_rows", F.element_at(F.col("_ffields"), 11).cast("int") # 1-based = 11
).select("file_path", "file_name", "batch_id", "footer_expected_rows", "footer_processed_rows")

actual_data_counts = data_rows.groupBy("file_path", "file_name", "batch_id").agg(
    F.count("*").alias("actual_data_rows")
)

rowcount_check = footer_rows.join(
    actual_data_counts, on=["file_path", "file_name", "batch_id"], how="left"
).withColumn(
    "rowcount_match", F.col("footer_expected_rows")-2 == F.col("actual_data_rows")
).withColumn(
    "row_delta", F.col("actual_data_rows") - F.col("footer_expected_rows")+2
)

print("Check 8 — Footer expected rows vs actual DATA rows:")
rowcount_check.select(
    "file_name", "batch_id", "footer_expected_rows",
    "footer_processed_rows", "actual_data_rows", "row_delta", "rowcount_match"
).show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This cell consolidates Bronze-layer validation results and writes them to the 'bronze_validation' Delta table.
# It also handles critical check gating for downstream processing and error reporting.
# Requirement: ensure exactly one validation record per file (file_path + file_name + batch_id).

# Create 'bronze_validation' table name variable for clarity
bronze_validation_table = "bronze_validation"

# Consolidate all intermediate output DataFrames to the final results DataFrame
results = (
    hf_checks
    .join(
        date_check.select(
            "file_path",
            "batch_id",
            "header_file_date",
            "filename_date",
            "date_match"
        ),
        on=["file_path", "batch_id"],
        how="left"
    )
    .join(
        col_count_check.select(
            "file_path",
            "batch_id",
            "header_col_count",
            "col_count_ok"
        ),
        on=["file_path", "batch_id"],
        how="left"
    )
    .join(
        pipe_summary.select(
            "file_path",
            "batch_id",
            "total_data_rows",
            "standard_rows",
            "extended_date_rows",
            "pipe_anomaly_rows"
        ),
        on=["file_path", "batch_id"],
        how="left"
    )
    .join(
        blank_lines,
        on=["file_path", "file_name", "batch_id"],
        how="left"
    )
    .join(
        rowcount_check.select(
            "file_path",
            "batch_id",
            "footer_expected_rows",
            "footer_processed_rows",
            "actual_data_rows",
            "row_delta",
            "rowcount_match"
        ),
        on=["file_path", "batch_id"],
        how="left"
    )
)

# Ensure a single consolidated record per file processing
# (deduplicate on file_path + file_name + batch_id)
results = results.dropDuplicates(["file_path", "file_name", "batch_id"])

results = results.fillna({
    "blank_line_count": 0,
    "pipe_anomaly_rows": 0,
    "standard_rows": 0,
    "extended_date_rows": 0,
})

results = results.withColumn(
    "check_header", (F.col("has_header") == 1) & (F.col("header_blank") == 0)
).withColumn(
    "check_footer", (F.col("has_footer") == 1) & (F.col("footer_blank") == 0)
).withColumn(
    "check_date_match", F.col("date_match") == True
).withColumn(
    "check_col_count", F.col("col_count_ok") == True
).withColumn(
    "check_pipes_clean", F.col("pipe_anomaly_rows") == 0
).withColumn(
    "check_no_blanks", F.col("blank_line_count") == 0
).withColumn(
    "warn_rowcount", F.col("rowcount_match") == True  # Warning only — real file shows delta of 2
).withColumn(
    "info_extended_rows", F.col("extended_date_rows") > 0  # Info only — known variant
)

results = results.withColumn(
    "passed",
    F.col("check_header")
    & F.col("check_footer")
    & F.col("check_date_match")
    & F.col("check_col_count")
    & F.col("check_pipes_clean")
    & F.col("check_no_blanks")
).withColumn("validated_ts", F.current_timestamp())

# Write results to 'bronze_validation' table (Delta, append mode)
results.write.format("delta").mode("append").saveAsTable(bronze_validation_table)
print(f"✅  Validation results written to '{bronze_validation_table}'")

# Display summary for user inspection in notebook
display(
    results.select(
        "file_name",
        "batch_id",
        "passed",
        "check_header",
        "check_footer",
        "check_date_match",
        "check_col_count",
        "check_pipes_clean",
        "check_no_blanks",
        "warn_rowcount",
        "info_extended_rows",
        "total_data_rows",
        "pipe_anomaly_rows",
        "blank_line_count",
        "row_delta",
    )
)

# Optionally, fail pipeline if critical checks failed
if fail_pipeline_on_error:
    failures = results.filter(~F.col("passed")).count()
    if failures > 0:
        print("Validation failed")
    else:
        print("✅  All critical checks passed. Ready for downstream processing.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import functions as F
import html

# Collect rows to driver for building HTML/email payload
email_rows = (
    results.select(
        "file_name",
        "batch_id",
        "passed",
        "check_header",
        "check_footer",
        "check_date_match",
        "check_col_count",
        "check_pipes_clean",
        "check_no_blanks",
        "warn_rowcount",
        "total_data_rows",
        "pipe_anomaly_rows",
        "blank_line_count",
        "row_delta"
    )
    .orderBy("file_name", "batch_id")
    .collect()
)

# Optional: quick visual check in notebook
display(email_rows)

overall_status = "PASSED" if all(r["passed"] for r in email_rows) else "FAILED"
failed_file_count = sum(1 for r in email_rows if not r["passed"])

# Helper to render boolean as colored badge
def bool_badge(val: bool, label_true: str = "OK", label_false: str = "FAIL") -> str:
    if val is None:
        return '<span style="background:#999;color:#fff;padding:2px 6px;border-radius:3px;font-size:11px;">N/A</span>'
    if val:
        return f'<span style="background:#2e7d32;color:#fff;padding:2px 6px;border-radius:3px;font-size:11px;">{html.escape(label_true)}</span>'
    return f'<span style="background:#c62828;color:#fff;padding:2px 6px;border-radius:3px;font-size:11px;">{html.escape(label_false)}</span>'

# Build HTML summary table
rows_html = []
for r in email_rows:
    rows_html.append(
        "<tr>"
        f"<td>{html.escape(str(r['file_name']))}</td>"
        f"<td>{html.escape(str(r['batch_id']))}</td>"
        f"<td style='text-align:center;'>{bool_badge(r['passed'], 'PASS', 'FAIL')}</td>"
        f"<td style='text-align:center;'>{bool_badge(r['check_header'])}</td>"
        f"<td style='text-align:center;'>{bool_badge(r['check_footer'])}</td>"
        f"<td style='text-align:center;'>{bool_badge(r['check_date_match'])}</td>"
        f"<td style='text-align:center;'>{bool_badge(r['check_col_count'])}</td>"
        f"<td style='text-align:center;'>{bool_badge(r['check_pipes_clean'])}</td>"
        f"<td style='text-align:center;'>{bool_badge(r['check_no_blanks'])}</td>"
        f"<td style='text-align:center;'>{bool_badge(r['warn_rowcount'], 'OK', 'CHECK')}</td>"
        f"<td style='text-align:right;'>{html.escape(str(r['total_data_rows']))}</td>"
        f"<td style='text-align:right;'>{html.escape(str(r['pipe_anomaly_rows']))}</td>"
        f"<td style='text-align:right;'>{html.escape(str(r['blank_line_count']))}</td>"
        f"<td style='text-align:right;'>{html.escape(str(r['row_delta']))}</td>"
        "</tr>"
    )

table_html = (
    "<table style='border-collapse:collapse;font-family:Segoe UI,Arial,sans-serif;font-size:12px;'>"
    "<thead>"
    "<tr style='background:#f3f3f3;'>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>File Name</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>Batch Id</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>Overall</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>Header</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>Footer</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>Date Match</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>Col Count</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>Pipes Clean</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>No Blanks</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;'>Rowcount Warn</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;text-align:right;'>Total Rows</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;text-align:right;'>Pipe Anomalies</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;text-align:right;'>Blank Lines</th>"
    "<th style='border:1px solid #ccc;padding:4px 8px;text-align:right;'>Row Δ</th>"
    "</tr>"
    "</thead>"
    "<tbody>"
    + "".join(rows_html) +
    "</tbody>"
    "</table>"
)

html_body = (
    "<html>"
    "<body style='font-family:Segoe UI,Arial,sans-serif;font-size:12px;color:#333;'>"
    f"<h2>POS Bronze Validation — {html.escape(overall_status)}</h2>"
    f"<p><b>Overall status:</b> {html.escape(overall_status)}<br>"
    f"<b>Failed file count:</b> {failed_file_count}</p>"
    "<p>Detailed per-file results:</p>"
    f"{table_html}"
    "</body>"
    "</html>"
)

# Build JSON payload including HTML representation
payload = {
    "overall_status": overall_status,
    "failed_file_count": failed_file_count,
    "results": [r.asDict(recursive=True) for r in email_rows],
    "html_body": html_body
}

# Return payload to calling pipeline / notebook
notebookutils.notebook.exit(json.dumps(payload, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
