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

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import uuid

# =========================================================
# INPUTS
# =========================================================
file_path = "Files/bairddemofiles/samples/POS/POS_20260309.csv"     # Change as needed
source_file_name = "POS_20260309.csv"    # Change as needed

header_table = "header_table"
pos_table = "pos_table"
bad_table = "pos_bad_table"

# =========================================================
# BUSINESS COLUMNS FOR POS DETAIL RECORDS
# =========================================================
pos_columns = [
    "BA_RECCODE", "REP", "ACCT_NO", "SEC_NO", "ACCT_TYPE", "CTRL_NO", "SEQ_NO",
    "IOEX_IND", "SUB_NO", "FIRM_NO", "CRNCY_TYPE", "TRADE_CYMD", "SETTLE_CYMD",
    "ASOF_CYMD", "SUM_SEC_CODE", "SRCE_CODE", "PURCHASE_PRX", "CASH_AMT",
    "CHK_TODAY_AMT", "PSTN_QTY", "SEG_QTY", "LEG_QTY", "SAFEKEEP_FIRM_QTY",
    "SAFEKEEP_CUST_QTY", "TRAN_QTY", "PBOO_QTY", "DTOO_QTY", "DOC_QTY",
    "TRAN_CYMD", "BUY_SELL_IND", "MKT_PRX", "TOO_QTY", "MKT_VALUE_AMT",
    "USD_EQUIV_AMT", "SEG_REINVEST_QTY", "ADJUST_IND", "REINVEST_IND",
    "FREE_CR_IND", "USD_MKT_PRX", "CSI_OVRD_CODE", "CSI_ITEM_NO",
    "OPEN_CLOSE_IND", "NEW_1_SEC_TYPE"
]

expected_pos_col_count = len(pos_columns)

# Unique ID for this file load
file_load_id = str(uuid.uuid4())

# =========================================================
# OPTIONAL: CREATE TARGET TABLES IF NOT EXISTS
# =========================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {header_table} (
    file_load_id STRING,
    source_file_name STRING,
    source_file_path STRING,
    load_ts TIMESTAMP,
    header_line STRING,
    header_rec_type STRING,
    header_code_1 STRING,
    header_code_2 STRING,
    header_code_3 STRING,
    header_code_4 STRING,
    header_file_date STRING,
    header_file_desc STRING,
    header_source STRING,
    header_batch_no INT,
    footer_line STRING,
    footer_rec_type STRING,
    footer_file_date STRING,
    footer_file_desc STRING,
    footer_source STRING,
    footer_batch_no INT,
    footer_total_row_count INT,
    footer_detail_row_count INT,
    footer_error_count INT,
    footer_unused_count INT,
    expected_detail_count INT,
    actual_total_row_count INT,
    actual_detail_count INT,
    bad_record_count INT,
    load_status STRING,
    load_message STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bad_table} (
    file_load_id STRING,
    source_file_name STRING,
    source_file_path STRING,
    load_ts TIMESTAMP,
    raw_line STRING,
    reason STRING
)
USING DELTA
""")

# Create POS table only if it does not exist yet
if not spark.catalog.tableExists(pos_table):
    pos_schema_ddl = ",\n    ".join([f"{c} STRING" for c in pos_columns])
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {pos_table} (
        file_load_id STRING,
        source_file_name STRING,
        source_file_path STRING,
        load_ts TIMESTAMP,
        {pos_schema_ddl}
    )
    USING DELTA
    """)

# =========================================================
# IDEMPOTENCY CHECK
# Prevent loading same file again after successful load
# =========================================================
already_loaded = (
    spark.table(header_table)
    .filter(
        (F.col("source_file_name") == source_file_name) &
        (F.col("source_file_path") == file_path) &
        (F.col("load_status") == "SUCCESS")
    )
    .limit(1)
    .count()
)

if already_loaded > 0:
    raise Exception(f"File already loaded successfully: {source_file_name}")

# =========================================================
# READ RAW FILE AS TEXT
# =========================================================
raw_df = (
    spark.read.text(file_path)
    .withColumn("value", F.regexp_replace("value", "^\uFEFF", ""))  # remove BOM if present
)

# Add row number to identify header/footer/body
w = Window.orderBy(F.monotonically_increasing_id())
numbered_df = raw_df.withColumn("rn", F.row_number().over(w))

total_rows = numbered_df.agg(F.max("rn").alias("max_rn")).collect()[0]["max_rn"]

if total_rows is None or total_rows < 3:
    raise Exception(f"File has insufficient rows. total_rows={total_rows}")

# =========================================================
# EXTRACT HEADER / FOOTER / BODY
# =========================================================
header_line_df = (
    numbered_df
    .filter(F.col("rn") == 1)
    .select(F.col("value").alias("header_line"))
)

footer_line_df = (
    numbered_df
    .filter(F.col("rn") == total_rows)
    .select(F.col("value").alias("footer_line"))
)

body_df = (
    numbered_df
    .filter((F.col("rn") > 1) & (F.col("rn") < total_rows))
    .select(F.col("value").alias("raw_line"))
)
display(body_df)  



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# PARSE HEADER
# Assumes header layout is similar in structure to footer.
# Adjust if your real header layout differs.
# =========================================================
header_parsed_df = (
    header_line_df
    .withColumn("parts", F.split(F.col("header_line"), r"\|"))
    .select(
        F.lit(file_load_id).alias("file_load_id"),
        F.lit(source_file_name).alias("source_file_name"),
        F.lit(file_path).alias("source_file_path"),
        F.current_timestamp().alias("load_ts"),
        F.col("header_line"),
        F.col("parts")[0].alias("header_rec_type"),
        F.col("parts")[1].alias("header_code_1"),
        F.col("parts")[2].alias("header_code_2"),
        F.col("parts")[3].alias("header_code_3"),
        F.col("parts")[4].alias("header_code_4"),
        F.col("parts")[5].alias("header_file_date"),
        F.trim(F.col("parts")[6]).alias("header_file_desc"),
        F.trim(F.col("parts")[7]).alias("header_source"),
        F.when(F.col("parts")[8].rlike("^[0-9]+$"), F.col("parts")[8].cast("int"))
         .otherwise(F.lit(None).cast("int"))
         .alias("header_batch_no")
    )
)

# =========================================================
# PARSE FOOTER
# Based on your confirmed footer format:
# +|9|9999999|13|9|20260309|LOAD-FULL  : LEDS|RWB|1|74|72|0|0
#
# parts[9]  = total physical rows in file
# parts[10] = detail rows excluding header/footer
# =========================================================
footer_parsed_df = (
    footer_line_df
    .withColumn("parts", F.split(F.col("footer_line"), r"\|"))
    .select(
        F.col("footer_line"),
        F.col("parts")[0].alias("footer_rec_type"),
        F.col("parts")[5].alias("footer_file_date"),
        F.trim(F.col("parts")[6]).alias("footer_file_desc"),
        F.trim(F.col("parts")[7]).alias("footer_source"),
        F.when(F.col("parts")[8].rlike("^[0-9]+$"), F.col("parts")[8].cast("int"))
         .otherwise(F.lit(None).cast("int"))
         .alias("footer_batch_no"),
        F.when(F.col("parts")[9].rlike("^[0-9]+$"), F.col("parts")[9].cast("int"))
         .otherwise(F.lit(None).cast("int"))
         .alias("footer_total_row_count"),
        F.when(F.col("parts")[10].rlike("^[0-9]+$"), F.col("parts")[10].cast("int"))
         .otherwise(F.lit(None).cast("int"))
         .alias("footer_detail_row_count"),
        F.when(F.col("parts")[11].rlike("^[0-9]+$"), F.col("parts")[11].cast("int"))
         .otherwise(F.lit(None).cast("int"))
         .alias("footer_error_count"),
        F.when(F.col("parts")[12].rlike("^[0-9]+$"), F.col("parts")[12].cast("int"))
         .otherwise(F.lit(None).cast("int"))
         .alias("footer_unused_count")
    )
    .withColumn("expected_detail_count", F.col("footer_detail_row_count"))
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CLASSIFY BODY ROWS
# Valid POS rows start with A| and must have exact column count
# =========================================================
body_with_parts_df = body_df.withColumn("parts", F.split(F.col("raw_line"), r"\|"))


good_pos_df = (
    body_with_parts_df
    .filter(F.col("raw_line").startswith("A|"))
   
)

bad_pos_df = (
    body_with_parts_df
    .filter(
        (~F.col("raw_line").startswith("A|"))
       
    )
    .select(
        F.lit(file_load_id).alias("file_load_id"),
        F.lit(source_file_name).alias("source_file_name"),
        F.lit(file_path).alias("source_file_path"),
        F.current_timestamp().alias("load_ts"),
        F.col("raw_line"),
        F.when(~F.col("raw_line").startswith("A|"), F.lit("Invalid record prefix"))
         .when(F.size("parts") != expected_pos_col_count, F.concat(F.lit("Invalid column count: "), F.size("parts").cast("string")))
         .otherwise(F.lit("Unknown"))
         .alias("reason")
    )
)

# =========================================================
# BUILD POS DETAIL DATAFRAME
# =========================================================
pos_df = (
    good_pos_df
    .select(
        F.lit(file_load_id).alias("file_load_id"),
        F.lit(source_file_name).alias("source_file_name"),
        F.lit(file_path).alias("source_file_path"),
        F.current_timestamp().alias("load_ts"),
        *[F.trim(F.col("parts")[i]).alias(pos_columns[i]) for i in range(expected_pos_col_count)]
    )
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(pos_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# OPTIONAL TYPE CASTS
# Note: your file dates look like yyyyMMdd, not yyyy-MM-dd
# =========================================================


# =========================================================
# VALIDATION COUNTS
# =========================================================
actual_detail_count = pos_df.count()
bad_record_count = bad_pos_df.count()

footer_row = footer_parsed_df.collect()[0]
expected_detail_count = footer_row["footer_detail_row_count"]
footer_total_row_count = footer_row["footer_total_row_count"]

load_status = "SUCCESS"
load_message = "Loaded successfully"

if expected_detail_count is not None and expected_detail_count != actual_detail_count:
    load_status = "FAILED_VALIDATION"
    load_message = f"Footer detail count {expected_detail_count} does not match actual detail count {actual_detail_count}"

if footer_total_row_count is not None and footer_total_row_count != total_rows:
    load_status = "FAILED_VALIDATION"
    load_message = f"Footer total row count {footer_total_row_count} does not match physical file row count {total_rows}"

# =========================================================
# BUILD FINAL HEADER/AUDIT ROW
# Keep file_load_id only from header side to avoid duplicate column issue
# =========================================================
header_final_df = (
    header_parsed_df
    .crossJoin(
        footer_parsed_df.select(
            "footer_line",
            "footer_rec_type",
            "footer_file_date",
            "footer_file_desc",
            "footer_source",
            "footer_batch_no",
            "footer_total_row_count",
            "footer_detail_row_count",
            "footer_error_count",
            "footer_unused_count",
            "expected_detail_count"
        )
    )
    .withColumn("actual_total_row_count", F.lit(total_rows))
    .withColumn("actual_detail_count", F.lit(actual_detail_count))
    .withColumn("bad_record_count", F.lit(bad_record_count))
    .withColumn("load_status", F.lit(load_status))
    .withColumn("load_message", F.lit(load_message))
)

# =========================================================
# WRITE BAD RECORDS
# =========================================================
if bad_record_count > 0:
    bad_pos_df.write.mode("append").format("delta").saveAsTable(bad_table)

# =========================================================
# WRITE HEADER / AUDIT ROW
# Always write the audit row, even on validation failure
# =========================================================
header_final_df.write.mode("append").format("delta").option("mergeSchema","true").saveAsTable(header_table)

# =========================================================
# STOP IF VALIDATION FAILED
# =========================================================
if load_status != "SUCCESS":
    raise Exception(load_message)

# =========================================================
# DETAIL TABLE LOAD
# Recommended: append + rely on file-level idempotency check
# This is safer than merge unless you are 100% sure of business key
# =========================================================
pos_df.write.mode("append").format("delta").saveAsTable(pos_table)

# =========================================================
# OPTIONAL MERGE VERSION
# Uncomment only if you know the true business key
# =========================================================
# merge_key = ["source_file_name", "ACCT_NO", "SEC_NO", "CTRL_NO", "SEQ_NO"]
#
# if not spark.catalog.tableExists(pos_table):
#     pos_typed_df.write.mode("overwrite").format("delta").saveAsTable(pos_table)
# else:
#     target = DeltaTable.forName(spark, pos_table)
#     source = pos_typed_df.alias("s")
#     target_alias = target.alias("t")
#
#     merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in merge_key])
#
#     (
#         target_alias.merge(source, merge_condition)
#         .whenNotMatchedInsertAll()
#         .execute()
#     )

# =========================================================
# DEBUG / VALIDATION OUTPUT
# =========================================================
print(f"Loaded file_load_id       = {file_load_id}")
print(f"Total physical file rows  = {total_rows}")
print(f"Footer total row count    = {footer_total_row_count}")
print(f"Footer detail row count   = {expected_detail_count}")
print(f"Actual detail row count   = {actual_detail_count}")
print(f"Bad record count          = {bad_record_count}")
print(f"Load status               = {load_status}")

display(header_final_df)
display(pos_df)

if bad_record_count > 0:
    display(bad_pos_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
