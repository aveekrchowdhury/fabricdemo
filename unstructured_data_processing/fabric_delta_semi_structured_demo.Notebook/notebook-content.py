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

# # Fabric Notebook: Delta Lake demo for semi-structured customer interactions
# 
# This notebook demonstrates how to:
# - Load nested multi-channel customer interactions from JSON
# - Inspect inferred schema for arrays, structs, and maps
# - Persist raw semi-structured records into Delta (Bronze)
# - Flatten selected fields for analytics (Silver)
# - Query nested fields with Spark SQL

# MARKDOWN ********************

# ## 1) Configure paths
# 
# > Update the base path to match your Fabric Lakehouse Files location if needed.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Replace this path with your Fabric Lakehouse path if needed.
input_path = "Files/bairddemofiles/unstructured/"
bronze_path = "dbo.bronze_customer_interactions"
silver_path = "dbo.silver_customer_interactions_flat"

print("Input:", input_path)
print("Bronze:", bronze_path)
print("Silver:", silver_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2) Load the JSON file
# 
# The sample file contains deeply nested structs, arrays, and heterogeneous channel payloads.

# CELL ********************

df = spark.read.option("multiline", "true").json(input_path)

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Schema for semi-structured JSON:")
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3) Persist raw JSON into a Bronze Delta table
# 
# This preserves the nested data exactly as ingested while giving you Delta features like ACID transactions and time travel.

# CELL ********************

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("mergeSchema", "true")
      .saveAsTable(bronze_path)
)

print("Bronze Delta table created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4) Query nested fields directly from Delta

# CELL ********************

display(spark.sql("""
SELECT
    interactionId,
    channel.type              AS channel_type,
    customer.profile.fullName AS customer_name,
    sentiment.label           AS sentiment_label,
    sentiment.score           AS sentiment_score,
    case.status               AS case_status,
    size(topics)              AS topic_count
FROM dbo.bronze_customer_interactions
ORDER BY eventTimeUtc
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5) Explode arrays for channel/product/topic-level analysis

# CELL ********************

products_df = (
    df
    .withColumn("product", F.explode_outer("products"))
    .select(
        "interactionId",
        F.col("channel.type").alias("channel_type"),
        F.col("customer.customerId").alias("customer_id"),
        F.col("customer.profile.fullName").alias("customer_name"),
        F.col("product.sku").alias("sku"),
        F.col("product.name").alias("product_name"),
        F.col("product.quantity").alias("quantity"),
        F.col("product.unitPrice").alias("unit_price"),
        F.col("product.attributes").alias("product_attributes")
    )
)

display(products_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

topics_df = (
    df
    .withColumn("topic", F.explode_outer("topics"))
    .groupBy(F.col("channel.type").alias("channel_type"), "topic")
    .count()
    .orderBy(F.desc("count"), "topic")
)

display(topics_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6) Create a Silver flattened analytical table

# CELL ********************

silver_df = (
    df.select(
        "interactionId",
        F.to_timestamp("eventTimeUtc").alias("event_ts"),
        F.to_date("eventTimeUtc").alias("event_date"),
        F.col("customer.customerId").alias("customer_id"),
        F.col("customer.profile.fullName").alias("customer_name"),
        F.col("customer.profile.segment").alias("customer_segment"),
        F.col("customer.profile.preferredLanguage").alias("preferred_language"),
        F.col("channel.type").alias("channel_type"),
        F.col("journeyStage").alias("journey_stage"),
        F.col("sentiment.label").alias("sentiment_label"),
        F.col("sentiment.score").alias("sentiment_score"),
        F.size("topics").alias("topic_count"),
        F.when(F.col("case.caseId").isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("has_case"),
        F.col("case.caseId").alias("case_id"),
        F.col("case.priority").alias("case_priority"),
        F.col("case.status").alias("case_status"),
        F.col("customAttributes").alias("custom_attributes")
    )
)

(
    silver_df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(silver_path)
)


print("Silver Delta table created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from dbo.bronze_customer_interactions


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from dbo.silver_customer_interactions_flat

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7) Example analytical queries

# CELL ********************

display(spark.sql("""
SELECT
    channel_type,
    sentiment_label,
    COUNT(*) AS interactions,
    ROUND(AVG(sentiment_score), 3) AS avg_sentiment
FROM silver_customer_interactions_flat
GROUP BY channel_type, sentiment_label
ORDER BY interactions DESC, channel_type
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
SELECT
    event_date,
    channel_type,
    COUNT(*) AS interactions,
    SUM(CASE WHEN has_case THEN 1 ELSE 0 END) AS case_backed_interactions
FROM silver_customer_interactions_flat
GROUP BY event_date, channel_type
ORDER BY event_date, channel_type
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8) Optional: retain full raw payload as a JSON string column

# CELL ********************

raw_json_df = df.select(
    "interactionId",
    F.to_json(F.struct(*df.columns)).alias("raw_payload")
)

display(raw_json_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
