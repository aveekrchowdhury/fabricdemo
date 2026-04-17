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

# PARAMETERS CELL ********************


subfolder =  "vendor1" # Default to empty string if not provided
folder_name = "bairddemofiles"
entity_name = "dimension_city"
schema_name = "dbo"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

input_folder = f"Files/{folder_name}/{subfolder}/{entity_name}"
file_list = notebookutils.fs.ls(input_folder)
csv_files = [f.path for f in file_list if f.path.endswith(".csv")]

if not csv_files:
    raise FileNotFoundError(f"No CSV files found in {input_folder}")

df = spark.read.format("csv").option("header", "true").load(csv_files)

if schema_name:
    table_full_name = f"{schema_name}.{entity_name}"
else:
    table_full_name = entity_name

df.write.mode("overwrite").saveAsTable(table_full_name)

display(spark.read.table(table_full_name))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
