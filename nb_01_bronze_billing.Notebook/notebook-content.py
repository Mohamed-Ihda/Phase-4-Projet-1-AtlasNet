# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1a032321-a1eb-44a4-8cbf-79c7f59eaa8a",
# META       "default_lakehouse_name": "lkh_atlasnet",
# META       "default_lakehouse_workspace_id": "5308aa28-eb93-41df-9239-ed8ff3a2c753",
# META       "known_lakehouses": [
# META         {
# META           "id": "1a032321-a1eb-44a4-8cbf-79c7f59eaa8a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Import pyspark types and functions

# CELL ********************

import pyspark.sql.types as T
import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create the Bronze layer

# MARKDOWN ********************

# ## Move the content of each table in the billing data to a Delta table

# PARAMETERS CELL ********************

# create two parameters one for the table name and the other for the source file
table_name = "table_name"
source_file = "source_file"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(table_name)
print(source_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
from datetime import datetime, timezone

# create the dataframe containing all cdr data
raw = spark.read.table(table_name)
print(raw.count())

run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
print("run_id:", run_id)  

# add metadata to the cdr data 
bronze = (raw
.withColumn("_source_file", F.lit(source_file))
.withColumn("_ingested_at", F.current_timestamp())
.withColumn("_ingest_run_id", F.lit(run_id)) # a UUID or timestamp string
.withColumn("_row_hash", F.sha2(F.concat_ws("||",
*[F.coalesce(F.col(c).cast("string"), F.lit(""))
for c in raw.columns]), 256)))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# write the bronze data in a Delta Lake format
(bronze.write
.mode("overwrite")
.format("delta")
.option("overwriteSchema", "true")
.saveAsTable(table_name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
