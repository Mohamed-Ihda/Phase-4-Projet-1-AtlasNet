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

# ## 1. Move the CDR data to a Delta table

# CELL ********************

# define the schema of the cdr data
cdr_schema = T.StructType([
    T.StructField("record_id", T.StringType(), False),
    T.StructField("event_timestamp", T.StringType(), True), 
    T.StructField("msisdn", T.StringType(), True),
    T.StructField("imsi", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("direction", T.StringType(), True),
    T.StructField("other_party", T.StringType(), True),
    T.StructField("duration_sec", T.IntegerType(), True),
    T.StructField("bytes_uplink", T.LongType(), True),
    T.StructField("bytes_downlink", T.LongType(), True),
    T.StructField("cell_id", T.StringType(), True),
    T.StructField("roaming_flag", T.StringType(), True),
    T.StructField("rate_plan_code", T.StringType(), True),
    T.StructField("charge_amount_mad", T.DecimalType(12,4), True),
    T.StructField("record_status", T.StringType(), True),
    T.StructField("file_date", T.DateType(), True),
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
from datetime import datetime, timezone

# create the dataframe containing all cdr data
raw = spark.read.csv("Files/landing/cdr/cdr_*.csv.gz",header=True, nullValue="", schema=cdr_schema)
print(raw.count())

run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
print("run_id:", run_id)  

# add metadata to the cdr data 
bronze = (raw
.withColumn("_source_file", F.input_file_name())
.withColumn("_ingested_at", F.current_timestamp())
.withColumn("_ingest_run_id", F.lit(run_id)) # a UUID or timestamp string
.withColumn("_row_hash", F.sha2(F.concat_ws("||",
*[F.coalesce(F.col(c).cast("string"), F.lit(""))
for c in raw.columns]), 256)))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write the bronze data in a Delta Lake format
(bronze.write
.mode("overwrite")
.format("delta")
.partitionBy("file_date")
.option("overwriteSchema", "true")
.saveAsTable("bronze_cdr"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check if thier is no probleme in the bronze data
manifest_schema = T.StructType([T.StructField("file_date", T.DateType(), True), T.StructField("file_name", T.StringType(), True), T.StructField("record_count", T.LongType(), True)])
manifest = spark.read.csv("Files/landing/cdr/manifest.csv", header=True, schema=manifest_schema).withColumnRenamed("file_date", "m_file_date")
actual = spark.read.table("bronze_cdr").groupBy("file_date").count()
check = manifest.join(actual, manifest.m_file_date == actual.file_date, "full_outer").withColumn("diff", F.col("record_count") - F.col("count"))
# display(check)
display(check.filter(F.col("diff") != 0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Move the Topups data to a Delta table

# CELL ********************

# define the schema of the topups data
topups_schema = T.StructType([
    T.StructField("topup_id", T.StringType(), False),
    T.StructField("subscriber_id", T.StringType(), True),
    T.StructField("msisdn", T.StringType(), True),
    T.StructField("topup_timestamp", T.StringType(), True),
    T.StructField("amount_mad", T.DecimalType(12,4), True),
    T.StructField("channel", T.StringType(), True),
    T.StructField("topup_status", T.StringType(), True),
    T.StructField("payment_ref", T.StringType(), True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
from datetime import datetime, timezone

# create the dataframe containing all topups data
raw = spark.read.csv("Files/landing/topups.csv",header=True, nullValue="", schema=topups_schema)
print(raw.count())

run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
print("run_id:", run_id)  

# add metadata to the topups data 
bronze = (raw
.withColumn("_source_file", F.input_file_name())
.withColumn("_ingested_at", F.current_timestamp())
.withColumn("_ingest_run_id", F.lit(run_id)) # a UUID or timestamp string
.withColumn("_row_hash", F.sha2(F.concat_ws("||",
*[F.coalesce(F.col(c).cast("string"), F.lit(""))
for c in raw.columns]), 256)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write the bronze data in a Delta Lake format
(bronze.write
.mode("overwrite")
.format("delta")
.option("overwriteSchema", "true")
.saveAsTable("bronze_topups"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Move the Care_tickets data to a Delta table

# CELL ********************

# define the schema of the care_tickets data
care_tickets_schema = T.StructType([
    T.StructField("ticket_id", T.StringType(), False),
    T.StructField("subscriber_id", T.StringType(), True),
    T.StructField("opened_date", T.StringType(), True),
    T.StructField("closed_date", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("priority", T.StringType(), True),
    T.StructField("resolution", T.StringType(), True),
    T.StructField("satisfaction_score", T.IntegerType(), True),
    T.StructField("channel", T.StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
from datetime import datetime, timezone

# create the dataframe containing all care_tickets data
raw = spark.read.csv("Files/landing/care_tickets.csv",header=True, nullValue="", schema=care_tickets_schema)
print(raw.count())

run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
print("run_id:", run_id)  

# add metadata to the care_tickets data 
bronze = (raw
.withColumn("_source_file", F.input_file_name())
.withColumn("_ingested_at", F.current_timestamp())
.withColumn("_ingest_run_id", F.lit(run_id)) # a UUID or timestamp string
.withColumn("_row_hash", F.sha2(F.concat_ws("||",
*[F.coalesce(F.col(c).cast("string"), F.lit(""))
for c in raw.columns]), 256)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write the bronze data in a Delta Lake format
(bronze.write
.mode("overwrite")
.format("delta")
.option("overwriteSchema", "true")
.saveAsTable("bronze_care_tickets"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Move the churn_labels data to a Delta table

# CELL ********************

# define the schema of the churn_labels data
churn_labels_schema = T.StructType([
    T.StructField("subscriber_id", T.StringType(), False),
    T.StructField("msisdn", T.StringType(), True),
    T.StructField("churn_date", T.StringType(), True),
    T.StructField("churn_type", T.StringType(), True),
    T.StructField("churn_reason", T.StringType(), True),
    T.StructField("retention_offered", T.StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
from datetime import datetime, timezone

# create the dataframe containing all churn_labels data
raw = spark.read.csv("Files/landing/churn_labels.csv",header=True, nullValue="", schema=churn_labels_schema)
print(raw.count())

run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}"
print("run_id:", run_id)  

# add metadata to the churn_labels data 
bronze = (raw
.withColumn("_source_file", F.input_file_name())
.withColumn("_ingested_at", F.current_timestamp())
.withColumn("_ingest_run_id", F.lit(run_id)) # a UUID or timestamp string
.withColumn("_row_hash", F.sha2(F.concat_ws("||",
*[F.coalesce(F.col(c).cast("string"), F.lit(""))
for c in raw.columns]), 256)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write the bronze data in a Delta Lake format
(bronze.write
.mode("overwrite")
.format("delta")
.option("overwriteSchema", "true")
.saveAsTable("bronze_churn_labels"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
