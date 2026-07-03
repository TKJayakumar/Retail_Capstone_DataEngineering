# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# 1. Define storage credentials
STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

SILVER_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/silver"
SCD_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/scd"

# 2. Read existing datasets using Parquet and inline credentials
old_df = spark.read.format("parquet") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .load(f"{SILVER_PATH}/customers.parquet")

new_df = spark.read.format("parquet") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .load(f"{SILVER_PATH}/customers_incremental.parquet")

# 3. Clean and parse date fields safely
old_df = old_df.withColumn(
    "LastUpdated",
    to_date(try_to_timestamp(col("LastUpdated"), lit("dd-MM-yyyy")))
)

new_df = new_df.withColumn(
    "LastUpdated",
    to_date(try_to_timestamp(col("LastUpdated"), lit("dd-MM-yyyy")))
)

# 4. Identify changed customer rows using an inner join
joined = old_df.alias("O").join(
    new_df.alias("N"),
    "CustomerID"
)

changed = joined.filter(
    (col("O.FirstName") != col("N.FirstName")) |
    (col("O.LastName") != col("N.LastName")) |
    (col("O.Email") != col("N.Email")) |
    (col("O.Phone") != col("N.Phone")) |
    (col("O.City") != col("N.City")) |
    (col("O.State") != col("N.State"))
)

unchanged = joined.filter(
    (col("O.FirstName") == col("N.FirstName")) &
    (col("O.LastName") == col("N.LastName")) &
    (col("O.Email") == col("N.Email")) &
    (col("O.Phone") == col("N.Phone")) &
    (col("O.City") == col("N.City")) &
    (col("O.State") == col("N.State"))
)

# 5. Track history using corrected SCD Type 2 logic
# OLD VERSION: Effectively started on its original date (21-01-2026) and expires TODAY
old_version = changed.select("O.*") \
    .withColumn("CustomerKey", row_number().over(Window.orderBy("CustomerID"))) \
    .withColumn("EffectiveDate", col("LastUpdated")) \
    .withColumn("ExpiryDate", current_date()) \
    .withColumn("IsCurrent", lit(False))

# Calculate the maximum key dynamically to prevent sequential ID overlaps
max_key = old_version.agg(max("CustomerKey")).collect()[0][0]
if max_key is None:
    max_key = 0

# NEW VERSION: Takes effect TODAY and has no expiry date yet
new_version = changed.select("N.*") \
    .withColumn("CustomerKey", row_number().over(Window.orderBy("CustomerID")) + max_key) \
    .withColumn("EffectiveDate", current_date()) \
    .withColumn("ExpiryDate", lit(None).cast("date")) \
    .withColumn("IsCurrent", lit(True))

# UNCHANGED RECORDS: Keep their original properties active
current_customers = unchanged.select("O.*") \
    .withColumn("CustomerKey", row_number().over(Window.orderBy("CustomerID")) + max_key + new_version.count()) \
    .withColumn("EffectiveDate", col("LastUpdated")) \
    .withColumn("ExpiryDate", lit(None).cast("date")) \
    .withColumn("IsCurrent", lit(True))

# 6. Consolidate historical and current rows
scd = old_version.unionByName(new_version).unionByName(current_customers)

# 7. Save output dataset using Parquet
scd.write.format("parquet") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .mode("overwrite") \
    .save(f"{SCD_PATH}/scd_customers.parquet")

print("SCD Type 2 Completed Successfully.")

# 8. View output dataset formatted exactly as requested
display(
    scd.select(
        "CustomerID",
        concat_ws(" ", col("FirstName"), col("LastName")).alias("Name"),
        "City",
        coalesce(date_format("EffectiveDate", "dd-MM-yyyy"), lit("currentdate()")).alias("EffectiveDate"),
        coalesce(date_format("ExpiryDate", "dd-MM-yyyy"), lit("null")).alias("ExpiryDate"),
        "IsCurrent"
    ).orderBy("CustomerID", "IsCurrent")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Incremental loading using the csv files csv_testing
# MAGIC .csv file

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# 1. Storage Configuration Details
STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

SILVER_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/silver"
SCD_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/scd"

# PATHS RESET TO ORIGINAL PRODUCTION LOCATIONS
CSV_INPUT_PATH = f"{SILVER_PATH}/csv_testing.csv"
INCREMENTAL_PARQUET_PATH = f"{SILVER_PATH}/customers_incremental.parquet"
FINAL_SCD_PATH = f"{SCD_PATH}/scd_customers.parquet"

# 2. Read the Incoming CSV Load
raw_csv_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .load(CSV_INPUT_PATH)

# Drop ADF metadata columns if present to prevent schema pollution
metadata_cols = ["_AdfPipelineRunId", "_AdfTriggerName", "_AdfTriggerTime"]
raw_csv_df = raw_csv_df.drop(*metadata_cols)

# 3. Automated Logic: Dynamic date alignment from dataset rows
updated_inc_df = raw_csv_df.withColumn(
    "LastUpdated", 
    coalesce(col("LastUpdated"), date_format(current_date(), "dd-MM-yyyy"))
)

# 4. Save the Incremental Batch to Parquet (Original Location)
updated_inc_df.write \
    .format("parquet") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .mode("overwrite") \
    .save(INCREMENTAL_PARQUET_PATH)

# 5. Read Existing Master Records and New Dataset
old_df = spark.read.format("parquet") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .load(f"{SILVER_PATH}/customers.parquet").drop(*metadata_cols)

new_df = spark.read.format("parquet") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .load(INCREMENTAL_PARQUET_PATH)

# 6. Parse and Clean Date Field Schemas safely
old_df = old_df.withColumn("LastUpdated", to_date(try_to_timestamp(col("LastUpdated"), lit("dd-MM-yyyy"))))
new_df = new_df.withColumn("LastUpdated", to_date(try_to_timestamp(col("LastUpdated"), lit("dd-MM-yyyy"))))

# 7. Use Full Outer Join to find differences automatically
joined = old_df.alias("O").join(new_df.alias("N"), "CustomerID", "full")

# Window partition configuration
windowSpec = Window.partitionBy("CustomerID").orderBy("CustomerID")

# 8. Segment Records into SCD Operational Groups
# Group A: Expired historical segments (Old values that have updates)
old_version = joined.filter(
    col("O.CustomerID").isNotNull() & col("N.CustomerID").isNotNull() & (
        (col("O.FirstName") != col("N.FirstName")) | (col("O.LastName") != col("N.LastName")) |
        (col("O.Email") != col("N.Email")) | (col("O.Phone") != col("N.Phone")) |
        (col("O.City") != col("N.City")) | (col("O.State") != col("N.State"))
    )
).select("O.*") \
 .withColumn("CustomerKey", row_number().over(windowSpec)) \
 .withColumn("EffectiveDate", col("LastUpdated")) \
 .withColumn("ExpiryDate", current_date()) \
 .withColumn("IsCurrent", lit(False))

# Calculate maximum sequence index offset safely using list extraction
try:
    max_key_result = old_version.agg(max("CustomerKey")).collect()
    if max_key_result and max_key_result[0][0] is not None:
        max_key = max_key_result[0][0]
    else:
        max_key = 0
except Exception:
    max_key = 0

# Group B: Brand new records and newly updated modifications active today
new_and_inserts = joined.filter(
    col("N.CustomerID").isNotNull() & (
        col("O.CustomerID").isNull() | 
        (col("O.FirstName") != col("N.FirstName")) | (col("O.LastName") != col("N.LastName")) |
        (col("O.Email") != col("N.Email")) | (col("O.Phone") != col("N.Phone")) |
        (col("O.City") != col("N.City")) | (col("O.State") != col("N.State"))
    )
).select("N.*") \
 .withColumn("CustomerKey", row_number().over(windowSpec) + max_key) \
 .withColumn("EffectiveDate", current_date()) \
 .withColumn("ExpiryDate", lit(None).cast("date")) \
 .withColumn("IsCurrent", lit(True))

# Group C: Records remaining entirely untouched by this script run
unchanged_version = joined.filter(
    col("O.CustomerID").isNotNull() & (
        col("N.CustomerID").isNull() | 
        ((col("O.FirstName") == col("N.FirstName")) & (col("O.LastName") == col("N.LastName")) &
         (col("O.Email") == col("N.Email")) & (col("O.Phone") == col("N.Phone")) &
         (col("O.City") == col("N.City")) & (col("O.State") == col("N.State")))
    )
).select("O.*") \
 .withColumn("CustomerKey", row_number().over(windowSpec) + max_key + new_and_inserts.count()) \
 .withColumn("EffectiveDate", col("LastUpdated")) \
 .withColumn("ExpiryDate", lit(None).cast("date")) \
 .withColumn("IsCurrent", lit(True))

# 9. Form the Consolidated Final SCD Dataset Array
scd = old_version.unionByName(new_and_inserts, allowMissingColumns=True) \
                 .unionByName(unchanged_version, allowMissingColumns=True)

# 10. Persist Output Changes to the Original Target Storage Path
scd.write.format("parquet") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .mode("overwrite") \
    .save(FINAL_SCD_PATH)

print(f"Production Pipeline Finished successfully. Saved safely into: {FINAL_SCD_PATH}")

# 11. View all columns to track and verify changes easily
display(
    scd.select(
        "CustomerID",
        "FirstName",
        "LastName",
        "Email",
        "Phone",
        "City",
        "State",
        coalesce(date_format("EffectiveDate", "dd-MM-yyyy"), lit("currentdate()")).alias("EffectiveDate"),
        coalesce(date_format("ExpiryDate", "dd-MM-yyyy"), lit("null")).alias("ExpiryDate"),
        "IsCurrent"
    ).orderBy("CustomerID", "IsCurrent")
)
