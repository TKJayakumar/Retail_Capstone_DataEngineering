# Databricks notebook source
from pyspark.sql.functions import *

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

METADATA_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Metadata"

file_meta = spark.read.option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/Filemetadata.csv")

schema_meta = spark.read.option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/SchemaMetadata.csv")

validation_meta = spark.read.option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/ValidationMetadata.csv")

duplicate_meta = spark.read.option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/DuplicateMetadata.csv")

lookup_meta = spark.read.option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/LookupMetadata.csv")

display(file_meta)
display(schema_meta)
display(validation_meta)
display(duplicate_meta)
display(lookup_meta)


# COMMAND ----------

# MAGIC %md
# MAGIC Removing Duplicates

# COMMAND ----------

from pyspark.sql.functions import *

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

BRONZE_BASE = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/bronze"
METADATA_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Metadata"

duplicate_meta = spark.read.option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/DuplicateMetadata.csv")

def remove_duplicates(table):
    df = spark.read.format("parquet") \
        .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
        .load(f"{BRONZE_BASE}/{table}")
    
    row = duplicate_meta.filter(col("TableName") == table).first()
    keys = [x.strip() for x in row["DuplicateColumns"].split(",")]
    return df.dropDuplicates(keys)


# COMMAND ----------

# MAGIC %md
# MAGIC DataType Checking

# COMMAND ----------

from pyspark.sql.functions import *

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

METADATA_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Metadata"

schema_meta = spark.read.option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/SchemaMetadata.csv")

def cast_schema(df, table):
    cols = schema_meta.filter(col("TableName") == table).collect()
    for c in cols:
        name = c["ColumnName"]
        dtype = c["DataType"].lower()
        if dtype == "int":
            df = df.withColumn(name, col(name).cast("int"))
        elif dtype == "double":
            df = df.withColumn(name, col(name).cast("double"))
        elif dtype == "date":
            df = df.withColumn(name, to_date(try_to_timestamp(col(name), lit("dd-MM-yyyy"))))
        else:
            df = df.withColumn(name, col(name).cast("string"))
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC Validataion of the data

# COMMAND ----------

from pyspark.sql.functions import *

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

METADATA_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Metadata"

validation_meta = spark.read.option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/ValidationMetadata.csv")

def validate(df, table):
    valid = df
    rules = validation_meta.filter(col("TableName") == table).collect()
    for r in rules:
        c = r["ColumnName"]
        rule = r["Rule"]
        val = r["RuleValue"]
        if rule == "NOT_NULL":
            valid = valid.filter(col(c).isNotNull() & (trim(col(c)) != ""))
        elif rule == "GREATER_THAN":
            valid = valid.filter(col(c) > float(val))
        elif rule == "NOT_EQUALS":
            valid = valid.filter(col(c) != val)
        elif rule == "REGEX":
            valid = valid.filter(col(c).rlike(val))
        elif rule == "VALID_DATE":
            valid = valid.filter(col(c).isNotNull())
        elif rule == "IN":
            valid = valid.filter(col(c).isin(val.split(",")))
    return valid, df.subtract(valid)


# COMMAND ----------

from pyspark.sql.functions import *

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

SILVER = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/silver"

def validate_orders(df):
    customers = spark.read.format("parquet") \
        .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
        .load(f"{SILVER}/customers.parquet") \
        .select("CustomerID")
        
    products = spark.read.format("parquet") \
        .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
        .load(f"{SILVER}/products.parquet") \
        .select("ProductID")
        
    df = df.join(customers.withColumnRenamed("CustomerID", "ValidCustomerID"), df.CustomerID == col("ValidCustomerID"), "inner").drop("ValidCustomerID")
    df = df.join(products.withColumnRenamed("ProductID", "ValidProductID"), df.ProductID == col("ValidProductID"), "inner").drop("ValidProductID")
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC Write to the silver Layer

# COMMAND ----------

from pyspark.sql.functions import *

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

SILVER = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/silver"

def write_silver(df, table):
    df = df.withColumn("_ProcessedTimestamp", current_timestamp()).withColumn("_IsRejected", lit(False))
    
    output_path = f"{SILVER}/{table.lower()}.parquet"
    
    df.write \
        .format("parquet") \
        .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
        .mode("overwrite") \
        .save(output_path)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

REJECT = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/rejected"

def write_rejected(df, table):
    df = df.withColumn("_ProcessedTimestamp", current_timestamp()).withColumn("_IsRejected", lit(True))
    
    output_path = f"{REJECT}/{table.lower()}.parquet"
    
    df.write \
        .format("parquet") \
        .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
        .mode("overwrite") \
        .save(output_path)


# COMMAND ----------

# MAGIC %md
# MAGIC Main Bronze to Silver Conversion Code

# COMMAND ----------

from pyspark.sql.functions import *

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

BRONZE_BASE = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/bronze"
SILVER_BASE = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/silver"
REJECT_BASE = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/rejected"

table_name_map = {"products": "Products", "customers": "Customers", "orders": "Orders", "exchangerate": "ExchangeRates"}
tables = ["products", "customers", "orders", "exchangerate"]

for table in tables:

    print("="*80)
    print(f"Processing Table : {table}")
    print("="*80)
    
    table_meta = table_name_map[table]
    
    # Check if target is 'exchangerate' (json) or others (csv) to match your folder structures
    if table == "exchangerate":
        full_bronze_path = f"{BRONZE_BASE}/{table}/{table}.json"
    else:
        full_bronze_path = f"{BRONZE_BASE}/{table}/{table}.csv"
        
    print(f"Reading from path: {full_bronze_path}")
    
    try:
        df = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(full_bronze_path)
            
        row = duplicate_meta.filter(col("TableName") == table_meta).first()
        if row:
            keys = [x.strip() for x in row["DuplicateColumns"].split(",")]
            df = df.dropDuplicates(keys)
            
        initial_count = df.count()
        
    except Exception as e:
        error_msg = str(e)
        if "UNABLE_TO_INFER_SCHEMA" in error_msg or "Path does not exist" in error_msg or "404" in error_msg:
            print(f"⚠️ Warning: Skipping table '{table}' because path {full_bronze_path} is empty or missing.")
            continue
        else:
            raise e

    print("After Removing Duplicates")
    print("Record Count :", initial_count)
    display(df)

    cols = schema_meta.filter(col("TableName") == table_meta).collect()
    for c in cols:
        name = c["ColumnName"]
        dtype = c["DataType"].lower()
        if dtype == "int":
            df = df.withColumn(name, col(name).cast("int"))
        elif dtype == "double":
            df = df.withColumn(name, col(name).cast("double"))
        elif dtype == "date":
            df = df.withColumn(name, to_date(try_to_timestamp(col(name), lit("dd-MM-yyyy"))))
        else:
            df = df.withColumn(name, col(name).cast("string"))

    print("After Schema Casting")
    df.printSchema()
    display(df)

    valid_df = df
    rules = validation_meta.filter(col("TableName") == table_meta).collect()
    for r in rules:
        c = r["ColumnName"]
        rule = r["Rule"]
        val = r["RuleValue"]
        if rule == "NOT_NULL":
            valid_df = valid_df.filter(col(c).isNotNull() & (trim(col(c)) != ""))
        elif rule == "GREATER_THAN":
            valid_df = valid_df.filter(col(c) > float(val))
        elif rule == "NOT_EQUALS":
            valid_df = valid_df.filter(col(c) != val)
        elif rule == "REGEX":
            valid_df = valid_df.filter(col(c).rlike(val))
        elif rule == "VALID_DATE":
            valid_df = valid_df.filter(col(c).isNotNull())
        elif rule == "IN":
            valid_df = valid_df.filter(col(c).isin(val.split(",")))
    rejected_df = df.subtract(valid_df)

    print("Valid Records :", valid_df.count())
    print("Rejected Records :", rejected_df.count())

    display(valid_df)
    display(rejected_df)

    if table == "orders":
        valid_df = validate_orders(valid_df)
        print("After Lookup Validation")
        print("Valid Orders :", valid_df.count())
        display(valid_df)

    write_silver(valid_df, table)

    write_rejected(rejected_df, table)

    silver_path = f"{SILVER_BASE}/{table.lower()}.parquet"
    reject_path = f"{REJECT_BASE}/{table.lower()}.parquet"

    silver_df = spark.read.format("parquet") \
        .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
        .load(silver_path)
        
    reject_df = spark.read.format("parquet") \
        .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
        .load(reject_path)

    print(f"Silver {table} Count :", silver_df.count())
    print(f"Rejected {table} Count :", reject_df.count())

    display(silver_df)
    display(reject_df)

print("="*80)
print("SILVER FRAMEWORK COMPLETED SUCCESSFULLY")
print("="*80)
