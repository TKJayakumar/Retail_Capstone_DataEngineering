# Databricks notebook source
# Install the required library if not already present
%pip install azure-storage-blob --quiet

from azure.storage.blob import BlobServiceClient
import pandas as pd
import io

# 1. Provide your connection details
STORAGE_ACCOUNT_NAME = "retailstorageaccjk"
CONTAINER_NAME = "retail"
BLOB_NAME = "orders.csv"
ACCESS_KEY = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

# 2. Build the connection string and download blob
conn_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={ACCESS_KEY};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(conn_str)
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

# 3. Stream data into a local Pandas DataFrame
csv_data = blob_client.download_blob().readall()
pdf = pd.read_csv(io.BytesIO(csv_data))

# 4. Convert to a Spark DataFrame if you need to use Spark operations
df = spark.createDataFrame(pdf)
display(df)


# COMMAND ----------

# 1. Define your file path using the ABFSS protocol
file_path = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/orders.csv"

# 2. Configure the key directly inside the Spark read function using .option()
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net", "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A==") \
    .load(file_path)

# 3. View your Spark DataFrame data
display(df)


# COMMAND ----------

file_path = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Rawpath/product/products.csv"

# 2. Configure the key directly inside the Spark read function using .option()
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net", "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A==") \
    .load(file_path)

# 3. View your Spark DataFrame data
display(df)

# COMMAND ----------

file_path = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Rawpath/product/products.csv"

# 2. Configure the key directly inside the Spark read function using .option()
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net", "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A==") \
    .load(file_path)

# 3. View your Spark DataFrame data
display(df)

# COMMAND ----------



file_path = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Rawpath/customers/customers.csv"

# 2. Configure the key directly inside the Spark read function using .option()
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net", "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A==") \
    .load(file_path)

# 3. View your Spark DataFrame data
display(df)

# COMMAND ----------



file_path = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Rawpath/orders/orders.csv"

# 2. Configure the key directly inside the Spark read function using .option()
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net", "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A==") \
    .load(file_path)

# 3. View your Spark DataFrame data
display(df)

# COMMAND ----------



file_path = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Rawpath/exchangerate/exchangerate.json"

# 2. Configure the key directly inside the Spark read function using .option()
df = spark.read.format("json") \
    .option("header", "true") \
    .option("fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net", "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A==") \
    .load(file_path)

# 3. View your Spark DataFrame data
display(df)

# COMMAND ----------

#abfss://retail@retailstorageaccjk.dfs.core.windows.net/bronze/product/products.csv



file_path = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/bronze/orders/orders.parquet"

# 2. Configure the key directly inside the Spark read function using .option()
df = spark.read.format("parquet") \
    .option("header", "true") \
    .option("fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net", "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A==") \
    .load(file_path)

# 3. View your Spark DataFrame data
display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# 1. Define storage credentials
STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

# 2. Load your metadata file
METADATA_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Metadata/Filemetadata.csv"

metadata = spark.read \
    .option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(METADATA_PATH)

display(metadata)


for row in metadata.collect():

    file_name = row["FileName"]
    file_type = row["FileType"]
    raw_path = row["RawPath"]
    bronze_path = row["BronzePath"] 
    header = row["Header"]

    print("=" * 60)
    print(f"Loading {file_name}...")
    print("=" * 60)

    
    if file_type.lower() == "csv":
        df = spark.read \
            .option("header", header) \
            .option("inferSchema", "false") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .csv(raw_path)

    elif file_type.lower() == "json":
        df = spark.read \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .json(raw_path)

    else:
        print(f"Unsupported File Type : {file_type}")
        continue

    # 4. Transform data by adding metadata columns
    df = df \
        .withColumn("_AdfPipelineRunId", lit("Manual_Load")) \
        .withColumn("_IngestionTimestamp", current_timestamp()) \
        .withColumn("_SourceFile", lit(file_name)) \
        .withColumn("_LoadDate", current_date())

    # 5. Direct, Simple Native Save to ADLS (Overwrites old data perfectly)
    df.write \
        .format("parquet") \
        .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
        .mode("overwrite") \
        .save(bronze_path)

    print(f"{file_name} Loaded Successfully")
    print(f"Total Records : {df.count()}")
    display(df)

print("=" * 60)
print("Bronze Layer Created Successfully")
print("=" * 60)


# COMMAND ----------

# 1. Define your storage key configurations
STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

# 2. Define the path to your products parquet data folder
BRONZE_PRODUCTS_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/bronze/products/products.csv"

# 3. Read the data with inline credentials
df_products = spark.read.format("csv") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .load(BRONZE_PRODUCTS_PATH)

# 4. View your data
display(df_products)
