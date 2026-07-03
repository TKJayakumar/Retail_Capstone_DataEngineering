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
