# Databricks notebook source
from pyspark.sql.functions import *

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

METADATA_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Metadata"
GOLD_BASE = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/gold"

gold_metadata = spark.read \
    .option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/GoldMetadata.csv")

print("="*70)
print("Gold Metadata Processing Framework")
print("="*70)

for row in gold_metadata.collect():
    table_name = row["TableName"]
    source1 = row["Source1"]
    source2 = row["Source2"]
    transformation = row["Transformation"]
    raw_target_path = row["TargetPath"]

    clean_target_path = f"{GOLD_BASE}/{table_name.lower()}.parquet"

    print("--------------------------------------------")
    print("Table Name         :", table_name)
    print("Clean Target Path  :", clean_target_path)
    print("--------------------------------------------")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

STORAGE_KEY_CONFIG = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="

METADATA_PATH = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/Metadata"
SILVER = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/silver"
SCD = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/scd/scd_customers.parquet"
GOLD = "abfss://retail@retailstorageaccjk.dfs.core.windows.net/gold"

metadata = spark.read.option("header", "true") \
    .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
    .csv(f"{METADATA_PATH}/GoldMetadata.csv")

for row in metadata.collect():

    table = row["TableName"]
    trans = row["Transformation"]
    
    clean_target = f"{GOLD}/{table.lower()}.parquet"

    print("="*70)
    print("Creating", table)
    print("="*70)

    if trans == "DIM_PRODUCT":

        df = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{SILVER}/products.parquet")

        out = df.withColumn("ProductKey", row_number().over(Window.orderBy("ProductID"))) \
            .select("ProductKey", "ProductID", "ProductName", "Category", "SubCategory", "Brand", "CostPrice")

    elif trans == "DIM_CUSTOMER":

        df = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(SCD)

        out = df.filter(col("IsCurrent") == True)

    elif trans == "FACT_SALES":

        orders = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{SILVER}/orders.parquet")
            
        ex = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{SILVER}/exchangerates.parquet")
            
        cust = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{GOLD}/dimcustomers.parquet")
            
        prod = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{GOLD}/dimproducts.parquet")

        out = orders.join(cust.select("CustomerID", "CustomerKey"), "CustomerID") \
            .join(prod.select("ProductID", "ProductKey"), "ProductID") \
            .join(ex.select("RateDate", "ExchangeRate"), orders.OrderDate == ex.RateDate, "left") \
            .drop("RateDate") \
            .withColumn("SalesAmount", col("Quantity") * col("UnitPrice")) \
            .withColumn("ExchangeRate", coalesce(col("ExchangeRate"), lit(1.0))) \
            .withColumn("SalesUSD", round(col("SalesAmount") * col("ExchangeRate"), 2))

    elif trans == "CATEGORY_AGG":

        fact = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{GOLD}/factsales.parquet")
            
        prod = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{GOLD}/dimproducts.parquet")

        out = fact.join(prod, "ProductKey").groupBy("Category").agg(sum("SalesAmount").alias("TotalSales"), sum("Quantity").alias("TotalQuantity"))

    elif trans == "STORE_AGG":

        fact = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{GOLD}/factsales.parquet")
            
        out = fact.groupBy("StoreCode").agg(sum("SalesAmount").alias("TotalSales"), count("OrderID").alias("Orders"))

    elif trans == "MONTH_AGG":

        fact = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{GOLD}/factsales.parquet")
            
        out = fact.withColumn("Month", date_format(col("OrderDate"), "MMM-yyyy")).groupBy("Month").agg(sum("SalesAmount").alias("TotalSales"))

    elif trans == "CUSTOMER_AGG":

        fact = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{GOLD}/factsales.parquet")
            
        cust = spark.read.format("parquet") \
            .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
            .load(f"{GOLD}/dimcustomers.parquet")
            
        out = fact.drop("CustomerID").join(cust.select("CustomerKey", "CustomerID", "FirstName", "LastName"), "CustomerKey").groupBy("CustomerID", "FirstName", "LastName").agg(sum("SalesAmount").alias("TotalSales"), count("OrderID").alias("Orders"))

    out.write.format("parquet") \
        .option(STORAGE_KEY_CONFIG, STORAGE_KEY_VALUE) \
        .mode("overwrite") \
        .save(clean_target)

    print(table, "Created at:", clean_target)
    print("Rows :", out.count())
    display(out)

print("="*70)
print("Gold Framework Completed")
print("="*70)
