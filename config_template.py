"""
==========================================================
Azure Configuration Template
Enterprise Retail Analytics Platform

Instructions

1. Rename this file to config.py

2. Replace the placeholder values below with your
   Azure Storage Account information.

3. Save the file.

NOTE:
Never upload config.py to GitHub.
==========================================================
"""

# ==========================================================
# Azure Storage Configuration
# ==========================================================

# Azure Storage Account Name
STORAGE_ACCOUNT = "fs.azure.account.key.retailstorageaccjk.dfs.core.windows.net"

# Azure Storage Container Name
CONTAINER = "retail"

# Spark Configuration Property
STORAGE_KEY_CONFIG = (
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net"
)

# Azure Storage Account Key
STORAGE_KEY_VALUE = "+X8xbeH4e/xAPw1kPKqIY4X4+7R601Di/FwoDR33ZqNbSdPhIMoKue1Fj4Lgj0rtHXUvH7tgxO2X+ASt+gWP7A=="


# ==========================================================
# Base ADLS Path
# ==========================================================

BASE_PATH = (
    f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
)


# ==========================================================
# Metadata Paths
# ==========================================================

METADATA_PATH = f"{BASE_PATH}/Metadata"

FILE_METADATA = f"{METADATA_PATH}/FileMetadata.csv"

SCHEMA_METADATA = f"{METADATA_PATH}/SchemaMetadata.csv"

VALIDATION_METADATA = f"{METADATA_PATH}/ValidationMetadata.csv"

DUPLICATE_METADATA = f"{METADATA_PATH}/DuplicateMetadata.csv"

LOOKUP_METADATA = f"{METADATA_PATH}/LookupMetadata.csv"

SCD_METADATA = f"{METADATA_PATH}/SCDMetadata.csv"

GOLD_METADATA = f"{METADATA_PATH}/GoldMetadata.csv"


# ==========================================================
# Data Lake Paths
# ==========================================================

RAW_PATH = f"{BASE_PATH}/raw"

BRONZE_PATH = f"{BASE_PATH}/bronze"

SILVER_PATH = f"{BASE_PATH}/silver"

REJECT_PATH = f"{BASE_PATH}/rejected"

SCD_PATH = f"{BASE_PATH}/scd"

GOLD_PATH = f"{BASE_PATH}/gold"
