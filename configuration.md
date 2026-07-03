# Configuration

## Overview

The Enterprise Retail Analytics Platform uses a centralized configuration file to manage all environment-specific settings required for the Azure Data Engineering pipeline.

Instead of hardcoding storage account information and file paths inside the notebooks, these values are stored as configuration variables. This approach improves maintainability, simplifies deployment across different environments, and enhances security by separating sensitive information from the application code.

The configuration file is shared across the Bronze, Silver, SCD Type 2, and Gold layer notebooks.

---

# Configuration Variables

| Configuration Variable | Description | Purpose |
|------------------------|-------------|---------|
| STORAGE_KEY_CONFIG | Stores the Spark configuration property required to authenticate Azure Data Lake Storage Gen2. | Enables Spark to establish a secure connection with the Azure Storage Account before reading or writing data. |
| STORAGE_KEY_VALUE | Stores the Azure Storage Account access key. | Provides secure authentication between Azure Databricks and Azure Data Lake Storage Gen2. |
| METADATA_PATH | Specifies the location of all metadata files. | Allows the notebooks to dynamically read processing configurations. |
| RAW_PATH | Specifies the location of the raw source datasets. | Acts as the landing zone for incoming data before processing. |
| BRONZE_PATH | Specifies the storage location of the Bronze layer. | Stores raw ingested datasets along with audit information. |
| SILVER_PATH | Specifies the storage location of the Silver layer. | Stores cleansed, validated, and standardized datasets. |
| REJECT_PATH | Specifies the storage location of rejected records. | Stores records that fail validation rules for future review. |
| SCD_PATH | Specifies the storage location used for SCD Type 2 processing. | Stores historical and current customer records while maintaining version history. |
| GOLD_PATH | Specifies the storage location of the Gold layer. | Stores business-ready Fact tables, Dimension tables, and aggregated datasets used for reporting. |

---

# Benefits

The centralized configuration approach provides the following benefits:

- Eliminates hardcoded values throughout the notebooks.
- Simplifies environment configuration changes.
- Improves maintainability and readability.
- Enhances security by separating credentials from application code.
- Supports reusable and scalable notebook development.
