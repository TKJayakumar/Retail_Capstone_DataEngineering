# Enterprise Retail Analytics Platform on Azure

## Overview

The **Enterprise Retail Analytics Platform** is an end-to-end Azure Data Engineering project developed using the **Medallion Architecture (Bronze, Silver, Gold)**. The solution demonstrates how retail data can be ingested, validated, transformed, historized using SCD Type 2, and prepared for business analytics through a metadata-driven framework.

The project emphasizes scalability, maintainability, and automation by minimizing hardcoded logic and using metadata to control data ingestion, validation, and transformation.

---

## Project Architecture

```text
Source Systems
      │
      ▼
Azure Data Factory
      │
      ▼
Azure Data Lake Storage Gen2
      │
      ▼
Bronze Layer
      │
      ▼
Silver Layer
      │
      ▼
SCD Type 2
      │
      ▼
Gold Layer
      │
      ▼
Power BI
```

---

## Features

* Metadata-driven Bronze ingestion
* Metadata-driven Silver processing
* Automatic schema evaluation
* Business rule validation
* Duplicate removal
* Lookup validation
* Rejected record handling
* Customer SCD Type 2 implementation
* Metadata-driven Gold layer
* Incremental data loading
* Audit columns for data lineage
* Business-ready Fact and Dimension tables

---

## Source Datasets

| Dataset       | Format | Description               |
| ------------- | ------ | ------------------------- |
| Orders        | CSV    | Retail sales transactions |
| Products      | CSV    | Product master data       |
| Customers     | CSV    | Customer master data      |
| ExchangeRates | JSON   | Currency exchange rates   |

---

## Medallion Architecture

### Bronze Layer

The Bronze layer ingests raw source data into Delta tables and adds audit information without modifying the original business data.

**Audit Columns**

* _AdfPipelineRunId
* _IngestionTimestamp
* _SourceFile
* _LoadDate

---

### Silver Layer

The Silver layer performs data cleansing and validation.

Operations include:

* Duplicate removal
* Schema standardization
* Data type conversion
* Business rule validation
* Lookup validation
* Rejected record handling

Additional audit columns:

* _ProcessedTimestamp
* _IsRejected

---

### SCD Type 2

Customer history is maintained using Slowly Changing Dimension (Type 2).

Additional columns:

* CustomerKey
* EffectiveDate
* ExpiryDate
* IsCurrent

This enables historical tracking whenever customer information changes.

---

### Gold Layer

The Gold layer creates business-ready analytical datasets.

Generated tables include:

* DimCustomers
* DimProducts
* FactSales
* CategorySales
* StoreSales
* MonthlySales
* CustomerSales

These tables are optimized for reporting and Power BI dashboards.

---

## Metadata-Driven Framework

The project uses metadata files to control processing instead of hardcoded logic.

Metadata files include:

* FileMetadata.csv
* SchemaMetadata.csv
* ValidationMetadata.csv
* DuplicateMetadata.csv
* LookupMetadata.csv
* SCDMetadata.csv
* GoldMetadata.csv

This approach makes the framework reusable and simplifies onboarding of new datasets.

---

## Technologies Used

* Microsoft Azure
* Azure Data Factory
* Azure Data Lake Storage Gen2
* Azure Databricks
* PySpark
* Delta Lake
* Azure SQL Database
* Power BI
* GitHub

---

## Project Structure

```text
Enterprise-Retail-Analytics/

├── Dataset/
├── Metadata/
│   ├── FileMetadata.csv
│   ├── SchemaMetadata.csv
│   ├── ValidationMetadata.csv
│   ├── DuplicateMetadata.csv
│   ├── LookupMetadata.csv
│   ├── SCDMetadata.csv
│   └── GoldMetadata.csv
│
├── Bronze/
├── Silver/
├── SCD/
├── Gold/
│
├── Notebooks/
│   ├── Bronze Framework
│   ├── Silver Framework
│   ├── Incremental Load
│   ├── SCD Type 2
│   └── Gold Framework
│
├── PowerBI/
├── Documentation/
└── README.md
```

---

## Data Flow

1. Source data is ingested using Azure Data Factory.
2. Raw files are stored in Azure Data Lake Storage Gen2.
3. Bronze layer loads raw data into Delta tables.
4. Silver layer validates, cleanses, and standardizes the data.
5. Customer changes are tracked using SCD Type 2.
6. Gold layer creates analytical Fact and Dimension tables.
7. Power BI consumes the Gold layer for dashboards and reporting.

---

## Learning Outcomes

This project demonstrates practical implementation of:

* Azure Data Engineering
* Metadata-driven ETL design
* Medallion Architecture
* Delta Lake
* PySpark transformations
* SCD Type 2 implementation
* Incremental data loading
* Data quality validation
* Enterprise data warehousing
* Power BI reporting

---

## Author

**Name:** Jayakumar T

**Project:** Enterprise Retail Analytics Platform on Azure

**Technology Stack:** Azure Data Factory, Azure Databricks, PySpark, Delta Lake, Azure SQL Database, Power BI

---

## License

This project is developed for educational and learning purposes as part of an Azure Data Engineering capstone project.
