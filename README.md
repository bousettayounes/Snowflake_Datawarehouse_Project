# ❄️ Snowflake Data Warehouse Project

## Overview

This project implements a full data warehouse solution using **Snowflake**, based on the best practices of the **Bronze**, **Silver**, and **Gold** layered architecture.  
It enables scalable, organized, and business-ready data for analytics, reporting, and machine learning.

---

## 📌 High-Level Architecture

![High Level Architecture](./docs/architecture.png)

- **Source Systems**:  
  - ERP System (CSV files)  
  - CRM System (CSV files)

- **Snowflake Layers**:
  - **Bronze Layer**: Raw, untransformed data storage.
  - **Silver Layer**: Cleaned, standardized, and enriched data.
  - **Gold Layer**: Business-ready data modeled for consumption.

- **Consumption Methods**:
  - BI and Reporting (Power BI, Tableau)
  - Ad-Hoc SQL Queries
  - Machine Learning

---

## 📂 Layer Descriptions

| Layer         | Description                       | Object Type | Load Type                  | Transformations                                      |
| :------------ | :--------------------------------- | :---------- | :-------------------------- | :--------------------------------------------------- |
| **Bronze**    | Raw source data                    | Table       | Batch Processing (Full Load) | None (as-is ingestion)                              |
| **Silver**    | Cleaned and standardized data      | Table       | Batch Processing (Full Load) | Data Cleansing, Standardization, Normalization, Enrichment |
| **Gold**      | Business-ready data                | View        | No Load (Transformation Views) | Data Integration, Aggregation, Business Logic         |

---

## 🛠️ Naming Conventions

### General Rules
- Use **`snake_case`** (lowercase letters + underscores).
- Always write in **English**.
- Avoid **SQL reserved keywords** in names.

### Table Naming Pattern

| Layer  | Pattern                     | Example               |
| :----- | :-------------------------- | :-------------------- |
| Bronze | `<source_system>_<entity>`   | `crm_customer_data`    |
| Silver | `<source_system>_<entity>`   | `erp_sales_orders`     |
| Gold   | `<category>_<entity>`        | `dim_product`, `fact_sales`, `agg_customer_spend` |

> **Categories**:  
> `dim_` = Dimension table  
> `fact_` = Fact table  
> `agg_` = Aggregated table

### Column Naming
- **Surrogate Keys**:  
  Format: `<table_name>_key`  
  Example: `customer_key`
- **Technical Columns**:  
  Prefix: `dwh_`  
  Example: `dwh_inserted_timestamp`

### Stored Procedures
- Naming Format:  
  `load_<layer>_<table>`  
  Example: `load_bronze_crm_customer`, `load_silver_erp_orders`

---

## 📈 Key Features

- Reliable batch ingestion process from ERP and CRM systems.
- Robust data transformation and enrichment pipelines.
- Business-ready data modeled in Star Schema, Flat Tables, and Aggregated Views.
- Supports seamless integration with BI tools and Machine Learning platforms.

---

## 🚀 Technologies Used

- **Snowflake** — Cloud Data Warehouse
- **Power BI / Tableau** — Business Intelligence
- **Python / SQL** — Data processing and orchestration

---

## 🗓️ Project Structure

```bash
├── README.md
├── architecture.png
├── scripts/
│   ├── load_bronze.sql
│   ├── load_silver.sql
│   └── load_gold.sql
└── models/
    ├── bronze/
    ├── silver/
    └── gold/
```

---

## 📢 Contribution

Contributions are welcome!  
Feel free to fork this repository, submit pull requests, or open issues.

---

# ✅ Final Notes

This project is designed to serve as a foundation for building scalable, efficient, and business-ready data warehouses in Snowflake.