# ❄️ Snowflake Data Warehouse Project

## Overview

This project implements a full data warehouse solution using **Snowflake**, based on the best practices of the **Bronze**, **Silver**, and **Gold** layered architecture.  
It enables scalable, organized, and business-ready data for analytics, reporting, and machine learning.

---

## 📌 High-Level Architecture

![High Level Architecture](docs/architecture.png)

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

## 🔄 Integration Model (CRM & ERP Systems)

The Integration Model describes how CRM and ERP data are aligned and integrated:

- **CRM System**:
  - `crm_prd_info`: Product details with `prd_key` as primary key.
  - `crm_cust_info`: Customer details with `cst_id` and `cst_key`.
  - `crm_sls_details`: Sales transactions, connecting `sls_prd_key` and `sls_cust_id`.

- **ERP System**:
  - `erp_px_cat_g1v2`: Product category details.
  - `erp_loc_a101`: Customer location information.
  - `erp_cust_az12`: Extended customer information.

**Key Relationships**:
- CRM's `prd_key` maps to ERP’s product IDs.
- CRM's `cst_id/cst_key` maps to ERP’s customer IDs (`cid`).
- Sales records (`crm_sls_details`) form the link between customers and products.

---

## 🔥 Data Pipeline: Bronze → Silver → Gold

The data pipeline is structured into three major transformation layers:

### Bronze Layer (Raw Ingestion)
- Data from ERP and CRM systems are loaded as-is into Snowflake.
- Tables:
  - `crm_sales_details`
  - `crm_cust_info`
  - `crm_prd_info`
  - `erp_cust_az12`
  - `erp_loc_a101`
  - `erp_px_cat_g1v2`

### Silver Layer (Cleaning & Enrichment)
- Data is cleaned, standardized, and minor enrichment occurs.
- One-to-one mappings from Bronze tables:
  - Example: `crm_cust_info` → cleaned `crm_cust_info` in Silver.

### Gold Layer (Business-Ready Models)
- Final business models created:
  - `fact_sales`: Sales fact table.
  - `dim_customers`: Customer dimension, combining CRM and ERP customer data.
  - `dim_products`: Product dimension, combining CRM and ERP product categories.

**Highlights**:
- **Data Modeling**: Star Schema (fact and dimensions)
- **Transformation**: Joins between CRM and ERP entities.
- **Usage**: Reporting, analytics, and ML ready datasets.

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
├── datasets/
│   ├── source_crm/
│   │   └── cust_info.csv
│   │   └── prd_info.csv
│   │   └── sales_details.csv
│   ├── source_erp/
│   │   ├── CUST_AZ12.csv
│   │   ├── LOC_A101.csv
│   │   └── PX_CAT_G1V2.csv
├── docs/
│   ├── Architecture.png
│   ├── Architecture.drawio
│   ├── Data_Integration_Model.drawio
│   ├── Star schema Model.drawio
│   ├── data_catalog.md
│   ├── naming_convention.md
│   └── data_flow.drawio
├── myven/                
├── scripts/
│   ├── bronze_layer/
│   │   ├── ddl_bronze_layer.sql
│   │   └── proc_load_bronze.sql
│   ├── silver_layer/
│   │   ├── ddl_silver_layer.sql
│   │   ├── dml_silver_layer.sql
│   │   └── init_database.sql
│   ├── gold_layer/
│   │   └── ddl_gold_layer.sql
│   │  
│   │  
│   ├── init_database.sql 
├── tests/
│   ├── gold_layer_quality_check.sql
│   └── silver_layer_quality_check.sql
├── .gitignore
├── README.md  
```
