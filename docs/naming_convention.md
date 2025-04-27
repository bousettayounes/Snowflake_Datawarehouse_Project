## General Principles

---

- **Naming Conventions**: Use `snake_case` with lower case letters and underscores (_) to separate words (e.g., `load_date`).
- **Language**: Use English for all names.
- **Avoid Reserved Words**: Do not use SQL reserved words as object names.

---

## Table Naming Conventions

### Bronze Rules:

- All names must start with the source system name, and table names must match their original names without renaming.
- `<sourcesystem>_<entity>`
  - `<source system>`: Name of the source system (e.g., `crm`, `erp`).
  - `<entity>`: Exact table name from the source system.
  - **Example**: `crm_customer_info` —> Customer information from the CRM system.

---

### Silver Rules:

- All names must start with the source system name, and table names must match their original names without renaming.
- `<sourcesystem>_<entity>`
  - `<source system>`: Name of the source system (e.g., `crm`, `erp`).
  - `<entity>`: Exact table name from the source system.
  - **Example**: `crm_customer_info` —> Customer information from the CRM system.

---

### Gold Rules:

- All names must use meaningful business-aligned names for tables, starting with a category prefix.
- `<category>_<entity>`
  - `<category>`: Describes the role of the table, such as `dim` or `fact`.
  - `<entity>`: Descriptive name of the table, aligned with the business domain (e.g., `customers`, `products`, `sales`).
  - **Example**: `dim_customers` and `fact_sales`.

| Pattern | Meaning            | Examples            |
|---------|--------------------|---------------------|
| `dim_`  | Dimension Table     | `dim_product`        |
| `fact_` | Fact Table          | `fact_sales`         |
| `agg_`  | Aggregated Table    | `agg_customers`, `agg_sales` |

---

## Column Naming Convention

### Surrogate Keys

- All primary keys in dimension tables must use the suffix `_key`.
- `<table_name>_key`
  - `<table_name>`: Refers to the name of the table or entity the key belongs to.
  - `_key`: A suffix indicating that this column is a surrogate key.
  - **Example**: `customer_key` surrogate key in `dim_customers` table.

---

### Technical Columns

- All technical columns must start with the prefix `dwh_`, followed by a descriptive name indicating the column’s purpose.
- `dwh_<column_name>`:
  - `dwh`: Prefix exclusively for system-generated metadata.
  - `<column_name>`: Descriptive name indicating the column’s purpose.
  - **Example**: `dwh_load_date` → System-generated column used to store the date when the record was loaded.

---

### Stored Procedures

- All stored procedures used for loading data must follow the naming pattern: `load_<layer>`
  - `<layer>`: Represents the layer being loaded, such as `bronze`, `silver`, or `gold`.
  - **Example**:
    - `load_bronze`: Stored procedure for loading data into the bronze layer.
    - `load_silver`: Stored procedure for loading data into the silver layer.
    - `load_gold`: Stored procedure for loading data into the gold layer.