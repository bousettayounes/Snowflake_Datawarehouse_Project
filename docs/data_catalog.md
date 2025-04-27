# 📚 DATA CATALOG

## 1. Table: `gold.dim_customers`
**Description**: Customer dimension table holding customer-related attributes.

| Column Name     | Data Type | Description                          |
|-----------------|-----------|--------------------------------------|
| customer_key    | NUMBER(18,0) (PK)  | Surrogate primary key for customer.  |
| customer_id     | VARCHAR(50)    | Business key from source system.     |
| customer_number | VARCHAR(50)    | Alternate customer number.           |
| first_name      | VARCHAR(50)    | Customer's first name.               |
| last_name       | VARCHAR(50)    | Customer's last name.                |
| gender          | VARCHAR(50)    | Gender of the customer (M/F/Other).  |
| birth_date      | DATE      | Date of birth of the customer.       |
| country         | VARCHAR(50)    | Country where the customer lives.    |
| marital_status  | VARCHAR(50)    | Marital status (e.g., Single, Married). |

---

## 2. Table: `gold.dim_products`
**Description**: Product dimension table storing product-related information.

| Column Name       | Data Type | Description                          |
|-------------------|-----------|--------------------------------------|
| product_key       | NUMBER(38,0) (PK)  | Surrogate primary key for product.   |
| product_id        | VARCHAR(50)    | Business key for product.            |
| product_number    | VARCHAR(50)    | Product number from system.          |
| product_name      | VARCHAR(50)    | Name of the product.                 |
| category_id       | VARCHAR(50)    | ID of the category the product belongs to. |
| category          | VARCHAR(50)    | Name of the product category.        |
| subcategory       | VARCHAR(50)    | Name of the product subcategory.     |
| maNUMBER(38,0)enance       | BOOLEAN   | Indicates if maNUMBER(38,0)enance is needed. |
| cost              | NUMBER(38,0)   | Cost of the product.                 |
| product_line      | VARCHAR(50)    | Product line it belongs to.          |
| product_start_date| DATE      | Date when the product started selling. |

---

## 3. Table: `gold.fact_sales`
**Description**: Sales fact table capturing transactional sales data.

| Column Name     | Data Type | Description                          |
|-----------------|-----------|--------------------------------------|
| order_number    | VARCHAR(50)    | Unique order identifier.             |
| product_key     | NUMBER(38,0) (FK)  | Foreign key linking to dim_products. |
| customer_key    | NUMBER(18,0) (FK)  | Foreign key linking to dim_customers.|
| order_date      | DATE      | Date when the order was placed.      |
| ship_date       | DATE      | Date when the order was shipped.     |
| due_duedate     | DATE      | Date when the order is due.          |
| sales           | NUMBER(38,0)   | Revenue generated (Quantity × Price).|
| quantity        | NUMBER(38,0)       | Number of units sold.                |
| price           | NUMBER(38,2)   | Price per unit at the time of sale.  |

---

# 📌 Additional Notes:

- **Sales Formula**:
  > `sales = quantity * price`

- **Primary Keys**:
  - `gold.dim_customers.customer_key`
  - `gold.dim_products.product_key`
  - `gold.fact_sales.order_number` (plus foreign keys)

- **Foreign Key Relationships**:
  - `fact_sales.product_key → dim_products.product_key`
  - `fact_sales.customer_key → dim_customers.customer_key`

- **Grain**:
  - `gold.fact_sales` is at the **order line level** (each record = one product sold to one customer at one poNUMBER(38,0) in time).

---