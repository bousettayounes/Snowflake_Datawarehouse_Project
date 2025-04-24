/*
Purpose:
--------
This script sets up raw staging tables in the 'bronze' schema as part of the Medallion Architecture.
It integrates CRM and ERP source systems by creating foundational tables for customers, products, and sales data.

Tables Created:
---------------
1. CRM customer info
2. CRM product info
3. CRM sales details
4. ERP customer demographics (AZ12)
5. ERP customer location (A101)
6. ERP product category mapping (G1V2)

Notes:
------
- Some fields (e.g., dates and cost) are stored as text or integers and may require transformation in later stages.
- These tables represent raw ingested data and should not be used for analytics directly.
*/





-- CRM Customer Information Table
-- Stores raw customer data from the CRM system.
CREATE or REPLACE TABLE bronze.crm_cst_info (
    cst_id INT,                           -- Customer ID (numeric)
    cst_key NVARCHAR(50),                -- Unique customer key (possibly UUID or alphanumeric)
    cst_first_name NVARCHAR(50),         -- First name
    cst_last_name NVARCHAR(50),          -- Last name
    cst_marital_status NVARCHAR(50),     -- Marital status (e.g., Single, Married)
    cst_gndr NVARCHAR(50),               -- Gender
    cst_create_date DATE                 -- Customer creation date
);

-- CRM Product Information Table
-- Stores product metadata from the CRM system.
CREATE OR REPLACE TABLE bronze.crm_prd_info (
    prd_id INT,                          -- Product ID (numeric)
    prd_key NVARCHAR(50),                -- Unique product key
    prd_nm NVARCHAR(50),                 -- Product name
    prd_cost NVARCHAR(50),               -- ⚠️ Cost stored as text, should be DECIMAL
    prd_line NVARCHAR(50),               -- Product line or category value
    prd_start_dt DATETIME,               -- Product availability start date
    prd_end_dt DATETIME                  -- Product end or discontinued date
);

-- CRM Sales Details Table
-- Captures transactional sales records.
CREATE OR REPLACE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),            -- Sales order number
    sls_prd_key NVARCHAR(50),            -- Product key from sales
    sls_cust_id INT,                     -- Customer ID associated with the sale
    sls_order_dt INT,                    -- ⚠️ Order date stored as INT, should convert to DATE
    sls_ship_dt INT,                     -- ⚠️ Ship date stored as INT
    sls_due_dt INT,                      -- ⚠️ Due date stored as INT
    sls_sales INT,                       -- Sales amount
    sls_quantity INT,                    -- Quantity sold
    sls_price INT                        -- Price per unit
);

-- ERP Customer Demographics (System AZ12)
-- Contains basic customer demographic information.
CREATE OR REPLACE TABLE bronze.erp_cust_AZ12 (
    CID NVARCHAR(50),                    -- Customer identifier
    BDATE DATE,                          -- Birthdate
    GEN NVARCHAR(50)                     -- Gender
);

-- ERP Customer Location (System A101)
-- Maps customers to their countries.
CREATE OR REPLACE TABLE bronze.erp_loc_A101 (
    CID NVARCHAR(50),                    -- Customer identifier
    CNTRY NVARCHAR(50)                   -- Country of residence
);

-- ERP Product Category Mapping (System G1V2)
-- Maps products to categories and subcategories.
CREATE OR REPLACE TABLE bronze.erp_px_cat_G1V2 (
    ID NVARCHAR(50),                     -- Product or item ID
    CAT NVARCHAR(50),                    -- Product category
    SUBCAT NVARCHAR(50),                 -- Product subcategory
    MAINTENANCE NVARCHAR(50)            -- Maintenance or status flag
);
