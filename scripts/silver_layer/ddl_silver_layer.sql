-- CRM Customer Information Table
-- Stores raw customer data from the CRM system.
CREATE or REPLACE TABLE silver.crm_cst_info (
    cst_id INT,                           -- Customer ID (numeric)
    cst_key NVARCHAR(50),                -- Unique customer key (possibly UUID or alphanumeric)
    cst_first_name NVARCHAR(50),         -- First name
    cst_last_name NVARCHAR(50),          -- Last name
    cst_marital_status NVARCHAR(50),     -- Marital status (e.g., Single, Married)
    cst_gndr NVARCHAR(50),               -- Gender
    cst_create_date DATE ,               -- Customer creation date
    dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP      -- Data warehouse creation date
);

-- CRM Product Information Table
-- Stores product metadata from the CRM system.
CREATE OR REPLACE TABLE silver.crm_prd_info (
    prd_id INT,                          -- Product ID (numeric)
    cat_id NVARCHAR(50),                 -- Category ID (derived from prd_key)
    prd_key NVARCHAR(50),                -- Unique product key
    prd_nm NVARCHAR(50),                 -- Product name
    prd_cost DECIMAL(10,2),              -- ⚠️ Cost stored as text, should be DECIMAL
    prd_line NVARCHAR(50),               -- Product line or category value
    prd_start_dt DATE,                   -- Product availability start date
    prd_end_dt DATE,                     -- Product end or discontinued date
    dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- CRM Sales Details Table
-- Captures transactional sales records.
CREATE OR REPLACE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),            -- Sales order number
    sls_prd_key NVARCHAR(50),            -- Product key from sales
    sls_cust_id INT,                     -- Customer ID associated with the sale
    sls_order_dt INT,                    -- ⚠️ Order date stored as INT, should convert to DATE
    sls_ship_dt INT,                     -- ⚠️ Ship date stored as INT
    sls_due_dt INT,                      -- ⚠️ Due date stored as INT
    sls_sales INT,                       -- Sales amount
    sls_quantity INT,                    -- Quantity sold
    sls_price INT,                        -- Price per unit
    dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- ERP Customer Demographics (System AZ12)
-- Contains basic customer demographic information.
CREATE OR REPLACE TABLE silver.erp_cust_AZ12 (
    CID NVARCHAR(50),                    -- Customer identifier
    BDATE DATE,                          -- Birthdate
    GEN NVARCHAR(50),                     -- Gender
    dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- ERP Customer Location (System A101)
-- Maps customers to their countries.
CREATE OR REPLACE TABLE silver.erp_loc_A101 (
    CID NVARCHAR(50),                    -- Customer identifier
    CNTRY NVARCHAR(50),                   -- Country of residence
    dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- ERP Product Category Mapping (System G1V2)
-- Maps products to categories and subcategories.
CREATE OR REPLACE TABLE silver.erp_px_cat_G1V2 (
    ID NVARCHAR(50),                     -- Product or item ID
    CAT NVARCHAR(50),                    -- Product category
    SUBCAT NVARCHAR(50),                 -- Product subcategory
    MAINTENANCE NVARCHAR(50),            -- Maintenance or status flag
    dwh_create_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);
