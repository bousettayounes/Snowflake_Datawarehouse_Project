/*
---------------------------------------------------------------------------------------------
View Names: gold.dim_customers, gold.dim_products, gold.fact_sales
Description: Creates unified dimension and fact views for BI and reporting purposes
EXECUTION: These views are queried directly ! 
----------------------------------------------------------------------------------------------
Purpose:
--------
This script creates a comprehensive data warehouse structure with two dimension tables and one fact table:
- dim_customers: Integrates customer information across CRM and ERP systems
- dim_products: Creates a consolidated product dimension with consistent attributes
- fact_sales: Establishes the central sales fact table linking to customer and product dimensions

---------------
Sources:
---------------
Customer Dimension:
- silver.crm_cst_info - Primary customer information from CRM
- silver.erp_cust_AZ12 - Supplemental customer data from ERP system
- silver.erp_loc_A101 - Location/geographic data from ERP system

Product Dimension:
- silver.crm_prd_info - Primary product information from CRM
- silver.erp_px_cat_g1v2 - Product categorization and maintenance data from ERP

Sales Fact:
- DATAWAREHOUSE.SILVER.CRM_SALES_DETAILS - Transaction-level sales data
- DATAWAREHOUSE.GOLD.DIM_PRODUCTS - Product dimension for product_key lookup
- DATAWAREHOUSE.GOLD.DIM_CUSTOMERS - Customer dimension for customer_key lookup

---------------
Transformations:
---------------

Customer Dimension:
- Generates surrogate key (customer_key) as sequence number
- Maintains source system identifiers (customer_id, customer_number)
- Standardizes naming conventions for customer attributes
- Implements CASE logic to handle gender information priority and defaults
- Joins customer information across disparate systems using common keys

Product Dimension:
- Generates surrogate key (product_key) as sequence number
- Standardizes naming conventions for product attributes
- Filters out historical products (WHERE product_end_date is null)
- Joins product information with categorization data
- Flattens nested subquery to provide clean dimensional structure

Sales Fact:
- Links transaction data to dimension tables using surrogate keys
- Preserves original order dates and fulfillment dates
- Maintains core sales metrics (sales amount, quantity, price)
- Creates star schema structure for optimized analytical queries

---------------
Notes:
---------------
- The product view filters out discontinued products where end_date is not null
- Maintenance information is sourced exclusively from ERP system
- Fully qualified table names are used for DATAWAREHOUSE schema references
- Joins on business keys (product_number, customer_id) to relevant dimension tables
*/

-- View: gold.dim_customers - Integrates customer information from CRM and ERP systems
Create OR replace view gold.dim_customers AS
    (SELECT 
        ROW_NUMBER() OVER (Order by cst_id ) AS customer_key,
        c1.cst_id AS customer_id,
        c1.cst_key AS customer_number,
        c1.cst_first_name AS first_name,
        c1.cst_last_name AS last_name ,
        CASE 
            WHEN c1.cst_gndr  != 'n/a' THEN c1.cst_gndr
            ELSE COALESCE(c2.gen,'n/a')
            END gender ,
        c2.bdate AS birth_date, 
        c3.cntry AS country,
        c1.cst_marital_status AS marital_status
    FROM silver.crm_cst_info c1
        LEFT JOIN silver.erp_cust_AZ12 c2 
        ON c1.CST_KEY = c2.CID
        LEFT JOIN silver.erp_loc_A101 c3
        on c1.CST_KEY = c3.cid);

-- View: gold.dim_products - Creates a consolidated product dimension excluding discontinued products
Create OR replace view gold.dim_products AS
    (SELECT product_key,
            product_id,
            product_number,  
            product_name,
            category_id,
            category,
            subcategory,
            maintenance, 
            cost,
            product_line,
            product_start_date
            FROM 
                (SELECT 
                        ROW_NUMBER() OVER (Order by prd_id ) AS product_key,
                        p1.prd_id AS product_id,
                        p1.prd_key AS product_number,
                        p1.cat_id AS category_id,
                        p1.prd_nm AS product_name,
                        p1.prd_line as product_line,
                        p1.prd_cost AS cost,
                        p1.prd_start_dt AS product_start_date,
                        p1.prd_end_dt AS product_end_date, -- filter Out historical data ! 
                        p2.cat as category,
                        p2.subcat AS subcategory,
                        p2.maintenance AS maintenance
                    FROM silver.crm_prd_info p1
                        LEFT JOIN silver.erp_px_cat_g1v2 p2 
                        ON p1.cat_id = p2.ID
                    WHERE product_end_date is null));

-- View: gold.fact_sales - Establishes the central sales fact table with dimension keys and sales metrics
CREATE OR REPLACE view gold.fact_sales AS
    (SELECT 
        sd.sls_ord_num AS order_number,
        pr.product_key ,
        cst.CUSTOMER_KEY,
        sd.sls_order_dt as order_date ,
        sd.sls_ship_dt as ship_date ,
        sd.sls_due_dt as due_date,
        sd.sls_sales as sales ,
        sd.sls_quantity  as quantity,
        sd.sls_price as price
    FROM DATAWAREHOUSE.SILVER.CRM_SALES_DETAILS sd
        LEFT JOIN DATAWAREHOUSE.GOLD.DIM_productS pr
        ON sd.sls_prd_key=pr.product_number
        LEFT JOIN DATAWAREHOUSE.GOLD.DIM_CUSTOMERS cst
        ON sd.sls_cust_id = cst.CUSTOMER_ID);