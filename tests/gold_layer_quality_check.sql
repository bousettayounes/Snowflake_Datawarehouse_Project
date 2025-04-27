/*
---------------------------------------------------------------------------------------------
Script Name: Data Quality Testing Script
Description: Performs testing and validation of silver and gold layer data
EXECUTION : Run each section individually as needed
----------------------------------------------------------------------------------------------
Purpose:
--------
This script provides a series of data quality checks to validate the integrity, consistency,
and correctness of the silver and gold layer data. It includes checks for duplicate records,
referential integrity, data consistency, and proper transformation validation.

Test Groups:
-----------
1. Customer Data Redundancy Test - Checks for duplicate customer records across systems
2. Gender Data Integrity Test - Validates the gender data harmonization logic
3. Gold Layer Validation Tests - Validates the final gold views for correctness
4. Referential Integrity Test - Checks for orphaned records between fact and dimension tables

Notes:
------
- Run these tests after data loads and transformations to ensure data quality
- The numbered comments separate the script into logical test sections
- Tests are designed to identify potential issues requiring attention
*/

-- 1) CHECK the id redundancy and duplicate records
SELECT 
ROW_NUMBER() OVER (Partition BY customer_id ) AS Customer_,
c1.cst_id,
c1.cst_key,
c1.cst_first_name,
c1.cst_last_name,
c1.cst_marital_status,
c1.cst_create_date,
c2.bdate,
c3.cntry
FROM silver.crm_cst_info c1
LEFT JOIN silver.erp_cust_AZ12 c2 
ON c1.CST_KEY = c2.CID
LEFT JOIN silver.erp_loc_A101 c3
on c1.CST_KEY = c3.cid;

-- 2) Gender integrity check
SELECT DISTINCT
    c1.cst_gndr,
    c2.gen,
    CASE 
    WHEN c1.cst_gndr  != 'n/a' THEN c1.cst_gndr
    ELSE COALESCE(c2.gen,'n/a')
    END new_gender_column
    
    FROM silver.crm_cst_info c1
    LEFT JOIN silver.erp_cust_AZ12 c2 
    ON c1.CST_KEY = c2.CID
    LEFT JOIN silver.erp_loc_A101 c3
    on c1.CST_KEY = c3.cid
    order by 1,2;


-- 3) Quality check of gold layer
SELECT * from gold.customer_info;

-- 4) Checking duplicate records in product_key
SELECT  --prduct_key,
        prduct_number,
        count(*) product_key_counted,
        -- category_id,
        -- prduct_name,
        -- prduct_cost,
        -- prduct_start_date,
        -- category,
        -- subcategory,
        -- maintenance 
        FROM 
            (SELECT 
                    ROW_NUMBER() OVER (Order by prd_id ) AS prduct_key,
                    p1.prd_id AS prduct_number,
                    p1.cat_id AS category_id,
                    p1.prd_nm AS prduct_name,
                    p1.prd_cost AS prduct_cost,
                    p1.prd_start_dt AS prduct_start_date,
                    p1.prd_end_dt AS prduct_end_date, -- filter Out historical data ! 
                    p2.cat as category,
                    p2.subcat AS subcategory,
                    p2.maintenance AS maintenance
                FROM silver.crm_prd_info p1
                    LEFT JOIN silver.erp_px_cat_g1v2 p2 
                    ON p1.cat_id = p2.ID
                WHERE prduct_end_date is null)
        GROUP BY prduct_number
        HAVING product_key_counted > 1;

-- 5) Checking the Gold.fact_sales view 
SELECT * from gold.fact_sales;


-- 6) Checking data integrity between fact and dimension views
SELECT * FROM GOLD.fact_sales f
LEFT JOIN GOLD.dim_customers d
ON f.customer_key = d.customer_key
LEFT JOIN GOLD.dim_products p
ON f.product_key = p.product_key
WHERE p.product_key is null;