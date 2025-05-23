/*
---------------------------------------------------------------------------------------------------------------
Description: Validates the quality, consistency, and standardization of the silver layer tables.
----------------------------------------------------------------------------------------------------------------
Purpose:
--------
This script checks for nulls, duplicates, unwanted spaces, invalid data, and standardization issues 
in the cleaned and processed data stored in the silver layer.

Steps:
------
1. Check for nulls or duplicates in primary keys across silver tables.
2. Detect unwanted spaces in key string columns.
3. Validate numeric fields for nulls, negatives, and logical correctness.
4. Confirm data standardization in gender, marital status, product categories, etc.
5. Ensure date fields are within valid ranges and maintain correct logical ordering.

Tables/Processes Validated:
----------------------------
- silver.crm_cst_info
- silver.crm_prd_info
- silver.crm_sales_details
- silver.erp_cust_AZ12
- silver.erp_loc_A101
- silver.erp_px_cat_G1V2

Notes:
------
- Primary key columns must not contain NULLs or duplicates.
- String columns must be trimmed and free of leading/trailing spaces.
- Numeric columns like cost, price, quantity, and sales must be non-null and non-negative.
- Date columns must fall within logical boundaries (1900-01-01 to 2100-12-31) and correct order.
- Standardized fields (gender, marital status, etc.) should contain only expected values.
- Run this validation procedure after each Silver load to catch issues early.
*/
-------------------------------------------------------------------------------------------------------
-- Checking for Nulls or duplicates in the primary_key
--expectation :  No nulls or duplicates in the primary key
SELECT cst_id,count(*) as id_appearence FROM silver.crm_cst_info
group by  cst_id 
Having id_appearence > 1 OR  cst_id is null 
order by id_appearence DESC 


-- check unwanted spaces in the string columns
--expectation : No unwanted spaces in the string columns
SELECT cst_key from silver.crm_cst_info where cst_key != trim(cst_key)
SELECT cst_first_name from silver.crm_cst_info where cst_first_name != trim(cst_first_name)
SELECT cst_last_name from silver.crm_cst_info where cst_last_name != trim(cst_last_name)


-- Data standardization & consistency check
Select distinct cst_marital_status from silver.crm_cst_info
Select distinct cst_gndr from silver.crm_cst_info

-------------------------------------------------------------------------------------------------------
-- Checking for Nulls or duplicates in the primary_key SILVER.CRM_PRD_INFO
--expectation :  No nulls or duplicates in the primary key
Select prd_id , count(*) as id_appearence
from  silver.crm_prd_info 
group by prd_id 
having id_appearence > 1 

-- check unwanted spaces in the string columns
--expectation : No unwanted spaces in the string columns
Select prd_nm from silver.crm_prd_info where prd_nm != trim(prd_nm)


-- check NULLS or Negative numbers 
--expectation : No NULLS or Negative numbers in the cost column
Select prd_cost from silver.crm_prd_info where prd_cost is null or prd_cost < 0

-- Data standardization & consistency check
--expectation : data should be standardized
Select distinct prd_line from silver.crm_prd_info 

--check for invalid date orders
--expectation : prd_start_dt should be less than prd_end_dt
Select prd_id,prd_start_dt,prd_end_dt from silver.crm_prd_info where prd_start_dt > prd_end_dt

--------------------------------------------------------------------------------------------------------
-- Checking for Nulls or duplicates in the primary_key SILVER.CRM_SALES_DETAILS
--expectation :  No nulls or duplicates in the primary key
select sls_ord_num,count(*) as id_appearence 
from silver.crm_sales_details
group by sls_ord_num
having id_appearence > 1 or sls_ord_num is null

------------------------------------------------------------------------------------------------------
-- check unwanted spaces in the string columns  
-- expectation : No unwanted spaces in the string columns
select sls_ord_num from silver.crm_sales_details where sls_ord_num != TRIM(sls_ord_num)
select sls_prd_key from silver.crm_sales_details where sls_prd_key != TRIM(sls_prd_key)

------------------------------------------------------------------------------------------------------
-- check NULLS or Negative numbers
-- expectation : No NULLS or Negative numbers in the sales amount and quantity columns
select * from silver.crm_sales_details where SLS_CUST_ID is null 
select * from silver.crm_sales_details where SLS_ORDER_DT is null -- the nulls represents the currepted date 
select * from silver.crm_sales_details where SLS_SHIP_DT is null
select * from silver.crm_sales_details where SLS_DUE_DT is null 
select * from silver.crm_sales_details where SLS_SALES is null or SLS_SALES < 0
select * from silver.crm_sales_details where SLS_QUANTITY is null or SLS_QUANTITY < 0
select * from silver.crm_sales_details where SLS_PRICE is null or SLS_PRICE < 0
select * from silver.crm_sales_details where SLS_PRICE*SLS_QUANTITY != SLS_SALES
------------------------------------------------------------------------------------------------------
-- check date format  , orders of dates
--  expectation : date format should be YYYYMMDD , not 0 or null , and outliers dates should be checked the boundaries
--  (e.g., 1900-01-01, 2100-12-31)
select nullif(SLS_ORDER_DT,0) from silver.crm_sales_details where SLS_ORDER_DT <=0 
or SLS_ORDER_DT >= 21001231 
or SLS_ORDER_DT <= 19000101
select nullif(SLS_SHIP_DT,0) from silver.crm_sales_details where SLS_SHIP_DT <=0 
or LEN(SLS_SHIP_DT) !=8
or SLS_SHIP_DT >= 21001231 
or SLS_SHIP_DT <= 19000101
select nullif(SLS_DUE_DT,0) from silver.crm_sales_details where SLS_DUE_DT <=0 
or LEN(SLS_DUE_DT) !=8
or SLS_DUE_DT >= 21001231 
or SLS_DUE_DT <= 19000101
--order of date columns 
select SLS_ORDER_DT, SLS_SHIP_DT, SLS_DUE_DT from silver.crm_sales_details 
where SLS_ORDER_DT > SLS_SHIP_DT or SLS_ORDER_DT > SLS_DUE_DT or SLS_SHIP_DT > SLS_DUE_DT

-- CHECK PRICE COLUMN   
-- expectation : price should be positive and not null
select  sls_sales , sls_quantity , sls_price from silver.crm_sales_details  
where sls_price*sls_quantity != sls_sales  OR  sls_price  is null OR sls_sales is null or sls_price <=0  or sls_quantity is null 


-------------------------------------------------------------------------------------------------------
-- check ERP Customer Demographics (System AZ12)
-- expectation : No NULLS or duplicates in the primary key and no unwanted spaces in the string columns 
SELECT cid from silver.erp_cust_AZ12 where cid != trim(cid);
SELECT cid  from silver.erp_cust_AZ12 where cid like'NAS%'
-------------------------------------------------------------------------------------------------------
-- check bdate column range
-- expectation : bdate should be in the range of 1900-01-01 and 2100-12-31
SELECT * 
FROM silver.erp_cust_AZ12 
WHERE bdate < '1900-01-01' OR bdate  > GETDATE();
-------------------------------------------------------------------------------------------------------
-- check GEN column values
-- expectation : GEN should be either 'Male ' or 'Female'
Select DISTINCT gen from silver.erp_cust_AZ12 
-------------------------------------------------------------------------------------------------------
-- check ERP RONZE.ERP_LOC_A101 
-- expectation : No NULLS or duplicates in the primary key and no unwanted spaces in the string columns 
select cid from silver.erp_loc_A101 where cid != trim(cid);
Select * from 
(select TRIM(REPLACE(cid,'-','')) as cid from silver.erp_loc_A101) where cid in (select cid from silver.erp_cust_az12);

-- check Ecntry column values
-- expectation : No duplicates in the country column and no unwanted spaces in the string columns
select Distinct cntry  from silver.erp_loc_A101 where cntry!=TRIM(cntry)
-------------------------------------------------------------------------------------------------------
-- check erp product category mapping (system G1V2)
-- expectation : No NULLS or duplicates in the primary key and no unwanted spaces in the string columns
select id from silver.erp_px_cat_g1v2 where id != trim(id);
Select cat from silver.erp_px_cat_g1v2 where cat != trim(cat);
Select Distinct cat from silver.erp_px_cat_g1v2 ; 
Select SUBCAT as subcat from silver.erp_px_cat_g1v2 where SUBCAT != trim(SUBCAT);
Select Distinct SUBCAT from silver.erp_px_cat_g1v2 ; 
Select MAINTENANCE from silver.erp_px_cat_g1v2 where MAINTENANCE != trim(MAINTENANCE) ; 
Select Distinct MAINTENANCE from silver.erp_px_cat_g1v2 ; 
