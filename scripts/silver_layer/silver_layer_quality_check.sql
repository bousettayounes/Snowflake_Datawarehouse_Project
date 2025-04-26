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

----------------------------------------------------------------------------------------------------
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

----------------------------------------------------------------------------------------------------
-- Checking for Nulls or duplicates in the primary_key SILVER.CRM_SALES_DETAILS
--expectation :  No nulls or duplicates in the primary key
select sls_ord_num,count(*) as id_appearence 
from silver.crm_sales_details
group by sls_ord_num
having id_appearence > 1 or sls_ord_num is null

------------------------------------------------------------------------------------
-- check unwanted spaces in the string columns  
-- expectation : No unwanted spaces in the string columns
select sls_ord_num from bronze.crm_sales_details where sls_ord_num != TRIM(sls_ord_num)
select sls_prd_key from bronze.crm_sales_details where sls_prd_key != TRIM(sls_prd_key)

------------------------------------------------------------------------------------
-- check NULLS or Negative numbers
-- expectation : No NULLS or Negative numbers in the sales amount and quantity columns
select SLS_CUST_ID from bronze.crm_sales_details where SLS_CUST_ID is null or SLS_CUST_ID < 0
select SLS_ORDER_DT from bronze.crm_sales_details where SLS_ORDER_DT is null or SLS_ORDER_DT < 0
select SLS_SHIP_DT from bronze.crm_sales_details where SLS_SHIP_DT is null or SLS_SHIP_DT < 0
select SLS_DUE_DT from bronze.crm_sales_details where SLS_DUE_DT is null or SLS_DUE_DT < 0
select SLS_SALES from bronze.crm_sales_details where SLS_SALES is null or SLS_SALES < 0
select SLS_QUANTITY from bronze.crm_sales_details where SLS_QUANTITY is null or SLS_QUANTITY < 0
select SLS_PRICE from bronze.crm_sales_details where SLS_PRICE is null or SLS_PRICE < 0

------------------------------------------------------------------------------------
-- check date format  , orders of dates
--  expectation : date format should be YYYYMMDD , not 0 or null , and outliers dates should be checked the boundaries
--  (e.g., 1900-01-01, 2100-12-31)
or SLS_ORDER_DT >= 21001231 
or SLS_ORDER_DT <= 19000101
select nullif(SLS_SHIP_DT,0) from bronze.crm_sales_details where SLS_SHIP_DT <=0 
or LEN(SLS_SHIP_DT) !=8
or SLS_SHIP_DT >= 21001231 
or SLS_SHIP_DT <= 19000101
select nullif(SLS_DUE_DT,0) from bronze.crm_sales_details where SLS_DUE_DT <=0 
or LEN(SLS_DUE_DT) !=8
or SLS_DUE_DT >= 21001231 
or SLS_DUE_DT <= 19000101
--order of date columns 
select SLS_ORDER_DT, SLS_SHIP_DT, SLS_DUE_DT from bronze.crm_sales_details 
where SLS_ORDER_DT > SLS_SHIP_DT or SLS_ORDER_DT > SLS_DUE_DT or SLS_SHIP_DT > SLS_DUE_DT

