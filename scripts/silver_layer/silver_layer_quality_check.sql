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

