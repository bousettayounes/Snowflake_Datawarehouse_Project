TRUNCATE Table silver.crm_cst_info ; 
INSERT INTO silver.crm_cst_info(
cst_id, 
cst_key, 
cst_first_name, 
cst_lASt_name, 
cst_marital_status, 
cst_gndr, 
cst_create_date)
-- Load the latest customer infORmation FROM the bronze layer into the silver layer.
SELECT 
cst_id,
TRIM(cst_key),
TRIM(cst_first_name) AS cst_first_name,
TRIM(cst_lASt_name) AS cst_lASt_name,
CASe 
    when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
    when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
    else 'n/a'
END cst_marital_status,
CASe 
    when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
    when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
    else 'n/a'
END cst_gndr,
cst_create_date,
FROM  (
        SELECT * , ROW_NUMBER() over (partition by cst_id ORder by cst_create_date desc) AS clm_flag
        FROM bronze.crm_cst_info
        WHERE cst_id is not null)
    where clm_flag = 1

TRUNCATE Table silver.crm_prd_info ; 
-- Load the latest product infORmation FROM the bronze layer into the silver layer.
INSERT INTO silver.crm_prd_info(
prd_id,
prd_key,
cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
SELECT prd_id,
SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
TRIM(prd_nm) AS prd_nm,
CAST(COALESCE(prd_cost,0) AS DECIMAL(10,2)) AS prd_cost,
CASE UPPER(TRIM(prd_line))
    when 'M' then 'Mountain'
    when 'R' then 'Road'
    when 'S' then 'Othher Sales'
    when 'T' then 'Touring'
    else 'n/a'
END prd_line,
CAST (prd_start_dt AS DATE ) ,
CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS Date) AS prd_end_dt ,
FROM bronze.crm_prd_info


TRUNCATE Table silver.crm_sales_details ; 
INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price)
-- Load the latest sales details FROM the bronze layer into the silver layer.
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASe when sls_order_dt =0 OR len(sls_order_dt) !=8 THEN NULL
        ELSE CAST (CAST (sls_order_dt AS VARCHAR(8)) AS DATE)
    END AS sls_ORder_dt,
    CASe when sls_ship_dt =0 OR len(sls_ship_dt) !=8 THEN NULL
        ELSE CAST (CAST (sls_ship_dt AS VARCHAR(8)) AS DATE)
    END AS sls_ship_dt,
    CASe when sls_due_dt =0 OR len(sls_due_dt) !=8 THEN NULL
        ELSE CAST (CAST (sls_due_dt AS VARCHAR(8)) AS DATE)
    END AS sls_due_dt,  
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price) 
            ELSE sls_sales
        END AS sls_sales,
        
        CASE 
            WHEN sls_quantity IS NULL OR sls_quantity <= 0 
                THEN ABS(sls_quantity)
            ELSE sls_quantity
        END AS sls_quantity,
        
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN CAST((sls_sales / NULLIF(sls_quantity, 0)) AS NUMBER(38,2))
            ELSE CAST(sls_price AS NUMBER(38,2))
        END AS sls_price

FROM bronze.crm_sales_details

