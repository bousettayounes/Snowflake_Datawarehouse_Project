INSERT INTO silver.crm_cst_info(
cst_id, 
cst_key, 
cst_first_name, 
cst_last_name, 
cst_marital_status, 
cst_gndr, 
cst_create_date)
-- Load the latest customer information FROM the bronze layer into the silver layer.
SELECT 
cst_id,
TRIM(cst_key),
TRIM(cst_first_name) as cst_first_name,
TRIM(cst_last_name) as cst_last_name,
Case 
    when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
    when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
    else 'n/a'
END cst_marital_status,
Case 
    when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
    when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
    else 'n/a'
END cst_gndr,
cst_create_date,
FROM  (
        SELECT * , ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as clm_flag
        FROM bronze.crm_cst_info
        WHERE cst_id is not null)
    where clm_flag = 1


-- Load the latest product information FROM the bronze layer into the silver layer.
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
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
TRIM(prd_nm) as prd_nm,
CAST(COALESCE(prd_cost,0) as DECIMAL(10,2)) as prd_cost,
CASE UPPER(TRIM(prd_line))
    when 'M' then 'Mountain'
    when 'R' then 'Road'
    when 'S' then 'Othher Sales'
    when 'T' then 'Touring'
    else 'n/a'
END prd_line,
CAST (prd_start_dt as DATE ) ,
CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) as Date) AS prd_end_dt ,
FROM bronze.crm_prd_info

SELECT 
    sls_cust_id,
    sls_prd_key, 
    Case when sls_order_dt =0 or len(sls_order_dt) !=8 THEN NULL
        ELSE CAST (sls_order_dt as VARCHAR(8))
    END as sls_order_dt,
    Case when sls_ship_dt =0 or len(sls_ship_dt) !=8 THEN NULL
        ELSE CAST (sls_ship_dt as VARCHAR(8))
    END as sls_ship_dt,
    Case when sls_due_dt =0 or len(sls_due_dt) !=8 THEN NULL
        ELSE CAST (sls_due_dt as VARCHAR(8))
    END as sls_due_dt,
    sls_ship_dt, 
    sls_due_dt, 
    sls_sales, 
    sls_quantity, 
    CAST(
        CASE 
            WHEN sls_price < 0 THEN ABS(sls_price)
            WHEN sls_price IS NULL THEN 0
            ELSE sls_price
        END AS DECIMAL(10,2)
    ) AS sls_price
FROM bronze.crm_sales_details




