/*
---------------------------------------------------------------------------------------------
Stored Procedure Name: silver.load_silver
Description: Loads cleaned and standardized data into the silver layer of the Data Warehouse (Bronze -> Silver).
EXECUTION : CALL silver.load_silver();
----------------------------------------------------------------------------------------------
Purpose:
--------
This script transforms and loads processed CRM and ERP data from the 'bronze' staging tables into the 'silver' layer in Snowflake.
It applies data cleaning, standardization, and minor corrections before inserting into silver tables.

Steps:
------
1. Truncates each silver target table to remove old data.
2. Transforms and inserts the latest, cleaned data from bronze tables into silver tables.
3. Applies business logic like gender/marital status mapping, price and sales corrections, and category normalization.
4. Handles exceptions at each step, logging errors while allowing the process to continue.

Files/Processes Loaded:
------------------------
- bronze.crm_cst_info     → silver.crm_cst_info
- bronze.crm_prd_info     → silver.crm_prd_info
- bronze.crm_sales_details → silver.crm_sales_details
- bronze.erp_cust_AZ12    → silver.erp_cust_AZ12
- bronze.erp_loc_A101     → silver.erp_loc_A101
- bronze.erp_px_cat_g1v2  → silver.erp_px_cat_G1V2

Notes:
------
- Only the latest customer records are kept (based on the most recent create date).
- Marital status and gender fields are standardized to human-readable values.
- Product keys are split to create category IDs, and missing dates/prices/sales values are fixed.
- Country names are normalized for consistency across different spellings.
- Ensure bronze tables are fully loaded before running this procedure to maintain data integrity.
- Each load step includes exception handling to log errors and continue processing.
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  log_msg STRING := '';
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  truncate_time TIMESTAMP;
  duration STRING;
  full_start_time TIMESTAMP;
  full_end_time TIMESTAMP;
  full_duration STRING;
BEGIN

-- Full procedure start
full_start_time := CURRENT_TIMESTAMP();
log_msg := '=== SILVER LAYER LOADING PROCESS STARTED ===\n\n';

-- 1) Load CRM Customer Info
BEGIN
    log_msg := log_msg || '>> Processing CRM Customer Info\n';

    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE silver.crm_cst_info;
    log_msg := log_msg || '   - Table silver.crm_cst_info has been truncated (' || 
              CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';

    start_time := CURRENT_TIMESTAMP();
    INSERT INTO silver.crm_cst_info (
        cst_id, 
        cst_key, 
        cst_first_name, 
        cst_last_name, 
        cst_marital_status, 
        cst_gndr, 
        cst_create_date
    )
    SELECT 
        cst_id,
        TRIM(cst_key),
        TRIM(cst_first_name),
        TRIM(cst_last_name),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS clm_flag
        FROM bronze.crm_cst_info
        WHERE cst_id IS NOT NULL
    ) AS sub
    WHERE clm_flag = 1;

    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - Data loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
EXCEPTION
    WHEN OTHER THEN
        log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM || '\n\n';
END;


-- 2) Load CRM Product Info
BEGIN
    log_msg := log_msg || '>> Processing CRM Product Info\n';

    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE silver.crm_prd_info;
    log_msg := log_msg || '   - Table silver.crm_prd_info has been truncated (' || 
              CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';

    start_time := CURRENT_TIMESTAMP();
    INSERT INTO silver.crm_prd_info (
        prd_id,
        prd_key,
        cat_id,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
        prd_id,
        SUBSTRING(prd_key, 7) AS prd_key,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        TRIM(prd_nm),
        CAST(COALESCE(prd_cost, 0) AS DECIMAL(10,2)),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE)
    FROM bronze.crm_prd_info;

    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - Data loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
EXCEPTION
    WHEN OTHER THEN
        log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM || '\n\n';
END;


-- 3) Load CRM Sales Details
BEGIN
    log_msg := log_msg || '>> Processing CRM Sales Details\n';

    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE silver.crm_sales_details;
    log_msg := log_msg || '   - Table silver.crm_sales_details has been truncated (' || 
              CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';

    start_time := CURRENT_TIMESTAMP();
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
             ELSE TO_DATE(TO_VARCHAR(sls_order_dt), 'YYYYMMDD')
        END,
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
             ELSE TO_DATE(TO_VARCHAR(sls_ship_dt), 'YYYYMMDD')
        END,
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
             ELSE TO_DATE(TO_VARCHAR(sls_due_dt), 'YYYYMMDD')
        END,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        CASE 
            WHEN sls_quantity IS NULL OR sls_quantity <= 0 
                THEN ABS(sls_quantity)
            ELSE sls_quantity
        END,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN CAST(sls_sales / NULLIF(sls_quantity, 0) AS NUMBER(38,2))
            ELSE CAST(sls_price AS NUMBER(38,2))
        END
    FROM bronze.crm_sales_details;

    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - Data loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
EXCEPTION
    WHEN OTHER THEN
        log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM || '\n\n';
END;


-- 4) Load ERP Customer Info
BEGIN
    log_msg := log_msg || '>> Processing ERP Customer Info\n';

    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE silver.erp_cust_az12;
    log_msg := log_msg || '   - Table silver.erp_cust_az12 has been truncated (' || 
              CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';

    start_time := CURRENT_TIMESTAMP();
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN TRIM(SUBSTRING(cid, 4))
            ELSE cid
        END,
        CASE 
            WHEN bdate <= '1900-01-01' OR bdate > CURRENT_DATE() THEN NULL
            ELSE bdate
        END,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - Data loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
EXCEPTION
    WHEN OTHER THEN
        log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM || '\n\n';
END;


-- 5) Load ERP Customer Location
BEGIN
    log_msg := log_msg || '>> Processing ERP Customer Location\n';

    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE silver.erp_loc_a101;
    log_msg := log_msg || '   - Table silver.erp_loc_a101 has been truncated (' || 
              CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';

    start_time := CURRENT_TIMESTAMP();
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT 
        TRIM(REPLACE(cid, '-', '')),
        CASE 
            WHEN UPPER(TRIM(cntry)) IN ('US', 'UNITED STATES', 'USA') THEN 'United States'
            WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
            WHEN UPPER(TRIM(cntry)) IN ('CA', 'CANADA') THEN 'Canada'
            WHEN UPPER(TRIM(cntry)) IN ('UK', 'UNITED KINGDOM') THEN 'United Kingdom'
            WHEN UPPER(TRIM(cntry)) IN ('AU', 'AUSTRALIA') THEN 'Australia'
            WHEN UPPER(TRIM(cntry)) IN ('FR', 'FRANCE') THEN 'France'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - Data loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
EXCEPTION
    WHEN OTHER THEN
        log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM || '\n\n';
END;


-- 6) Load ERP Product Category
BEGIN
    log_msg := log_msg || '>> Processing ERP Product Category\n';

    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    log_msg := log_msg || '   - Table silver.erp_px_cat_g1v2 has been truncated (' || 
              CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';

    start_time := CURRENT_TIMESTAMP();
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        Maintenance
    )
    SELECT 
        id,
        cat,
        subcat,
        Maintenance
    FROM bronze.erp_px_cat_g1v2;

    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - Data loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
EXCEPTION
    WHEN OTHER THEN
        log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM || '\n\n';
END;


-- Full procedure end
full_end_time := CURRENT_TIMESTAMP();
full_duration := CAST(DATEDIFF(SECOND, full_start_time, full_end_time) AS STRING);
log_msg := log_msg || '=== SILVER LAYER LOADING PROCESS COMPLETED ===\n';
log_msg := log_msg || 'Total duration: ' || full_duration || ' seconds\n';
log_msg := log_msg || 'Process completed at: ' || TO_VARCHAR(full_end_time, 'YYYY-MM-DD HH24:MI:SS.FF3');

RETURN log_msg;

END;
$$;
