/*
---------------------------------------------------------------------------------------------
Stored Procedure Name: bronze.load_bronze
Description: Loads data into the bronze layer of the Data Warehouse (Source -> Bronze).
EXECUTION : CALL bronze.load_bronze();
----------------------------------------------------------------------------------------------
Purpose:
--------
This script loads raw CRM and ERP data into the corresponding 'bronze' staging tables in Snowflake.
It clears existing data and imports fresh CSV files from the designated external stage. 

Steps:
------
1. Truncates each target table to ensure no residual data remains.
2. Loads data from CSV files located in the '@RAW_CRM_ERP_FILES' external stage.
3. Applies consistent CSV parsing options (e.g., delimiter, header skip, field enclosures).

Files Loaded:
-------------
- cust_info.csv      → crm_cst_info
- prd_info.csv       → crm_prd_info
- sales_details.csv  → crm_sales_details
- CUST_AZ12.csv      → erp_cust_AZ12  ⚠️ (Double-check source file)
- LOC_A101.csv       → erp_loc_A101
- PX_CAT_G1V2.csv    → erp_px_cat_G1V2

Notes:
------
- Ensure all CSV file names and formats match expected structures.
- Reusing `sales_details.csv` for `erp_cust_AZ12` may be incorrect—verify the source file.
- Intended for initial or periodic data refresh in the raw staging layer.
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
  -- Start global timer
  full_start_time := CURRENT_TIMESTAMP();
  log_msg := '=== BRONZE LAYER LOADING PROCESS STARTED ===\n\n';

  -- 1) CRM Customer Info
  BEGIN
    log_msg := log_msg || '>> Processing CRM Customer Info\n';
    
    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE bronze.crm_cst_info;
    log_msg := log_msg || '   - Table bronze.crm_cst_info has been truncated (' || 
               CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';
    
    start_time := CURRENT_TIMESTAMP();
    COPY INTO bronze.crm_cst_info
      FROM @RAW_CRM_ERP_FILES/cust_info.csv
      FILE_FORMAT = (
        TYPE='CSV',
        FIELD_DELIMITER=',',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
      );
    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - File cust_info.csv loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM  || '\n\n';
  END;

  -- 2) CRM Product Info
  BEGIN
    log_msg := log_msg || '>> Processing CRM Product Info\n';
    
    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE bronze.crm_prd_info;
    log_msg := log_msg || '   - Table bronze.crm_prd_info has been truncated (' || 
               CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';
    
    start_time := CURRENT_TIMESTAMP();
    COPY INTO bronze.crm_prd_info
      FROM @RAW_CRM_ERP_FILES/prd_info.csv
      FILE_FORMAT = (
        TYPE='CSV',
        FIELD_DELIMITER=',',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
      );
    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - File prd_info.csv loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM  || '\n\n';
  END;

  -- 3) CRM Sales Details
  BEGIN
    log_msg := log_msg || '>> Processing CRM Sales Details\n';
    
    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE bronze.crm_sales_details;
    log_msg := log_msg || '   - Table bronze.crm_sales_details has been truncated (' || 
               CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';
    
    start_time := CURRENT_TIMESTAMP();
    COPY INTO bronze.crm_sales_details
      FROM @RAW_CRM_ERP_FILES/sales_details.csv
      FILE_FORMAT = (
        TYPE='CSV',
        FIELD_DELIMITER=',',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
      );
    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - File sales_details.csv loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM  || '\n\n';
  END;

  -- 4) ERP Customer Data (AZ12)
  BEGIN
    log_msg := log_msg || '>> Processing ERP Customer Data (AZ12)\n';
    
    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE bronze.erp_cust_AZ12;
    log_msg := log_msg || '   - Table bronze.erp_cust_AZ12 has been truncated (' || 
               CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';
    
    start_time := CURRENT_TIMESTAMP();
    COPY INTO bronze.erp_cust_AZ12
      FROM @RAW_CRM_ERP_FILES/CUST_AZ12.csv
      FILE_FORMAT = (
        TYPE='CSV',
        FIELD_DELIMITER=',',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
      );
    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - File CUST_AZ12.csv loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM  || '\n\n';
  END;

  -- 5) ERP Location Data (A101)
  BEGIN
    log_msg := log_msg || '>> Processing ERP Location Data (A101)\n';
    
    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE bronze.erp_loc_A101;
    log_msg := log_msg || '   - Table bronze.erp_loc_A101 has been truncated (' || 
               CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';
    
    start_time := CURRENT_TIMESTAMP();
    COPY INTO bronze.erp_loc_A101
      FROM @RAW_CRM_ERP_FILES/LOC_A101.csv
      FILE_FORMAT = (
        TYPE='CSV',
        FIELD_DELIMITER=',',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
      );
    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - File LOC_A101.csv loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM  || '\n\n';
  END;

  -- 6) ERP Product Category Mapping (G1V2)
  BEGIN
    log_msg := log_msg || '>> Processing ERP Product Category Mapping (G1V2)\n';
    
    truncate_time := CURRENT_TIMESTAMP();
    TRUNCATE TABLE bronze.erp_px_cat_G1V2;
    log_msg := log_msg || '   - Table bronze.erp_px_cat_G1V2 has been truncated (' || 
               CAST(DATEDIFF(MILLISECOND, truncate_time, CURRENT_TIMESTAMP()) AS STRING) || ' ms)\n';
    
    start_time := CURRENT_TIMESTAMP();
    COPY INTO bronze.erp_px_cat_G1V2
      FROM @RAW_CRM_ERP_FILES/PX_CAT_G1V2.csv
      FILE_FORMAT = (
        TYPE='CSV',
        FIELD_DELIMITER=',',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"'
      );
    end_time := CURRENT_TIMESTAMP();
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS STRING);
    log_msg := log_msg || '   - File PX_CAT_G1V2.csv loaded successfully\n';
    log_msg := log_msg || '   - Operation completed in ' || duration || ' seconds\n\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '   ❌ ERROR: ' || SQLERRM  || '\n\n';
  END;

  -- End global timer
  full_end_time := CURRENT_TIMESTAMP();
  full_duration := CAST(DATEDIFF(SECOND, full_start_time, full_end_time) AS STRING);

  -- Append total duration message
  log_msg := log_msg || '=== BRONZE LAYER LOADING PROCESS COMPLETED ===\n';
  log_msg := log_msg || 'Total duration: ' || full_duration || ' seconds\n';
  log_msg := log_msg || 'Process completed at: ' || TO_VARCHAR(full_end_time, 'YYYY-MM-DD HH24:MI:SS.FF3');

  RETURN log_msg;
END;
$$;
