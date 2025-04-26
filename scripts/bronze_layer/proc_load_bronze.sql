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
  duration VARCHAR;
  full_start_time TIMESTAMP;
  full_end_time TIMESTAMP;
  full_duration VARCHAR;
BEGIN
  -- Start global timer
  full_start_time := CURRENT_TIMESTAMP();

  -- 1) CRM Customer Info
  BEGIN
    TRUNCATE TABLE bronze.crm_cst_info;
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
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS VARCHAR);
    log_msg := log_msg || '✔ -CRM- bronze.crm_cst_info loaded successfully in ' || duration || ' seconds.\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '❌ -CRM- bronze.crm_cst_info failed: ' || ERROR_MESSAGE() || '\n';
  END;

  -- 2) CRM Product Info
  BEGIN
    TRUNCATE TABLE bronze.crm_prd_info;
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
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS VARCHAR);
    log_msg := log_msg || '✔ -CRM- bronze.crm_prd_info loaded successfully in ' || duration || ' seconds.\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '❌ -CRM- bronze.crm_prd_info failed: ' || ERROR_MESSAGE() || '\n';
  END;

  -- 3) CRM Sales Details
  BEGIN
    TRUNCATE TABLE bronze.crm_sales_details;
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
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS VARCHAR);
    log_msg := log_msg || '✔ -CRM- bronze.crm_sales_details loaded successfully in ' || duration || ' seconds.\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '❌ -CRM- bronze.crm_sales_details failed: ' || ERROR_MESSAGE() || '\n';
  END;

  -- 4) ERP Customer Data (AZ12)
  BEGIN
    TRUNCATE TABLE bronze.erp_cust_AZ12;
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
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS VARCHAR);
    log_msg := log_msg || '✔ -ERP- bronze.erp_cust_AZ12 loaded successfully in ' || duration || ' seconds.\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '❌ -ERP- bronze.erp_cust_AZ12 failed: ' || ERROR_MESSAGE() || '\n';
  END;

  -- 5) ERP Location Data (A101)
  BEGIN
    TRUNCATE TABLE bronze.erp_loc_A101;
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
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS VARCHAR);
    log_msg := log_msg || '✔ -ERP- bronze.erp_loc_A101 loaded successfully in ' || duration || ' seconds.\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '❌ -ERP- bronze.erp_loc_A101 failed: ' || ERROR_MESSAGE() || '\n';
  END;

  -- 6) ERP Product Category Mapping (G1V2)
  BEGIN
    TRUNCATE TABLE bronze.erp_px_cat_G1V2;
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
    duration := CAST(DATEDIFF(SECOND, start_time, end_time) AS VARCHAR);
    log_msg := log_msg || '✔ -ERP- bronze.erp_px_cat_G1V2 loaded successfully in ' || duration || ' seconds.\n';
  EXCEPTION
    WHEN OTHER THEN
      log_msg := log_msg || '❌ -ERP- bronze.erp_px_cat_G1V2 failed: ' || ERROR_MESSAGE() || '\n';
  END;

  -- End global timer
  full_end_time := CURRENT_TIMESTAMP();
  full_duration := CAST(DATEDIFF(SECOND, full_start_time, full_end_time) AS VARCHAR);

  -- Append total duration message
  log_msg := log_msg || 'Loading Data into bronze layer is completed in ' || full_duration || ' seconds.';

  RETURN log_msg;
END;
$$;


CALL bronze.load_bronze();