/*
=====================================================================
Stored Procedure : Load Silver Layer (Bronze → Silver)
Schema           : silver
Procedure Name   : proc_load_silver
Database         : MySQL
=====================================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load)
    process to populate the 'silver' schema tables from the 'bronze'
    schema with cleaned, standardized, and validated data.

Actions Performed:
    - Loads latest and deduplicated customer data into silver.cust_info
    - Loads transformed product data with effective date ranges into
      silver.prd_info
    - Loads cleansed sales transactions into silver.sales_details
    - Standardizes customer demographic data into silver.cust_az12
    - Normalizes country data into silver.loc_a101
    - Loads product category reference data into silver.px_cat_g1v2
    - Prints step-level execution messages
    - Captures execution duration for each load step
    - Captures total Silver layer load duration
    - Implements robust error handling with diagnostic details

Parameters:
    None.
    This stored procedure does not accept any parameters
    and does not return any values.

Usage Example:
    CALL silver.proc_load_silver();

=====================================================================
*/


DROP PROCEDURE IF EXISTS silver.proc_load_silver;
DELIMITER $$

CREATE PROCEDURE silver.proc_load_silver()
BEGIN
    /* ===============================
       Variables
    =============================== */
    DECLARE v_batch_start DATETIME;
    DECLARE v_batch_end DATETIME;
    DECLARE v_step_start DATETIME;
    DECLARE v_step_end DATETIME;
    DECLARE v_step_duration INT;

    DECLARE v_error_msg TEXT;
    DECLARE v_error_no INT;
    DECLARE v_sqlstate CHAR(5);

    /* ===============================
       Error Handling
    =============================== */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            v_error_no = MYSQL_ERRNO,
            v_sqlstate = RETURNED_SQLSTATE,
            v_error_msg = MESSAGE_TEXT;

        SELECT '============================================' AS msg;
        SELECT 'Silver Load FAILED' AS status;
        SELECT CONCAT('Error Number : ', v_error_no) AS error_no;
        SELECT CONCAT('SQL State    : ', v_sqlstate) AS sql_state;
        SELECT CONCAT('Error Msg    : ', v_error_msg) AS error_message;
        SELECT '============================================' AS msg;
    END;

    /* ===============================
       Batch Start
    =============================== */
    SET v_batch_start = NOW();
    SELECT '============================================' AS msg;
    SELECT 'Silver Load Started' AS status;
    SELECT CONCAT('Batch Start Time : ', v_batch_start) AS batch_start;
    SELECT '============================================' AS msg;

    /* ============================
       cust_info
    ============================ */
    SET v_step_start = NOW();
    SELECT 'Loading silver.cust_info' AS step;

    INSERT INTO silver.cust_info (
         cst_id, cst_key, cst_firstname, cst_lastname,
         cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT 
        cst_id, cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
             WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
             ELSE 'N/A' END,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
             WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
             ELSE 'N/A' END,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_last
        FROM bronze.cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE Flag_last = 1;
    
     -- final look
     SELECT * FROM silver.cust_info;

    SET v_step_end = NOW();
    SET v_step_duration = TIMESTAMPDIFF(SECOND, v_step_start, v_step_end);
    SELECT CONCAT('cust_info loaded in ', v_step_duration, ' seconds') AS duration;

    /* ============================
       prd_info
    ============================ */
    SET v_step_start = NOW();
    SELECT 'Loading silver.prd_info' AS step;

    INSERT INTO silver.prd_info (
        prd_id, cat_id, prd_key, prd_nm,
        prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7),
        prd_nm,
        IFNULL(prd_cost, 0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'N/A'
        END,
        DATE(prd_start_dt),
        COALESCE(
            DATE_SUB(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
                INTERVAL 1 DAY
            ),
            '9999-12-31'
        )
    FROM bronze.prd_info
    WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')
          NOT IN (SELECT DISTINCT id FROM bronze.px_cat_g1v2)
      AND SUBSTRING(prd_key, 7)
          NOT IN (SELECT sls_prd_key FROM bronze.sales_details);
    
    
	-- Final look
    SELECT * FROM silver.prd_info;
    
    
    SET v_step_end = NOW();
    SET v_step_duration = TIMESTAMPDIFF(SECOND, v_step_start, v_step_end);
    SELECT CONCAT('prd_info loaded in ', v_step_duration, ' seconds') AS duration;

    /* ============================
       sales_details
    ============================ */
    SET v_step_start = NOW();
    SELECT 'Loading silver.sales_details' AS step;

    INSERT INTO silver.sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT
        TRIM(sls_ord_num),
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) <> 8 THEN NULL
             ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d') END,
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) <> 8 THEN NULL
             ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d') END,
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) <> 8 THEN NULL
             ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d') END,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0
                  OR sls_sales <> sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales END,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price END
    FROM bronze.sales_details;
    
    -- final result
    SELECT * FROM silver.sales_details;

    SET v_step_end = NOW();
    SET v_step_duration = TIMESTAMPDIFF(SECOND, v_step_start, v_step_end);
    SELECT CONCAT('sales_details loaded in ', v_step_duration, ' seconds') AS duration;

    /* ============================
       cust_az12
    ============================ */
    SET v_step_start = NOW();
    SELECT 'Loading silver.cust_az12' AS step;

    INSERT INTO silver.cust_az12 (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
        CASE WHEN bdate > CURRENT_DATE() THEN NULL ELSE bdate END,
        CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
             WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
             ELSE 'N/A' END
    FROM bronze.cust_az12;
    
    -- Final look
    SELECT * FROM silver.cust_az12;

    SET v_step_end = NOW();
    SET v_step_duration = TIMESTAMPDIFF(SECOND, v_step_start, v_step_end);
    SELECT CONCAT('cust_az12 loaded in ', v_step_duration, ' seconds') AS duration;

    /* ============================
       loc_a101
    ============================ */
    SET v_step_start = NOW();
    SELECT 'Loading silver.loc_a101' AS step;

    INSERT INTO silver.loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', ''),
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
             WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
             WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
             ELSE TRIM(cntry) END
    FROM bronze.loc_a101;
	
    -- Final look
	SELECT * FROM silver.loc_a101;
    
    
    
    SET v_step_end = NOW();
    SET v_step_duration = TIMESTAMPDIFF(SECOND, v_step_start, v_step_end);
    SELECT CONCAT('loc_a101 loaded in ', v_step_duration, ' seconds') AS duration;

    /* ============================
       px_cat_g1v2
    ============================ */
    SET v_step_start = NOW();
    SELECT 'Loading silver.px_cat_g1v2' AS step;

    INSERT INTO silver.px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.px_cat_g1v2;
    
    -- Final look
    SELECT * FROM silver.px_cat_g1v2;

    SET v_step_end = NOW();
    SET v_step_duration = TIMESTAMPDIFF(SECOND, v_step_start, v_step_end);
    SELECT CONCAT('px_cat_g1v2 loaded in ', v_step_duration, ' seconds') AS duration;

    /* ===============================
       Batch End
    =============================== */
    SET v_batch_end = NOW();
    SELECT '============================================' AS msg;
    SELECT 'Silver Load SUCCESS' AS status;
    SELECT CONCAT(
        'Total Silver Load Duration : ',
        TIMESTAMPDIFF(SECOND, v_batch_start, v_batch_end),
        ' seconds'
    ) AS total_duration;
    SELECT '============================================' AS msg;

END$$

DELIMITER ;

CALL silver.proc_load_silver();





