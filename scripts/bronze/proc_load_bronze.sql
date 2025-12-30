/*
================================================================================
 Stored Procedure : proc_load_bronze
 Layer            : Bronze (Raw Ingestion Layer)
 Database         : bronze
================================================================================

 Script Purpose:
 ----------------
 This stored procedure represents the Bronze layer batch process.
 It is responsible for validating and monitoring raw data that has
 been ingested into the bronze schema from external CSV files.

 The procedure performs the following actions:
 - Captures batch start time and end time
 - Calculates total batch duration
 - Validates data availability in Bronze tables
 - Handles errors with detailed diagnostics (error number, SQLSTATE, message)
 - Displays execution status messages for monitoring

 Source Systems:
 ----------------
 - CRM System (Customer, Product, Sales data)
 - ERP System (Customer demographics, Location, Product category)

 Data Ingestion Method:
 ----------------------
 - Data is imported directly from CSV files into Bronze tables
   using file-based ingestion (CSV import / LOAD DATA INFILE).
 - No transformations are applied in the Bronze layer.

 Parameters:
 -----------
 None
 This stored procedure does not accept any input parameters
 and does not return any output values.

 Usage Example:
 --------------
 CALL bronze.proc_load_bronze();

================================================================================
*/


USE bronze;
SHOW TABLES;

DROP PROCEDURE IF EXISTS bronze.proc_load_bronze;

DELIMITER $$

CREATE PROCEDURE bronze.proc_load_bronze()
BEGIN
    -- Batch timing variables
    DECLARE v_batch_start_time DATETIME;
    DECLARE v_batch_end_time   DATETIME;
    DECLARE v_batch_duration_sec INT;

    -- Error info
    DECLARE v_error_message TEXT;
    DECLARE v_error_number INT;
    DECLARE v_sqlstate CHAR(5);

    -- CATCH block
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_batch_end_time = NOW();
        SET v_batch_duration_sec =
            TIMESTAMPDIFF(SECOND, v_batch_start_time, v_batch_end_time);

        GET DIAGNOSTICS CONDITION 1
            v_error_number = MYSQL_ERRNO,
            v_sqlstate     = RETURNED_SQLSTATE,
            v_error_message = MESSAGE_TEXT;

        SELECT '=============================================' AS msg;
        SELECT 'Batch FAILED' AS status;
        SELECT CONCAT('Batch Start Time : ', v_batch_start_time) AS batch_start;
        SELECT CONCAT('Batch End Time   : ', v_batch_end_time) AS batch_end;
        SELECT CONCAT('Batch Duration   : ', v_batch_duration_sec, ' seconds') AS batch_duration;
        SELECT CONCAT('Error Number     : ', v_error_number) AS error_number;
        SELECT CONCAT('SQL State        : ', v_sqlstate) AS sql_state;
        SELECT CONCAT('Error Message    : ', v_error_message) AS error_message;
        SELECT '=============================================' AS msg;
    END;

    -- BATCH START
    SET v_batch_start_time = NOW();

    SELECT '=============================================' AS msg;
    SELECT 'Bronze Batch Load Started' AS status;
    SELECT CONCAT('Batch Start Time : ', v_batch_start_time) AS batch_start;
    SELECT '=============================================' AS msg;

    -- ================== LOAD LOGIC ==================
    SELECT * FROM bronze.cust_info;
    SELECT * FROM bronze.prd_info;
    SELECT * FROM bronze.sales_details;
    SELECT * FROM bronze.cust_az12;
    SELECT * FROM bronze.loc_a101;
    SELECT * FROM bronze.px_cat_g1v2;
    -- =================================================

    -- BATCH END
    SET v_batch_end_time = NOW();
    SET v_batch_duration_sec =
        TIMESTAMPDIFF(SECOND, v_batch_start_time, v_batch_end_time);

    SELECT '=============================================' AS msg;
    SELECT 'Bronze Batch Load SUCCESS' AS status;
    SELECT CONCAT('Batch Start Time : ', v_batch_start_time) AS batch_start;
    SELECT CONCAT('Batch End Time   : ', v_batch_end_time) AS batch_end;
    SELECT CONCAT('Batch Duration   : ', v_batch_duration_sec, ' seconds') AS batch_duration;
    SELECT '=============================================' AS msg;

END$$

DELIMITER ;



CALL bronze.proc_load_bronze();

