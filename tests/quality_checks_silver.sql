/*
=====================================================================
Script Name      : Silver Layer Data Quality & Validation Checks
Layer            : Bronze → Silver
Database         : MySQL
=====================================================================

Script Purpose:
    This script performs comprehensive data quality validation,
    consistency checks, and transformation verification between
    the Bronze and Silver layers.

    It is designed to validate that data cleansing, standardization,
    and business rules applied during the Silver load process are
    correctly implemented and enforced.

Actions Performed:
    - Validates primary key integrity (NULLs & duplicates)
    - Identifies and verifies deduplication logic using ROW_NUMBER()
    - Detects unwanted leading/trailing spaces in string columns
    - Verifies data standardization in low-cardinality fields
      (gender, product line, country, categories)
    - Checks numeric integrity (NULLs, negative values, zero values)
    - Validates date formats, ranges, and logical date ordering
    - Confirms derived date logic (effective start & end dates)
    - Ensures data consistency between related columns
      (sales = quantity × price)
    - Compares Bronze vs Silver outputs to ensure transformation accuracy
    - Provides final inspection queries for all Silver tables

Tables Covered:
    - bronze.cust_info        → silver.cust_info
    - bronze.prd_info         → silver.prd_info
    - bronze.sales_details    → silver.sales_details
    - bronze.cust_az12        → silver.cust_az12
    - bronze.loc_a101         → silver.loc_a101
    - bronze.px_cat_g1v2      → silver.px_cat_g1v2

Parameters:
    None.
    This script does not accept parameters and is intended
    for validation, auditing, and quality assurance purposes only.

Expected Outcome:
    - Bronze checks may return rows indicating data issues
    - Silver checks are expected to return NO ROWS
      (unless data quality rules are violated)

Usage:
    Execute manually after Silver load completion
    to validate data quality and transformation correctness.

=====================================================================
*/


-- ===================================
--              cust_info
-- ===================================
-- Data transformation and data cleansing in cust_info table
-- Check for null or duplicates in primary key
-- ---------------------------------------
--               Bronze

SELECT  
cst_id,
COUNT(*)
FROM bronze.cust_info
GROUP BY cst_id
HAVING COUNT(*) = 1 OR cst_id IS NULL;

SELECT * 
FROM (
	SELECT * ,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_last
	FROM bronze.cust_info
)t
WHERE Flag_last = 1 AND cst_id = 11177;

-- Check for unwanted spaces in string values
-- TRIM() just in case spaces appear later in your columns
-- In firstname

SELECT cst_firstname
FROM bronze.cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- In lastname
SELECT cst_lastname
FROM bronze.cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- In gndr
SELECT cst_gndr
FROM bronze.cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Check the data standardization & consistency of values in low cardinality columns
SELECT DISTINCT cst_gndr
FROM bronze.cust_info;

-- Quality check of the silver table
-- ---------------------------------------
--                    Silver

-- Expectation: No Results
SELECT cst_firstname
FROM silver.cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- In lastname
SELECT cst_lastname
FROM silver.cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- In gndr
SELECT cst_gndr
FROM silver.cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT DISTINCT cst_gndr
FROM silver.cust_info;

-- final look
SELECT * FROM silver.cust_info;

-- ===================================
--              prd_info
-- ===================================

--               Bronze
-- ---------------------------------------
-- Check for null or duplicates in primary key

SELECT  
prd_id,
COUNT(*)
FROM bronze.prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted Spaces
SELECT prd_nm
FROM bronze.prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULL or negative numbers
SELECT prd_cost
FROM bronze.prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check the data standardization & consistency of values in low cardinality columns
SELECT DISTINCT prd_line
FROM bronze.prd_info;

-- Check for Invalid Date Orders : end date = start date of next record - 1
SELECT *
FROM bronze.prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1  AS prd_end_dt_test
FROM bronze.prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');

-- Quality check of the silver table
-- ---------------------------------------
--                   Silver

-- Expectation: No Results
SELECT  
prd_id,
COUNT(*)
FROM silver.prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted Spaces
SELECT prd_nm
FROM silver.prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULL or negative numbers
SELECT prd_cost
FROM silver.prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check the data standardization & consistency of values in low cardinality columns
SELECT DISTINCT prd_line
FROM silver.prd_info;

-- Check for Invalid Date Orders : end date = start date of next record - 1
SELECT *
FROM silver.prd_info
WHERE prd_end_dt < prd_start_dt;

-- Final look
SELECT * FROM silver.prd_info;


-- ===================================
--             sales_details
-- ===================================

--               Bronze
-- ---------------------------------------

-- check join the column  
-- WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.prd_info)
-- WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.cust_info);

-- Check for Invalid Dates
SELECT 
    NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.sales_details
WHERE sls_order_dt <= 0 -- negative numbers or zeros can't be cast to a date
OR LENGTH(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101;   -- check for outliers by validating the boundaries of the date range
 
-- Check for Invalid Dates 
SELECT 
    NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM bronze.sales_details
WHERE sls_ship_dt <= 0 -- negative numbers or zeros can't be cast to a date
OR LENGTH(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101;

-- Check for Invalid Dates 
SELECT 
    NULLIF(sls_due_dt,0) AS sls_due_dt
FROM bronze.sales_details
WHERE sls_due_dt <= 0 -- negative numbers or zeros can't be cast to a date
OR LENGTH(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101;


-- Check for Invalid Date orders
SELECT 
*
FROM bronze.sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


-- check data consistency: between sales, Quantity and Price
-- >> sales = Quantity * Price
-- >> values must be NULL, zero or negative

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.sales_details
WHERE sls_sales <> sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


--               Silver
-- ---------------------------------------

-- Check for Invalid Date orders
SELECT 
*
FROM silver.sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


-- final result
SELECT * FROM silver.sales_details;


-- ===================================
--             cust_az12
-- ===================================

--               Bronze
-- ---------------------------------------

-- WHERE cid NOT IN (SELECT DISTINCT cst_key FROM silver.cust_info);

-- Identify out-of-range dates

SELECT DISTINCT
    bdate
FROM bronze.cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > CURRENT_DATE();

-- Data Standardization and consistency
SELECT distinct
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
     ELSE 'N/A'
END AS gen
FROM bronze.cust_az12;

--               Silver
-- ---------------------------------------

SELECT DISTINCT gen
FROM silver.cust_az12;

-- Final look
SELECT * FROM silver.cust_az12;

-- ===================================
--             loc_a101
-- ===================================

--               Bronze
-- ---------------------------------------
SELECT 
REPLACE(cid,'-','') AS cid,
cntry
FROM bronze.loc_a101
WHERE cid NOT IN  (
SELECT cst_key FROM silver.cust_info);

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.loc_a101
ORDER BY cntry;

SELECT 
REPLACE(cid,'-','') AS cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
    WHEN TRIM(cntry) = ' ' OR cntry IS NULL THEN 'N/A'
    ELSE TRIM(cntry)
END AS cntry
FROM bronze.loc_a101
WHERE cid NOT IN (
SELECT cst_key FROM silver.cust_info);


-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.loc_a101;

--               Silver
-- ---------------------------------------
-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.loc_a101;

-- Final look
SELECT * FROM silver.loc_a101;

-- ===================================
--             px_cat_g1v2
-- ===================================

--               Bronze
-- ---------------------------------------
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.px_cat_g1v2;

-- Check the unwanted Spaces
SELECT * FROM bronze.px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) ;

-- Data Standardization & Consistency

SELECT DISTINCT 
cat,
subcat,
maintenance
FROM bronze.px_cat_g1v2;


-- Final look
SELECT * FROM silver.px_cat_g1v2;
