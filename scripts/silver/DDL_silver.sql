/*
===============================================================================
 DDL Script : Create Silver Tables
 Layer      : Silver (Cleaned & Conformed Data)
 Database   : silver
===============================================================================

 Script Purpose:
 ----------------
 This script creates tables in the 'silver' schema for storing
 cleaned and standardized data sourced from the Bronze layer.

 Key Notes:
 ----------
 - Tables are dropped and recreated to allow schema re-definition
 - Data is sourced from Bronze tables (originally loaded from CSV files)
 - Minimal transformations are expected in Silver
 - dwh_create_date captures the record load timestamp

 Usage:
 ------
 Run this script once during initial setup or schema changes.
===============================================================================
*/

-- Explore and Understand the data
-- ----------------------------------
-- from source_crm 
-- ----------------------------------
-- Customer information
SELECT * FROM bronze.cust_info
LIMIT 1000;

-- Current and History Product information
SELECT * FROM bronze.prd_info 
LIMIT 1000;

-- Transactional Records about sales & orders
SELECT * FROM bronze.sales_details
LIMIT 1000;

-- --------------------------------------
-- from source_erp
-- ---------------------------------------

-- Extra customers information (Birthdate)
SELECT * FROM bronze.cust_az12
LIMIT 1000;

-- Location of customers(country)
SELECT * FROM bronze.loc_a101
LIMIT 1000;

-- Product categories
SELECT * FROM bronze.px_cat_g1v2
LIMIT 1000;



CREATE DATABASE IF NOT EXISTS silver;
USE silver;

-- =====================================================
-- CRM CUSTOMER INFORMATION
-- =====================================================
DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- CRM PRODUCT INFORMATION
-- =====================================================
DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          VARCHAR(50),
    prd_key         VARCHAR(50),
    prd_nm          VARCHAR(50),
    prd_cost        INT,
    prd_line        VARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- CRM SALES DETAILS
-- =====================================================
DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ERP LOCATION DATA
-- =====================================================
DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid             VARCHAR(50),
    cntry           VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ERP CUSTOMER DEMOGRAPHICS
-- =====================================================
DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ERP PRODUCT CATEGORY
-- =====================================================
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);




