/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================

Script Purpose:
This script creates views for the Gold layer in the data warehouse.

The Gold layer represents the final dimensional and fact tables
implemented using a Star Schema design.

These views combine, aggregate, and enrich cleansed data from the
Silver layer to produce analytics-ready, business-consumable datasets.

Objects Created:
- gold.dim_customers   : Customer dimension
- gold.dim_products    : Product dimension
- gold.fact_sales      : Sales fact table

Key Characteristics:
- Deduplication handled using GROUP BY and window functions
- Business rules applied for gender, country, and product categorization
- Surrogate keys generated using ROW_NUMBER()
- Referential integrity ensured via INNER JOINs in the fact view

Usage:
- These views can be queried directly for reporting, dashboards,
  and analytical workloads.
- Intended to be consumed by BI tools and downstream analytics.

Dependencies:
- silver.cust_info
- silver.cust_az12
- silver.loc_a101
- silver.prd_info
- silver.px_cat_g1v2
- silver.sales_details

===============================================================================
*/


-- =======================================
--          Dimension table: Customers
-- =======================================

-- Create the dimension Customers
DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id      AS customer_id,
    ci.cst_key     AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    MAX(la.cntry) AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr <> 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(MAX(ca.gen), 'N/A')
    END AS gender,
    MAX(ca.bdate) AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.cust_info ci
LEFT JOIN silver.cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.loc_a101 la ON ci.cst_key = la.cid
GROUP BY
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date;

-- Check the quality of the view
SELECT * FROM gold.dim_customers;

-- =======================================
--          Dimension table: Products
-- =======================================
-- Create the dimension Products
DROP VIEW IF EXISTS gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS product_key,
    prd_id AS product_id,
    prd_key AS product_number,
    prd_nm AS product_name,
    cat_id AS category_id,
    cat AS category,
    subcat AS subcategory,
    maintenance,
    prd_cost AS cost,
    prd_line AS product_line,
    prd_start_dt AS start_date    
FROM (
    SELECT
        pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_nm,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt,
        pc.cat,
        pc.subcat,
        pc.maintenance,
        ROW_NUMBER() OVER (
            PARTITION BY pn.prd_key
            ORDER BY pn.prd_start_dt DESC
        ) AS rn
    FROM silver.prd_info pn
    LEFT JOIN silver.px_cat_g1v2 pc
        ON pn.cat_id = pc.id
    WHERE pn.prd_end_dt IS NOT NULL
) t
WHERE rn = 1;

-- Check the quality of the view
SELECT * FROM gold.dim_products;

-- =======================================
--          fact table: sales_details
-- =======================================
-- create the fact table of sales_details
-- DROP VIEW IF EXISTS gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.sales_details sd
INNER JOIN gold.dim_products pr 
    ON sd.sls_prd_key = pr.product_number
INNER JOIN gold.dim_customers cu 
    ON sd.sls_cust_id = cu.customer_id;

-- Check the quality of the view
SELECT * FROM gold.fact_sales;


-- Check Foreign key Integrity (Dimensions)
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL AND p.product_key IS NULL ;










