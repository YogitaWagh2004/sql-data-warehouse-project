/*
===============================================================================
Data Quality Validation Script: Gold Layer
===============================================================================

Script Purpose:
This script performs data quality and integrity checks on the Gold layer
views to ensure the correctness of the final Star Schema implementation.

The checks validate:
- Uniqueness of surrogate keys in dimension tables
- Referential integrity between fact and dimension tables

These validations confirm that the Gold layer is analytics-ready and
safe for consumption by reporting and BI tools.

Checks Included:
- Duplicate surrogate keys in gold.dim_customers
- Duplicate surrogate keys in gold.dim_products
- Foreign key integrity between gold.fact_sales and dimensions

Expectation:
- All validation queries should return ZERO rows.
- Any returned rows indicate data quality issues that must be investigated.

Usage:
- Run this script after Gold views are created or refreshed.
- Use results for QA validation before publishing data to consumers.

===============================================================================
*/


-- ==================================
-- Checking 'gold.dim_customers'
-- ==================================
-- check for Uniqueness of customer key in gold.dim_customers
-- Expectation: No results
SELECT
   customer_key,
   COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1; 


-- ==================================
-- Checking 'gold.products_key'
-- ==================================
-- Check for Uniqueness of products key in gold.dim_products
-- Expectation: No results
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;


-- ===========================================
--  Checking Foreign key Integrity (Dimensions)
-- ===========================================
-- Expectation: No results
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL AND p.product_key IS NULL;
