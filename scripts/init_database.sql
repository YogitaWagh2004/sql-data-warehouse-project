/*
=============================================================
Create Database and Schemas
=============================================================
 
Script Purpose:
       This script initializes the Data_Warehouse database by dropping any existing
       version and recreating it from scratch. It then creates a layered schema
       structure (bronze, silver, and gold) to support a medallion-style data
	   architecture for data ingestion, transformation, and analytics.

	   1. Bronze Schema: Stores raw, unprocessed source data
       2. Silver Schema: Stores cleaned and transformed data
       3. Gold Schema:   Stores curated, business-ready data for reporting and analytics

 WARNING:
       Executing this script will permanently delete the existing Data_Warehouse
	   database and a-- ll its contents. Use with caution, especially in production.
============================================================================

*/



-- Drop and recreate the Data_Warehouse database
DROP DATABASE IF EXISTS Data_Warehouse;

-- Create the 'Data_Warehouse' database
CREATE DATABASE Data_Warehouse;

USE Data_Warehouse;

-- Create Schemas
-- bronze schema
CREATE SCHEMA bronze;

-- silver schema
CREATE SCHEMA silver;


-- gold schema
CREATE SCHEMA gold;
