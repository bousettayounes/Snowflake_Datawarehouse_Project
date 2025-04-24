/*
Purpose:
--------
This script ensures a clean setup of the 'Datawarehouse' database in Snowflake using the Medallion Architecture approach.
It creates three organizational schemas: 'bronze', 'silver', and 'gold', each representing different stages of data processing.

Steps:
------
1. Drops the 'Datawarehouse' database if it already exists.
2. Creates a fresh 'Datawarehouse' database.
3. Switches to using that database.
4. Creates the necessary schemas (if they don't already exist).

WARNING:
--------
- Dropping the database will permanently delete all existing schemas, tables, and data inside 'Datawarehouse'.
- Use this script with caution, especially in production environments.
- Ensure backups or exports are taken before running this in any non-development setting.
*/

-- Drop the database if it exists
DROP DATABASE IF EXISTS Datawarehouse;

-- Create the database
CREATE DATABASE Datawarehouse;

-- Use the new database
USE DATABASE Datawarehouse;

-- Create schemas if they do not exist
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;