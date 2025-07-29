-- ====================================================================
-- SNOWFLAKE ROLE & PERMISSION SETUP
-- Run this FIRST before any dbt project setup
-- ====================================================================

-- Create database and schemas
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS CORTEX_WH 
WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;
USE WAREHOUSE CORTEX_WH;

CREATE ROLE IF NOT EXISTS DBT_ROLE;

-- Grant DBT_ROLE comprehensive privileges for dbt operations
GRANT CREATE DATABASE ON ACCOUNT TO ROLE DBT_ROLE;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE DBT_ROLE;

-- Warehouse usage and management
GRANT ALL PRIVILEGES ON WAREHOUSE CORTEX_WH TO ROLE DBT_ROLE;

-- Grant roles to users
GRANT ROLE DBT_ROLE TO USER JDEMLOW;
GRANT ROLE DBT_ROLE TO USER jd_service_account_admin;

-- Dynamic grant for DBT_ROLE to current user
DECLARE
    sql_command STRING;
BEGIN
    sql_command := 'GRANT ROLE DBT_ROLE TO USER "' || CURRENT_USER() || '";';
    EXECUTE IMMEDIATE sql_command;
    RETURN 'Role DBT_ROLE granted successfully to user ' || CURRENT_USER();
END;

-- ====================================================================
-- VERIFICATION
-- ====================================================================
SHOW GRANTS TO ROLE DBT_ROLE;
SELECT 'Role setup complete! You can now run data_setup.sql or use dbt directly.' AS status; 