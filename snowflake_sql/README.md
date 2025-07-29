# Snowflake SQL Setup

This directory contains modular SQL scripts for setting up your Snowflake environment to work with the dbt project.

## 📁 Files Overview

| File | Purpose | When to Use |
|------|---------|-------------|
| `role_setup.sql` | **Essential role and permission setup** | **Always run first** - Required for any approach |
| `data_setup.sql` | **Raw data loading and database setup** | Run if you want to use external data sources |
| `snowflake_sql.sql` | **Original combined script** | Legacy - contains everything but less modular |

## 🚀 Quick Start

### Option 1: Modular Approach (Recommended)
```sql
-- 1. Run role setup first (always required)
-- Execute: role_setup.sql

-- 2. Choose your data approach:
-- Option A: External data (enterprise-like)
-- Execute: data_setup.sql

-- Option B: Use dbt seeds (self-contained)
-- Skip data_setup.sql and just run: dbt seed && dbt run
```

### Option 2: All-in-One (Legacy)
```sql
-- Execute: snowflake_sql.sql
-- Contains everything but less flexible
```

## 🔧 What Each Script Does

### `role_setup.sql` (Always Required)
- ✅ Creates `DBT_ROLE` with proper permissions
- ✅ Sets up `CORTEX_WH` warehouse
- ✅ Grants account-level privileges
- ✅ Assigns roles to users
- ✅ Dynamic role assignment for current user

### `data_setup.sql` (Optional - External Data)
- ✅ Creates `DBT_CORTEX_LLMS` database and schemas
- ✅ Loads JSON files from `data/samples/1000_Customers/`
- ✅ Creates raw tables for dbt to process
- ✅ Sets up stages and file formats

## 🎯 Benefits of Modular Approach

1. **Flexibility**: Choose your data source approach
2. **Reusability**: Role setup works for any dbt project
3. **Clarity**: Separate concerns for easier maintenance
4. **Options**: External data OR dbt seeds

## 📋 Prerequisites

- Snowflake account with ACCOUNTADMIN access
- dbt-snowflake installed and configured
- Data files in correct location (if using external data)

## 🎪 Usage Examples

### For Development/Demo
```bash
# 1. Run role setup in Snowflake
# 2. Use dbt seeds
cd dbt
dbt seed
dbt run
dbt test
```

### For Production/Enterprise
```bash
# 1. Run role_setup.sql in Snowflake
# 2. Run data_setup.sql in Snowflake  
# 3. Run dbt
cd dbt
dbt run
dbt test
``` 