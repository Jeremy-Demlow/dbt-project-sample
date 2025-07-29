# Streamlit Deployment Guide

This directory contains two deployment approaches for the Customer Intelligence Hub Streamlit application.

## 🚀 New Approach: Native Snowflake CLI (Recommended)

### Files:
- `snowflake.yml` - Project definition file for Snowflake CLI
- `deploy_native.sh` - Simple deployment script using `snow streamlit deploy`

### Usage:
```bash
# Deploy using the native approach
./deploy_native.sh
```

### Benefits:
- ✅ Uses official Snowflake CLI deployment method
- ✅ Automatic artifact management
- ✅ Cleaner configuration with `snowflake.yml`
- ✅ Better integration with Snowflake's development workflow
- ✅ Handles all file uploads and app creation in one command

## 📦 Legacy Approach: Manual Stage Upload

### Files:
- `deploy_streamlit_to_snowflake.sh` - Manual stage upload and app creation

### Usage:
```bash
# Deploy using the legacy approach
./deploy_streamlit_to_snowflake.sh
```

### Characteristics:
- 🔧 Manual file-by-file stage uploads
- 🔧 Explicit stage management
- 🔧 More verbose but granular control
- 🔧 66-line script with manual file handling


## 🔄 Migration Path

1. **Test the new approach**: Use `./deploy_native.sh` for new deployments
2. **Validate functionality**: Ensure all components work with native deployment
3. **Retire legacy**: Once confident, remove `deploy_streamlit_to_snowflake.sh`

## 🎯 Deployment Configuration

Both approaches target:
- **Database**: `DBT_CORTEX_LLMS`
- **Schema**: `ANALYTICS`
- **Warehouse**: `CORTEX_WH`
- **App Name**: `customer_intelligence_hub`
- **Connection**: `default` (from `~/.snowflake/config.toml`)

## 🔧 Prerequisites

- Snowflake CLI installed and configured
- Connection profile `default` set up in `~/.snowflake/config.toml`
- Appropriate Snowflake privileges for Streamlit app creation
- Access to target database, schema, and warehouse 