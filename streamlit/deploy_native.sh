#!/bin/bash

# Native Snowflake CLI deployment script for Streamlit app
# This replaces the manual stage upload approach with snow streamlit deploy

# --- Default Configuration ---
DEFAULT_CONNECTION="dbt_project_llm"
DEFAULT_DATABASE="DBT_CORTEX_LLMS"
DEFAULT_SCHEMA="ANALYTICS"

# Set working directory to streamlit folder
cd "$(dirname "$0")"

echo "🚀 Deploying Streamlit app using native Snowflake CLI..."
echo "Working directory: $(pwd)"
echo ""

# Prompt for configuration with defaults
echo "📋 Configuration (press Enter to use defaults):"
echo ""

read -p "Snowflake connection name [${DEFAULT_CONNECTION}]: " SNOWFLAKE_CONNECTION_NAME
SNOWFLAKE_CONNECTION_NAME=${SNOWFLAKE_CONNECTION_NAME:-$DEFAULT_CONNECTION}

read -p "Database name [${DEFAULT_DATABASE}]: " SNOWFLAKE_DATABASE
SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE:-$DEFAULT_DATABASE}

read -p "Schema name [${DEFAULT_SCHEMA}]: " SNOWFLAKE_SCHEMA
SNOWFLAKE_SCHEMA=${SNOWFLAKE_SCHEMA:-$DEFAULT_SCHEMA}

echo ""
echo "📊 Using configuration:"
echo "  Connection: ${SNOWFLAKE_CONNECTION_NAME}"
echo "  Database: ${SNOWFLAKE_DATABASE}"
echo "  Schema: ${SNOWFLAKE_SCHEMA}"
echo ""

# Confirm before proceeding
read -p "Continue with deployment? [Y/n]: " CONFIRM
CONFIRM=${CONFIRM:-Y}

if [[ ! $CONFIRM =~ ^[Yy]$ ]]; then
    echo "❌ Deployment cancelled."
    exit 0
fi

echo ""

# Deploy the Streamlit app using native CLI
echo "📦 Deploying Customer Intelligence Hub..."
snow streamlit deploy \
    --connection ${SNOWFLAKE_CONNECTION_NAME} \
    --database ${SNOWFLAKE_DATABASE} \
    --schema ${SNOWFLAKE_SCHEMA} \
    --replace

if [ $? -eq 0 ]; then
    echo "✅ Streamlit app deployed successfully!"
    echo "🔗 You can now access your app in Snowflake under:"
    echo "   Database: ${SNOWFLAKE_DATABASE}"
    echo "   Schema: ${SNOWFLAKE_SCHEMA}"
    echo "   App Name: customer_intelligence_hub"
else
    echo "❌ Deployment failed. Please check the error messages above."
    exit 1
fi

# Optional: Deploy semantic model for Cortex Analyst
echo ""
echo "📊 Deploying semantic model for Cortex Analyst..."
snow sql -q "CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.SEMANTIC_MODELS;" --connection ${SNOWFLAKE_CONNECTION_NAME}
snow sql -q "CREATE OR REPLACE STAGE ${SNOWFLAKE_DATABASE}.SEMANTIC_MODELS.YAML_STAGE;" --connection ${SNOWFLAKE_CONNECTION_NAME}
snow stage copy cortex_analyst/semantic_model.yaml @${SNOWFLAKE_DATABASE}.SEMANTIC_MODELS.YAML_STAGE/ --connection ${SNOWFLAKE_CONNECTION_NAME} --overwrite

echo "✅ Deployment completed!" 