#!/bin/bash

# Script to deploy Streamlit app to Snowflake using the Snowflake CLI (`snow`)

# --- Configuration ---
# Snowflake connection name (used by snow cli)
# Ensure this connection is defined in ~/.snowflake/config.toml
SNOWFLAKE_CONNECTION_NAME="default"

# Snowflake database, schema, and warehouse (from your secrets.toml)
# These are used for constructing fully qualified names. Ensure the connection has access.
SNOWFLAKE_DATABASE="DBT_CORTEX_LLMS"
SNOWFLAKE_SCHEMA="ANALYTICS"
SNOWFLAKE_WAREHOUSE="CORTEX_WH"
SNOWFLAKE_STAGE_NAME="streamlit_app_stage"

# Streamlit App Name in Snowflake
STREAMLIT_APP_NAME="customer_intelligence_hub"

# Create or replace the Snowflake stage
echo "Ensuring Streamlit app stage exists: ${SNOWFLAKE_STAGE_NAME}..."
snow sql -q "CREATE OR REPLACE STAGE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE_NAME};" --connection ${SNOWFLAKE_CONNECTION_NAME}
echo "Stage ${SNOWFLAKE_STAGE_NAME} ensured."

# Create or replace the semantic models stage for Cortex Analyst
echo "Ensuring semantic models stage exists for Cortex Analyst..."
snow sql -q "CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.SEMANTIC_MODELS;" --connection ${SNOWFLAKE_CONNECTION_NAME}
snow sql -q "CREATE OR REPLACE STAGE ${SNOWFLAKE_DATABASE}.SEMANTIC_MODELS.YAML_STAGE;" --connection ${SNOWFLAKE_CONNECTION_NAME}
echo "Semantic models stage ensured."

# Upload semantic model for Cortex Analyst
echo "Uploading semantic model for Cortex Analyst..."
snow stage copy streamlit/cortex_analyst/semantic_model.yaml @${SNOWFLAKE_DATABASE}.SEMANTIC_MODELS.YAML_STAGE/ --connection ${SNOWFLAKE_CONNECTION_NAME} --overwrite
echo "Semantic model uploaded successfully."

# Copy the Streamlit app source files to the Snowflake stage
echo "Uploading Streamlit app files to stage: ${SNOWFLAKE_STAGE_NAME}..."

# Copy directories
echo "Copying utils/..."
snow stage copy streamlit/src/utils @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE_NAME}/utils/ --connection ${SNOWFLAKE_CONNECTION_NAME} --recursive --overwrite
echo "Copying components/..."
snow stage copy streamlit/src/components @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE_NAME}/components/ --connection ${SNOWFLAKE_CONNECTION_NAME} --recursive --overwrite

echo "Copying sql/..."
snow stage copy streamlit/src/sql @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE_NAME}/sql/ --connection ${SNOWFLAKE_CONNECTION_NAME} --recursive --overwrite
echo "Copying assets/..."
snow stage copy streamlit/src/assets @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE_NAME}/assets/ --connection ${SNOWFLAKE_CONNECTION_NAME} --recursive --overwrite

# Copy top-level files
echo "Copying streamlit_app.py..."
snow stage copy streamlit/src/streamlit_app.py @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE_NAME}/ --connection ${SNOWFLAKE_CONNECTION_NAME} --overwrite
echo "Copying __init__.py..."
snow stage copy streamlit/src/__init__.py @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE_NAME}/ --connection ${SNOWFLAKE_CONNECTION_NAME} --overwrite
echo "Copying environment.yml..."
snow stage copy streamlit/src/environment.yml @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE_NAME}/ --connection ${SNOWFLAKE_CONNECTION_NAME} --overwrite

echo "File upload process completed."

echo "Creating or replacing Streamlit app in Snowflake: ${STREAMLIT_APP_NAME}..."
snow sql -q "CREATE OR REPLACE STREAMLIT ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STREAMLIT_APP_NAME} FROM '@${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE_NAME}' MAIN_FILE = 'streamlit_app.py' QUERY_WAREHOUSE = '${SNOWFLAKE_WAREHOUSE}' TITLE = 'Customer Intelligence Hub';" --connection ${SNOWFLAKE_CONNECTION_NAME}
echo "Streamlit app ${STREAMLIT_APP_NAME} deployment initiated."

# TODO: Add commands to create or replace the Streamlit app in Snowflake
# using `snow streamlit deploy` or `snow object create streamlit`
