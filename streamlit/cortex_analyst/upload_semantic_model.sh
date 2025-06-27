#!/bin/bash

# Script to upload the semantic model YAML to a Snowflake stage
# This script is intended to be run from the 'cortex_analyst' directory

# Variables
SCHEMA_NAME="DBT_CORTEX_LLMS.SEMANTIC_MODELS"
STAGE_NAME="yaml_stage"
# Adjusted path: assumes script is run from cortex_analyst directory
LOCAL_FILE_PATH="./semantic_model.yaml"
FULL_STAGE_PATH="@${SCHEMA_NAME}.${STAGE_NAME}"

# Ensure Snowflake CLI is configured (user should handle this prerequisite)
echo "Ensure your Snowflake CLI is configured and you are logged in."
echo "Target schema: ${SCHEMA_NAME}"
echo "Target stage: ${STAGE_NAME}"
echo "Local file (relative to script location): ${LOCAL_FILE_PATH}"

# Create the schema if it doesn't exist
echo "Creating schema ${SCHEMA_NAME} if it doesn't exist..."
snow sql -q "CREATE SCHEMA IF NOT EXISTS ${SCHEMA_NAME};"

# Check if the previous command was successful
if [ $? -ne 0 ]; then
  echo "Error creating schema ${SCHEMA_NAME}. Please check your Snowflake connection and permissions."
  exit 1
fi

echo "Schema ${SCHEMA_NAME} ensured."

# Create the stage if it doesn't exist
echo "Creating stage ${FULL_STAGE_PATH} if it doesn't exist..."
snow sql -q "CREATE STAGE IF NOT EXISTS ${SCHEMA_NAME}.${STAGE_NAME};"

# Check if the previous command was successful
if [ $? -ne 0 ]; then
  echo "Error creating stage ${FULL_STAGE_PATH}. Please check your Snowflake connection and permissions."
  exit 1
fi

echo "Stage ${FULL_STAGE_PATH} ensured."

# Put the file to the stage
echo "Uploading ${LOCAL_FILE_PATH} to ${FULL_STAGE_PATH}..."
snow sql -q "PUT 'file://${PWD}/${LOCAL_FILE_PATH#./}' '${FULL_STAGE_PATH}' OVERWRITE = TRUE AUTO_COMPRESS = FALSE;"

# Check if the upload was successful
if [ $? -ne 0 ]; then
  echo "Error uploading file to stage. Please check the file path and your permissions."
  exit 1
fi

echo "File ${LOCAL_FILE_PATH} successfully uploaded to ${FULL_STAGE_PATH}."

# Enable directory listing on the stage
echo "Enabling directory listing for stage ${SCHEMA_NAME}.${STAGE_NAME}..."
snow sql -q "ALTER STAGE ${SCHEMA_NAME}.${STAGE_NAME} SET DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE);"

# Check if enabling directory listing was successful
if [ $? -ne 0 ]; then
  echo "Error enabling directory listing for stage ${SCHEMA_NAME}.${STAGE_NAME}."
  # Optionally, you might not want to exit here if the upload was the main goal
  # exit 1 
fi

echo "Directory listing enabled for stage ${SCHEMA_NAME}.${STAGE_NAME}."

echo "Script finished." 