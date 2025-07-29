# Snowflake Classification Profiles in dbt

This project implements Snowflake data classification profiles using dbt macros to automatically detect and tag sensitive data across your schemas.

> ⚠️ **Enterprise Edition Required**: This feature requires Snowflake Enterprise Edition or higher.

## How It Works

1. **Classification Profiles**: Define semantic data detection rules in your `dbt_project.yml`
2. **Schema Assignment**: Profiles are assigned at the schema level (Snowflake limitation)
3. **Automatic Tagging**: Tables in classified schemas get automatic detection and tagging
4. **Custom Classifiers**: Add domain-specific patterns for classifying sensitive data
5. **Tag Mapping**: Map semantic categories to user-defined tags for governance
6. **Environment-Specific Configurations**: Different settings for dev vs. prod environments
7. **Immediate Classification**: Option to trigger immediate classification of schemas
8. **Job Monitoring**: Track classification jobs and retry failed ones

## Getting Started

### 1. Configure Profiles in dbt_project.yml

The following configurations are already set up:

```yaml
vars:
  # Classification profile definitions
  classification_profiles:
    basic_pii_profile:
      minimum_object_age_for_classification_days: 1
      maximum_classification_validity_days: 30
      auto_tag: true
    
    comprehensive_profile:
      minimum_object_age_for_classification_days: 0
      maximum_classification_validity_days: 7
      auto_tag: true
      tag_map:
        column_tag_map:
          - tag_name: "DBT_CORTEX_LLMS.governance.pii_tag"
            tag_value: "sensitive"
            semantic_categories: ["NAME", "EMAIL"]
          - tag_name: "DBT_CORTEX_LLMS.governance.pii_tag"
            tag_value: "highly_sensitive"
            semantic_categories: ["NATIONAL_IDENTIFIER", "CREDIT_CARD", "PASSWORD"]

    standard_profile:
      minimum_object_age_for_classification_days: 1
      maximum_classification_validity_days: 14
      auto_tag: true

  # Environment-specific profiles
  dev_classification_profiles:
    basic_pii_profile:
      minimum_object_age_for_classification_days: 0  # Immediate for testing
      maximum_classification_validity_days: 1        # Daily refresh for dev
      auto_tag: true
  
  # Schema to profile mapping
  schema_classification_profiles:
    # Dedicated PII schemas
    RAW_PII: "basic_pii_profile"
    SENSITIVE_DATA: "comprehensive_profile"
    
    # Standard schemas
    RAW: "standard_profile"
    ANALYTICS: "standard_profile"
    SEMANTIC_MODELS: "standard_profile"
```

### 2. Deployment Options

#### Option 1: Setup Automatic Classification (Serverless)

This sets up continuous, serverless classification that runs in the background:

```bash
# 1. Validate target schemas (pre-flight check)
dbt run-operation validate_target_schemas

# 2. Create profiles and assign to schemas
dbt run-operation setup_classification

# 3. Run your models
dbt run

# 4. Wait at least 1 hour for Snowflake to process classifications
# Then check classification results
dbt run-operation check_classification_status
```

#### Option 2: Immediate Classification (Uses Warehouse)

This triggers immediate classification of all schemas in parallel (takes minutes instead of hours):

```bash
# Trigger immediate classification of all schemas
dbt run-operation immediate_classify_schemas

# Check results after a few minutes
dbt run-operation check_classification_status
```

> **Note**: Immediate classification runs on your Snowflake warehouse and consumes warehouse credits.

#### Option 3: Combined Approach (Recommended)

For best results, use both methods together:

```bash
# 1. Set up profiles and schemas
dbt run-operation setup_classification

# 2. Trigger immediate classification
dbt run-operation immediate_classify_schemas

# 3. Check job status in queue
dbt run-operation check_classification_queue

# 4. Check results after a few minutes
dbt run-operation check_classification_status

# 5. Analyze coverage across schemas
dbt run-operation analyze_classification_coverage

# 6. If needed, retry any failed classifications
dbt run-operation retry_failed_classifications
```

This gives you the immediate feedback of SYSTEM$CLASSIFY_SCHEMA with the ongoing maintenance of classification profiles.

### 3. Testing Without Executing (Dry Run)

If you want to see what SQL would be generated without actually executing it:

```bash
# Preview SQL without executing (dry run)
dbt run-operation dry_run_setup

# Preview immediate classification SQL
dbt run-operation immediate_classify_schemas --args '{dry_run: true}'
```

### 4. Analyze Classification Coverage

After classification runs (either method), check the results:

```bash
# Analyze coverage across all schemas
dbt run-operation analyze_classification_coverage
```

## Available Macros

- `create_classification_profiles`: Creates profiles in Snowflake
- `assign_classification_profiles`: Assigns profiles to schemas
- `check_classification_status`: Shows detected categories and tags
- `remove_classification_profiles`: Removes profile assignments
- `setup_classification`: Runs both create and assign steps
- `test_classification`: Tests a profile on a specific table without waiting
- `set_custom_classifiers`: Adds custom classifiers to profiles
- `dry_run_setup`: Previews SQL without executing anything
- `validate_target_schemas`: Pre-flight check of required schemas
- `analyze_classification_coverage`: Analyzes classification effectiveness
- `immediate_classify_schemas`: Triggers immediate classification of all schemas
- `check_classification_queue`: Monitors recent classification jobs
- `retry_failed_classifications`: Automatically retries failed classification jobs

## Environment-Specific Profiles

The system automatically detects your dbt target environment and applies the appropriate configuration:

- **Development**: More aggressive classification (immediate, daily refreshes)
- **Production**: More conservative approach (1-day delay, monthly refreshes)

This allows for faster testing in development while being more resource-conscious in production.

## Classification Methods Comparison

| Feature | Classification Profiles | SYSTEM$CLASSIFY_SCHEMA |
|---------|------------------------|-----------------------|
| Speed | ~1 hour to start | Minutes |
| Resource usage | Serverless (no warehouse) | Uses warehouse credits |
| Ongoing updates | Yes, automatic | One-time |
| Credit cost | Serverless credits | Warehouse credits |
| Best for | Production | Development, testing |

## Job Monitoring and Recovery

You can monitor the status of classification jobs and retry failed ones:

```bash
# Check recent classification jobs (past 2 hours)
dbt run-operation check_classification_queue

# Automatically retry any failed jobs from past 24 hours
dbt run-operation retry_failed_classifications
```

The `check_classification_queue` macro provides:
- List of all recent classification jobs with their status
- Success rate percentage
- Count of pending and failed jobs
- Recommendations for next actions

## Testing a Profile Before Applying

To test a classification profile on a specific table without waiting for the automatic process:

```bash
dbt run-operation test_classification --args '{table_name: "my_schema.my_table", profile_name: "comprehensive_profile"}'
```

This uses Snowflake's `SYSTEM$CLASSIFY` stored procedure to run an immediate classification with your profile.

## Important Notes

1. **Enterprise Edition Required**: Classification is an Enterprise Edition feature
2. **One-Hour Delay**: Profile-based classification takes ~1 hour to begin processing
3. **Schema-Level Only**: Classification profiles work only at schema level, not table level
4. **Classification Tags**: Creates governance schema with tags for classifications
5. **Access Control**: You must have appropriate access to create/modify schemas
6. **Credit Usage**: This feature consumes serverless compute credits
7. **Error Handling**: All macros include try/except blocks to handle failures gracefully

## Analyzing Classification Results

After classification has been running for a while, you can analyze the effectiveness:

```bash
dbt run-operation analyze_classification_coverage
```

This macro:
- Calculates coverage percentage across all schemas
- Identifies top detected categories
- Provides recommendations for improving coverage
- Shows a summary of all classified data

## Tag-Based Masking

Once you've applied classification and tags, you can set up tag-based masking policies:

```sql
-- Create a masking policy
CREATE MASKING POLICY pii_mask AS (val STRING) 
  RETURNS STRING ->
    CASE WHEN CURRENT_ROLE() IN ('ANALYST') THEN '****'
         ELSE val
    END;

-- Attach the masking policy to a tag
ALTER TAG governance.pii_tag 
  SET MASKING POLICY pii_mask;
```

## Monitoring Credit Usage

You can monitor credit usage with these queries:

```sql
-- Hourly credit usage
SELECT
  service_type,
  start_time,
  end_time,
  credits_used
FROM snowflake.account_usage.metering_history
WHERE service_type = 'SENSITIVE_DATA_CLASSIFICATION';

-- Daily credit usage
SELECT
  service_type,
  usage_date,
  credits_used
FROM snowflake.account_usage.metering_daily_history
WHERE service_type = 'SENSITIVE_DATA_CLASSIFICATION';
```

## Troubleshooting

If classification isn't working as expected:

1. **Check Permissions**: Ensure your role has `EXECUTE AUTO CLASSIFICATION` on schema
2. **Verify Account Edition**: This feature requires Enterprise Edition or higher
3. **Check Event Table**: Query the event log table for classification errors
4. **Allow Sufficient Time**: Classification takes at least 1 hour to start
5. **Run Validation**: Use `validate_target_schemas` to check schema configurations
6. **Analyze Coverage**: Use `analyze_classification_coverage` to identify issues
7. **Try Immediate Classification**: Use `immediate_classify_schemas` if you need results quickly
8. **Monitor Job Queue**: Use `check_classification_queue` to view job status
9. **Retry Failed Jobs**: Use `retry_failed_classifications` for automatic recovery

For more information, see [Snowflake Classification Documentation](https://docs.snowflake.com/en/user-guide/classification) 