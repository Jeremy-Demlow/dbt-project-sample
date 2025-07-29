# Snowflake Classification Profiles in dbt

This project implements Snowflake data classification profiles using dbt macros to automatically detect and tag sensitive data across your schemas.

> ⚠️ **Enterprise Edition Required**: This feature requires Snowflake Enterprise Edition or higher.

## How It Works

1. **Classification Profiles**: Define semantic data detection rules in your `dbt_project.yml`
2. **Schema Assignment**: Profiles are assigned at the schema level (Snowflake limitation)
3. **Automatic Tagging**: Tables in classified schemas get automatic detection and tagging
4. **Custom Classifiers**: Add domain-specific patterns for classifying sensitive data
5. **Tag Mapping**: Map semantic categories to user-defined tags for governance

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
          - tag_name: "{{ target.database }}.governance.pii_tag"
            tag_value: "sensitive"
            semantic_categories: ["NAME", "EMAIL"]
          - tag_name: "{{ target.database }}.governance.pii_tag"
            tag_value: "highly_sensitive"
            semantic_categories: ["NATIONAL_IDENTIFIER", "CREDIT_CARD", "PASSWORD"]
  
  # Example custom classifiers
  custom_classifiers:
    comprehensive_profile:
      medical_codes: >
        {
          "pattern": "\\b[A-Z]\\d{2}(\\.\\d+)?\\b", 
          "semantic_category": "ICD_10_CODE",
          "privacy_category": "QUASI_IDENTIFIER"
        }
  
  # Schema to profile mapping
  schema_classification_profiles:
    RAW_PII: "basic_pii_profile"
    SENSITIVE_DATA: "comprehensive_profile"
```

### 2. Deploy Your Classifications

If you're on Enterprise Edition and have all the required permissions, use these commands:

```bash
# Create profiles and assign to schemas
dbt run-operation setup_classification

# Run your models
dbt run

# Wait at least 1 hour for Snowflake to process classifications
# Then check classification results
dbt run-operation check_classification_status
```

### 3. Testing Without Executing (Dry Run)

If you want to see what SQL would be generated without actually executing it:

```bash
# Preview SQL without executing (dry run)
dbt run-operation dry_run_setup

# Test a specific table's classification without waiting
dbt run-operation test_classification --args '{table_name: "my_schema.my_table", profile_name: "comprehensive_profile", dry_run: true}'
```

### 4. Organize Your Models

Put PII/sensitive models in the appropriate schemas:

```yaml
models:
  your_project:
    staging_pii:
      +schema: RAW_PII
      +materialized: table
    marts_sensitive:
      +schema: SENSITIVE_DATA
      +materialized: table
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

## Testing a Profile Before Applying

To test a classification profile on a specific table without waiting for the automatic process:

```bash
dbt run-operation test_classification --args '{table_name: "my_schema.my_table", profile_name: "comprehensive_profile"}'
```

This uses Snowflake's `SYSTEM$CLASSIFY` stored procedure to run an immediate classification with your profile.

## Important Notes

1. **Enterprise Edition Required**: Classification is an Enterprise Edition feature
2. **One-Hour Delay**: Snowflake classification takes ~1 hour to begin processing
3. **Schema-Level Only**: Classification profiles work only at schema level, not table level
4. **Classification Tags**: Creates governance schema with tags for classifications
5. **Access Control**: You must have appropriate access to create/modify schemas
6. **Credit Usage**: This feature consumes serverless compute credits

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

For more information, see [Snowflake Classification Documentation](https://docs.snowflake.com/en/user-guide/classification) 