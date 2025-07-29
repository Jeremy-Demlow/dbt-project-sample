{% macro immediate_classify_schemas(dry_run=false) %}
  {% if execute %}
    {% set schema_profiles = var('schema_classification_profiles', {}) %}
    {% do log("🚀 Triggering immediate classification of schemas...", info=true) %}
    
    {% for schema_name, profile_name in schema_profiles.items() %}
      {% set full_schema = target.database ~ '.' ~ schema_name %}
      {# Check prefixed schema name too (e.g., ANALYTICS_RAW_PII) #}
      {% set prefixed_schema_name = target.schema ~ '_' ~ schema_name %}
      {% set prefixed_schema = target.database ~ '.' ~ prefixed_schema_name %}
      
      {# For each schema, try both normal and prefixed versions #}
      {% set schemas_to_try = [full_schema, prefixed_schema] %}
      
      {% for schema_to_try in schemas_to_try %}
        {# Check if schema exists #}
        {% set schema_check_sql %}
          SELECT COUNT(*) FROM {{ target.database }}.information_schema.schemata 
          WHERE schema_name = '{{ schema_to_try.split(".")[1] }}';
        {% endset %}
        
        {% if not dry_run %}
          {% set schema_result = run_query(schema_check_sql) %}
          {% if schema_result and schema_result.rows | length > 0 and schema_result.rows[0][0] > 0 %}
            {# Schema exists, try to classify it #}
            {% set classify_sql %}
              CALL SYSTEM$CLASSIFY_SCHEMA('{{ schema_to_try }}', {'auto_tag': true, 'sample_count': 1000});
            {% endset %}
            
            {% do log("Triggering immediate classification of schema: " ~ schema_to_try, info=true) %}
            {% do log("Options: auto_tag=true, sample_count=1000", info=true) %}
            
            {% set classify_success = true %}
            {% set classify_error = "" %}
            
            {% set classify_result = none %}
            {% set error_message = none %}
            
            {% do dbt_utils.safe_run_query(
              sql = classify_sql,
              result_var_name = "classify_result",
              error_message_var_name = "error_message"
            ) %}
            
            {% if error_message != none %}
              {% do log("❌ Classification failed for schema: " ~ schema_to_try ~ " - " ~ error_message, info=true) %}
            {% else %}
              {% do log("✅ Classification job submitted for schema: " ~ schema_to_try, info=true) %}
              {% do log("Result: " ~ classify_result.rows[0][0], info=true) %}
            {% endif %}
          {% endif %}
        {% else %}
          {% do log("DRY RUN - Would check if schema exists: " ~ schema_check_sql, info=true) %}
          {% do log("DRY RUN - Would trigger classification: CALL SYSTEM$CLASSIFY_SCHEMA('" ~ schema_to_try ~ "', {'auto_tag': true, 'sample_count': 1000})", info=true) %}
        {% endif %}
      {% endfor %}
    {% endfor %}
    
    {% do log("✅ Classification jobs submitted! Check results with 'dbt run-operation check_classification_status'", info=true) %}
    {% do log("Note: Immediate classification may take a few minutes to complete and consumes warehouse credits", info=true) %}
  {% endif %}
{% endmacro %}

{% macro create_tags(dry_run=false) %}
  {% if execute %}
    {% set tags_config = var('classification_tags', {}) %}
    
    {% if not tags_config %}
      {# Default tag if none specified #}
      {% set tags_config = {
        'pii_tag': {
          'comment': 'Tag for PII data classification'
        }
      } %}
    {% endif %}
    
    {% for tag_name, tag_config in tags_config.items() %}
      {% set full_tag_name = target.database ~ '.governance.' ~ tag_name %}
      
      {% set create_tag_sql %}
        CREATE SCHEMA IF NOT EXISTS {{ target.database }}.governance;
        CREATE TAG IF NOT EXISTS {{ full_tag_name }}
          COMMENT = '{{ tag_config.comment | default("Created by dbt classification management") }}'
        ;
      {% endset %}
      
      {% do log("Creating tag: " ~ full_tag_name, info=true) %}
      {% if not dry_run %}
        {% do run_query(create_tag_sql) %}
      {% else %}
        {% do log("DRY RUN - SQL: " ~ create_tag_sql, info=true) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro create_classification_profiles(dry_run=false) %}
  {% if execute %}
    {% set profiles = var('classification_profiles', {}) %}
    
    {# Select environment-specific profiles if available #}
    {% set env = target.name %}
    {% if env == 'dev' and var('dev_classification_profiles', {}) %}
      {% do log("Using development-specific classification profiles", info=true) %}
      {% set profiles = var('dev_classification_profiles') %}
    {% elif env == 'prod' and var('prod_classification_profiles', {}) %}
      {% do log("Using production-specific classification profiles", info=true) %}
      {% set profiles = var('prod_classification_profiles') %}
    {% endif %}
    
    {% for profile_name, config in profiles.items() %}
      {% set full_profile_name = target.database ~ '.governance.' ~ profile_name %}
      
      {% set sql %}
        CREATE SCHEMA IF NOT EXISTS {{ target.database }}.governance;
        CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE 
          {{ full_profile_name }}(PARSE_JSON('{{ tojson(config) }}'));
      {% endset %}
      
      {% do log("Creating classification profile: " ~ full_profile_name, info=true) %}
      {% if not dry_run %}
        {% do run_query(sql) %}
      {% else %}
        {% do log("DRY RUN - SQL: " ~ sql, info=true) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro assign_classification_profiles(dry_run=false) %}
  {% if execute %}
    {% set schema_profiles = var('schema_classification_profiles', {}) %}
    {% for schema_name, profile_name in schema_profiles.items() %}
      {% set full_schema = target.database ~ '.' ~ schema_name %}
      {% set full_profile = target.database ~ '.governance.' ~ profile_name %}
      
      {# Check if schema exists first #}
      {% set schema_check_sql %}
        SELECT COUNT(*) FROM {{ target.database }}.information_schema.schemata 
        WHERE schema_name = '{{ schema_name }}';
      {% endset %}
      
      {% set schema_exists = true %}
      {% if not dry_run %}
        {% set schema_result = run_query(schema_check_sql) %}
        {% if schema_result and schema_result.rows | length > 0 %}
          {% if schema_result.rows[0][0] == 0 %}
            {% set schema_exists = false %}
            {% do log("Schema " ~ full_schema ~ " does not exist. Creating it...", info=true) %}
            {% set create_schema_sql %}
              CREATE SCHEMA IF NOT EXISTS {{ full_schema }};
            {% endset %}
            {% do run_query(create_schema_sql) %}
          {% endif %}
        {% endif %}
      {% else %}
        {% do log("DRY RUN - Checking if schema exists: " ~ schema_check_sql, info=true) %}
        {% do log("DRY RUN - Would create schema if needed: CREATE SCHEMA IF NOT EXISTS " ~ full_schema, info=true) %}
      {% endif %}
      
      {% set sql %}
        ALTER SCHEMA {{ full_schema }}
        SET CLASSIFICATION_PROFILE = '{{ full_profile }}';
      {% endset %}
      
      {% do log("Assigning profile " ~ profile_name ~ " to schema " ~ full_schema, info=true) %}
      {% if not dry_run %}
        {% do run_query(sql) %}
      {% else %}
        {% do log("DRY RUN - SQL: " ~ sql, info=true) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro remove_classification_profiles(dry_run=false) %}
  {% if execute %}
    {% set schema_profiles = var('schema_classification_profiles', {}) %}
    {% for schema_name, profile_name in schema_profiles.items() %}
      {% set full_schema = target.database ~ '.' ~ schema_name %}
      {% set sql %}
        ALTER SCHEMA {{ full_schema }} UNSET CLASSIFICATION_PROFILE;
      {% endset %}
      
      {% do log("Removing classification profile from schema " ~ full_schema, info=true) %}
      {% if not dry_run %}
        {% do run_query(sql) %}
      {% else %}
        {% do log("DRY RUN - SQL: " ~ sql, info=true) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro set_custom_classifiers(dry_run=false) %}
  {% if execute %}
    {% set custom_classifiers = var('custom_classifiers', {}) %}
    {% set schema_profiles = var('schema_classification_profiles', {}) %}
    
    {% for profile_name in schema_profiles.values() | unique %}
      {% set full_profile = target.database ~ '.governance.' ~ profile_name %}
      {% set classifier_config = custom_classifiers.get(profile_name, {}) %}
      
      {% if classifier_config %}
        {# Skip custom classifiers since they require creation in Snowflake first #}
        {% do log("⚠️ Note: Custom classifiers need to be pre-created in Snowflake before they can be applied to profiles", info=true) %}
        {% do log("Skipping custom classifier application for profile: " ~ full_profile, info=true) %}
        
        {# For reference, this is how you would apply them if they existed #}
        {% if false %}
          {% set sql %}
            CALL {{ full_profile }}!SET_CUSTOM_CLASSIFIERS(PARSE_JSON('{{ tojson(classifier_config) }}'));
          {% endset %}
          
          {% do log("Setting custom classifiers for profile: " ~ full_profile, info=true) %}
          {% if not dry_run %}
            {% do run_query(sql) %}
          {% else %}
            {% do log("DRY RUN - SQL: " ~ sql, info=true) %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro check_classification_status(dry_run=false) %}
  {% if execute %}
    {% set schema_profiles = var('schema_classification_profiles', {}) %}
    {% do log("⚠️ Note: Classification results may not be available until at least 1 hour after setting profiles", info=true) %}
    
    {% for schema_name, profile_name in schema_profiles.items() %}
      {% set full_schema = target.database ~ '.' ~ schema_name %}
      
      {# Check both schema variants (target schema and custom schema) #}
      {% set schemas_to_check = [schema_name, target.schema ~ '_' ~ schema_name] %}
      
      {% for schema_to_check in schemas_to_check %}
        {% set tables_sql %}
          SELECT table_name 
          FROM {{ target.database }}.information_schema.tables
          WHERE table_schema = '{{ schema_to_check }}'
          LIMIT 10;
        {% endset %}
        
        {% if not dry_run %}
          {% set tables = run_query(tables_sql) %}
        {% else %}
          {% do log("DRY RUN - SQL: " ~ tables_sql, info=true) %}
          {% set tables = [] %}
        {% endif %}
        
        {% do log("Checking classification status for schema: " ~ target.database ~ '.' ~ schema_to_check, info=true) %}
        
        {% if not dry_run and tables and tables.rows | length > 0 %}
          {% for row in tables %}
            {% set table_name = row[0] %}
            {% set full_table = target.database ~ '.' ~ schema_to_check ~ '.' ~ table_name %}
            
            {% set sql %}
              CALL SYSTEM$GET_CLASSIFICATION_RESULT('{{ full_table }}');
            {% endset %}
            
            {% do log("  Table: " ~ table_name, info=true) %}
            {% set results = run_query(sql) %}
            
            {% if results and results.rows | length > 0 %}
              {% do results.print_table() %}
            {% else %}
              {% do log("  No classification results found yet. It may take 1+ hours to process.", info=true) %}
            {% endif %}
          {% endfor %}
        {% elif dry_run %}
          {% do log("  DRY RUN - Would check tables in schema " ~ target.database ~ '.' ~ schema_to_check, info=true) %}
        {% else %}
          {% do log("  No tables found in schema " ~ target.database ~ '.' ~ schema_to_check, info=true) %}
        {% endif %}
      {% endfor %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro validate_target_schemas(dry_run=false) %}
  {% if execute %}
    {% set schema_profiles = var('schema_classification_profiles', {}) %}
    {% set required_schemas = schema_profiles.keys() | list %}
    
    {% do log("🔍 Validating target schemas for classification...", info=true) %}
    {% set valid_schemas = [] %}
    {% set missing_schemas = [] %}
    
    {% for schema_name in required_schemas %}
      {# Check normal schema name #}
      {% set schema_check_sql %}
        SELECT COUNT(*) FROM {{ target.database }}.information_schema.schemata 
        WHERE schema_name = '{{ schema_name }}';
      {% endset %}
      
      {# Also check prefixed schema name (e.g., ANALYTICS_RAW_PII) #}
      {% set prefixed_schema_name = target.schema ~ '_' ~ schema_name %}
      {% set prefixed_schema_check_sql %}
        SELECT COUNT(*) FROM {{ target.database }}.information_schema.schemata 
        WHERE schema_name = '{{ prefixed_schema_name }}';
      {% endset %}
      
      {% if not dry_run %}
        {% set schema_result = run_query(schema_check_sql) %}
        {% set prefixed_schema_result = run_query(prefixed_schema_check_sql) %}
        
        {% if (schema_result and schema_result.rows | length > 0 and schema_result.rows[0][0] > 0) or
             (prefixed_schema_result and prefixed_schema_result.rows | length > 0 and prefixed_schema_result.rows[0][0] > 0) %}
          {% do valid_schemas.append(schema_name) %}
        {% else %}
          {% do missing_schemas.append(schema_name) %}
        {% endif %}
      {% else %}
        {% do log("DRY RUN - Checking if schema exists: " ~ schema_check_sql, info=true) %}
        {% do log("DRY RUN - Checking if prefixed schema exists: " ~ prefixed_schema_check_sql, info=true) %}
      {% endif %}
    {% endfor %}
    
    {% if not dry_run %}
      {% do log("✅ Valid schemas: " ~ valid_schemas | join(", "), info=true) %}
      
      {% if missing_schemas | length > 0 %}
        {% do log("⚠️ Missing schemas: " ~ missing_schemas | join(", "), info=true) %}
        {% do log("These schemas will be created automatically during profile assignment.", info=true) %}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro analyze_classification_coverage() %}
  {% if execute %}
    {% set schema_profiles = var('schema_classification_profiles', {}) %}
    {% do log("📊 Analyzing classification coverage...", info=true) %}
    
    {# Check if SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_INFO exists #}
    {% set check_ee_sql %}
      SELECT COUNT(*) FROM information_schema.functions
      WHERE function_catalog = 'SNOWFLAKE'
      AND function_schema = 'DATA_PRIVACY'
      AND function_name = 'CLASSIFICATION_INFO';
    {% endset %}
    
    {% set ee_check = run_query(check_ee_sql) %}
    {% set has_ee = ee_check and ee_check.rows | length > 0 and ee_check.rows[0][0] > 0 %}
    
    {% if not has_ee %}
      {% do log("⚠️ Enterprise Edition features not detected. Classification analysis requires Enterprise Edition.", info=true) %}
      {% do log("This appears to be a " ~ target.type ~ " connection without Enterprise Edition access.", info=true) %}
      {% do log("For testing purposes, we'll check schema and table structures without classification data.", info=true) %}
    {% endif %}
    
    {% set coverage_data = {} %}
    
    {% for schema_name, profile_name in schema_profiles.items() %}
      {# Check both schema variants (target schema and custom schema) #}
      {% set schemas_to_check = [schema_name, target.schema ~ '_' ~ schema_name] %}
      
      {% for schema_to_check in schemas_to_check %}
        {# Get table count #}
        {% set tables_count_sql %}
          SELECT COUNT(*) FROM {{ target.database }}.information_schema.tables
          WHERE table_schema = '{{ schema_to_check }}';
        {% endset %}
        
        {% set tables_count_result = run_query(tables_count_sql) %}
        {% if tables_count_result and tables_count_result.rows | length > 0 and tables_count_result.rows[0][0] > 0 %}
          {% set table_count = tables_count_result.rows[0][0] %}
          
          {# Get total column count #}
          {% set columns_sql %}
            SELECT COUNT(*) FROM {{ target.database }}.information_schema.columns
            WHERE table_schema = '{{ schema_to_check }}';
          {% endset %}
          
          {% set columns_result = run_query(columns_sql) %}
          {% set total_columns = 0 %}
          {% if columns_result and columns_result.rows | length > 0 %}
            {% set total_columns = columns_result.rows[0][0] %}
          {% endif %}
          
          {% set classified_columns = 0 %}
          {% set top_categories = [] %}
          
          {% if has_ee %}
            {# Get classified column count #}
            {% set classified_sql %}
              SELECT 
                COUNT(DISTINCT CONCAT(table_name, '.', column_name)) as classified_columns
              FROM TABLE(SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_INFO(
                schema_name => '{{ target.database }}.{{ schema_to_check }}'
              ))
              WHERE semantic_category IS NOT NULL;
            {% endset %}
            
            {% set classified_result = run_query(classified_sql) %}
            {% if classified_result and classified_result.rows | length > 0 %}
              {% set classified_columns = classified_result.rows[0][0] %}
            {% endif %}
            
            {# Get top categories #}
            {% set categories_sql %}
              SELECT 
                semantic_category, 
                COUNT(*) as count
              FROM TABLE(SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_INFO(
                schema_name => '{{ target.database }}.{{ schema_to_check }}'
              ))
              WHERE semantic_category IS NOT NULL
              GROUP BY semantic_category
              ORDER BY count DESC
              LIMIT 5;
            {% endset %}
            
            {% set categories_result = run_query(categories_sql) %}
            {% if categories_result and categories_result.rows | length > 0 %}
              {% do log("  Top categories:", info=true) %}
              {% for row in categories_result %}
                {% do top_categories.append(row[0] ~ ": " ~ row[1]) %}
                {% do log("    - " ~ row[0] ~ ": " ~ row[1], info=true) %}
              {% endfor %}
            {% endif %}
          {% endif %}
          
          {# Calculate metrics #}
          {% set coverage_pct = 0 %}
          {% if total_columns > 0 and has_ee %}
            {% set coverage_pct = (classified_columns / total_columns * 100) | round(1) %}
          {% endif %}
          
          {# Store in coverage data #}
          {% set schema_coverage = {
            'schema_name': schema_to_check,
            'table_count': table_count,
            'total_columns': total_columns,
            'classified_columns': classified_columns,
            'coverage_pct': coverage_pct,
            'top_categories': top_categories
          } %}
          
          {# Add to results #}
          {% do coverage_data.update({schema_to_check: schema_coverage}) %}
          
          {# Display results #}
          {% do log("Schema: " ~ schema_to_check, info=true) %}
          {% do log("  Tables: " ~ table_count, info=true) %}
          {% do log("  Columns: " ~ total_columns ~ " total", info=true) %}
          {% if has_ee %}
            {% do log("  Classified: " ~ classified_columns ~ " columns", info=true) %}
            {% do log("  Coverage: " ~ coverage_pct ~ "%", info=true) %}
          {% else %}
            {% do log("  Classification data unavailable (requires Enterprise Edition)", info=true) %}
          {% endif %}
        {% endif %}
      {% endfor %}
    {% endfor %}
    
    {% do log("📝 Classification summary:", info=true) %}
    {% set total_schemas = coverage_data | length %}
    {% set total_tables = namespace(val=0) %}
    {% set total_columns = namespace(val=0) %}
    {% set total_classified = namespace(val=0) %}
    
    {% for schema_name, data in coverage_data.items() %}
      {% set total_tables.val = total_tables.val + data.table_count %}
      {% set total_columns.val = total_columns.val + data.total_columns %}
      {% set total_classified.val = total_classified.val + data.classified_columns %}
    {% endfor %}
    
    {% if has_ee %}
      {% set overall_pct = 0 %}
      {% if total_columns.val > 0 %}
        {% set overall_pct = (total_classified.val / total_columns.val * 100) | round(1) %}
      {% endif %}
      
      {% do log("Overall coverage: " ~ overall_pct ~ "% (" ~ total_classified.val ~ " of " ~ total_columns.val ~ " columns)", info=true) %}
      {% do log("Across " ~ total_tables.val ~ " tables in " ~ total_schemas ~ " schemas", info=true) %}
      
      {% if overall_pct < 10 %}
        {% do log("⚠️ Low coverage detected. Consider:", info=true) %}
        {% do log("  - Waiting longer for classification to complete (1+ hours)", info=true) %}
        {% do log("  - Checking classification profile configurations", info=true) %}
        {% do log("  - Adding more sensitive data to test classification", info=true) %}
      {% elif overall_pct >= 10 and overall_pct < 30 %}
        {% do log("🔶 Moderate coverage detected.", info=true) %}
      {% else %}
        {% do log("✅ Good classification coverage detected.", info=true) %}
      {% endif %}
    {% else %}
      {% do log("Found " ~ total_tables.val ~ " tables with " ~ total_columns.val ~ " columns across " ~ total_schemas ~ " schemas", info=true) %}
      {% do log("⚠️ Classification data unavailable - Enterprise Edition required for full analysis", info=true) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro check_classification_queue() %}
  {% if execute %}
    {% do log("📊 Checking recent classification jobs...", info=true) %}
    
    {% set queue_sql %}
      SELECT 
        classification_name,
        classification_status,
        start_time,
        end_time
      FROM snowflake.account_usage.classification_history
      WHERE start_time >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
      ORDER BY start_time DESC;
    {% endset %}
    
    {% set results = none %}
    {% set error_message = none %}
    
    {% do dbt_utils.safe_run_query(
      sql = queue_sql,
      result_var_name = "results",
      error_message_var_name = "error_message"
    ) %}
    
    {% if error_message != none %}
      {% do log("❌ Error accessing classification history: " ~ error_message, info=true) %}
      {% do log("This may be due to permission restrictions or account edition limitations.", info=true) %}
      {% do log("The snowflake.account_usage.classification_history view requires appropriate access rights.", info=true) %}
    {% else %}
      {% if results and results.rows | length > 0 %}
        {% do log("Recent classification jobs:", info=true) %}
        {% do results.print_table() %}
        
        {# Calculate success rate #}
        {% set total_jobs = results.rows | length %}
        {% set success_count = 0 %}
        {% set pending_count = 0 %}
        {% set failed_count = 0 %}
        
        {% for row in results %}
          {% if row[1] == 'SUCCESS' %}
            {% set success_count = success_count + 1 %}
          {% elif row[1] == 'PENDING' %}
            {% set pending_count = pending_count + 1 %}
          {% elif row[1] == 'FAILED' %}
            {% set failed_count = failed_count + 1 %}
          {% endif %}
        {% endfor %}
        
        {% set success_rate = (success_count / total_jobs * 100) | round(1) if total_jobs > 0 else 0 %}
        {% do log("Summary:", info=true) %}
        {% do log("  Total jobs: " ~ total_jobs, info=true) %}
        {% do log("  Success: " ~ success_count ~ " (" ~ success_rate ~ "%)", info=true) %}
        {% do log("  Pending: " ~ pending_count, info=true) %}
        {% do log("  Failed: " ~ failed_count, info=true) %}
        
        {% if pending_count > 0 %}
          {% do log("ℹ️ Some classifications are still pending. Check back later for complete results.", info=true) %}
        {% endif %}
        
        {% if failed_count > 0 %}
          {% do log("⚠️ Some classifications failed. Consider using 'retry_failed_classifications'", info=true) %}
        {% endif %}
      {% else %}
        {% do log("No recent classification jobs found in the past 2 hours.", info=true) %}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro retry_failed_classifications() %}
  {% if execute %}
    {% do log("🔄 Identifying failed classification jobs...", info=true) %}
    
    {% set failed_sql %}
      SELECT DISTINCT 
        REGEXP_SUBSTR(classification_name, '[^.]+\\.[^.]+$') as schema_name
      FROM snowflake.account_usage.classification_history
      WHERE classification_status = 'FAILED'
      AND start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP());
    {% endset %}
    
    {% set failed_schemas = none %}
    {% set error_message = none %}
    
    {% do dbt_utils.safe_run_query(
      sql = failed_sql,
      result_var_name = "failed_schemas",
      error_message_var_name = "error_message"
    ) %}
    
    {% if error_message != none %}
      {% do log("❌ Error accessing classification history: " ~ error_message, info=true) %}
      {% do log("This may be due to permission restrictions or account edition limitations.", info=true) %}
      {% do log("The snowflake.account_usage.classification_history view requires appropriate access rights.", info=true) %}
    {% else %}
      {% if failed_schemas and failed_schemas.rows | length > 0 %}
        {% do log("Found " ~ failed_schemas.rows | length ~ " failed schema classifications to retry.", info=true) %}
        
        {% for row in failed_schemas %}
          {% set schema_name = row[0] %}
          {% do log("Retrying classification for schema: " ~ schema_name, info=true) %}
          
          {% set classify_sql %}
            CALL SYSTEM$CLASSIFY_SCHEMA('{{ schema_name }}', {'auto_tag': true, 'sample_count': 1000});
          {% endset %}
          
          {% set retry_result = none %}
          {% set retry_error = none %}
          
          {% do dbt_utils.safe_run_query(
            sql = classify_sql,
            result_var_name = "retry_result",
            error_message_var_name = "retry_error"
          ) %}
          
          {% if retry_error != none %}
            {% do log("❌ Retry failed for schema: " ~ schema_name ~ " - " ~ retry_error, info=true) %}
          {% else %}
            {% do log("✅ Retry job submitted for schema: " ~ schema_name, info=true) %}
            {% do log("Result: " ~ retry_result.rows[0][0], info=true) %}
          {% endif %}
        {% endfor %}
      {% else %}
        {% do log("No failed classification jobs found in the past 24 hours.", info=true) %}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro setup_classification(dry_run=false) %}
  {{ validate_target_schemas(dry_run=dry_run) }}
  {{ create_tags(dry_run=dry_run) }}
  {{ create_classification_profiles(dry_run=dry_run) }}
  {{ set_custom_classifiers(dry_run=dry_run) }}
  {{ assign_classification_profiles(dry_run=dry_run) }}
  {% do log("⚠️ Classification profiles set! Wait 1+ hours before checking results with 'dbt run-operation check_classification_status'", info=true) %}
{% endmacro %}

{% macro dry_run_setup() %}
  {{ setup_classification(dry_run=true) }}
{% endmacro %}

{% macro test_classification(table_name, profile_name, dry_run=false) %}
  {% if execute %}
    {% set full_table = target.database ~ '.' ~ table_name %}
    {% set full_profile = target.database ~ '.governance.' ~ profile_name %}
    
    {% set sql %}
      CALL SYSTEM$CLASSIFY('{{ full_table }}', '{{ full_profile }}');
    {% endset %}
    
    {% do log("Testing classification of table " ~ full_table ~ " with profile " ~ profile_name, info=true) %}
    {% if not dry_run %}
      {% set results = run_query(sql) %}
      {% do results.print_table() %}
    {% else %}
      {% do log("DRY RUN - SQL: " ~ sql, info=true) %}
    {% endif %}
  {% endif %}
{% endmacro %} 