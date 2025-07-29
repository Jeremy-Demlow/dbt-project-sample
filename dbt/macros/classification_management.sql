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

{% macro check_classification_status(dry_run=false) %}
  {% if execute %}
    {% set schema_profiles = var('schema_classification_profiles', {}) %}
    {% do log("⚠️ Note: Classification results may not be available until at least 1 hour after setting profiles", info=true) %}
    
    {% for schema_name, profile_name in schema_profiles.items() %}
      {% set full_schema = target.database ~ '.' ~ schema_name %}
      
      {% set tables_sql %}
        SELECT table_name 
        FROM {{ target.database }}.information_schema.tables
        WHERE table_schema = '{{ schema_name }}'
        LIMIT 10;
      {% endset %}
      
      {% if not dry_run %}
        {% set tables = run_query(tables_sql) %}
      {% else %}
        {% do log("DRY RUN - SQL: " ~ tables_sql, info=true) %}
        {% set tables = [] %}
      {% endif %}
      
      {% do log("Checking classification status for schema: " ~ full_schema, info=true) %}
      
      {% if not dry_run and tables and tables.rows | length > 0 %}
        {% for row in tables %}
          {% set table_name = row[0] %}
          {% set full_table = full_schema ~ '.' ~ table_name %}
          
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
        {% do log("  DRY RUN - Would check tables in schema " ~ full_schema, info=true) %}
      {% else %}
        {% do log("  No tables found in schema " ~ full_schema, info=true) %}
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

{% macro setup_classification(dry_run=false) %}
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