{% macro check_model_classification_status() %}
  {% if execute %}
    {% do log("🔍 Checking classification status of actual dbt models...", info=true) %}
    
    {# Get all models in your project #}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' %}
        {% set model_schema = node.schema %}
        {% set model_name = node.name %}
        {% set full_table_name = target.database ~ '.' ~ model_schema ~ '.' ~ model_name %}
        
        {# Check if this table has been classified #}
        {% set classification_check_sql %}
          SELECT 
            table_name,
            column_name,
            semantic_category,
            privacy_category,
            classification_confidence
          FROM snowflake.account_usage.data_classification_latest
          WHERE table_database = '{{ target.database }}'
          AND table_schema = '{{ model_schema }}'
          AND table_name = '{{ model_name }}'
          AND semantic_category IS NOT NULL;
        {% endset %}
        
        {% set error_message = none %}
        {% set results = none %}
        
        {% do dbt_utils.safe_run_query(
          sql = classification_check_sql,
          result_var_name = "results",
          error_message_var_name = "error_message"
        ) %}
        
        {% if error_message != none %}
          {% do log("❌ Error checking " ~ full_table_name ~ ": " ~ error_message, info=true) %}
        {% elif results and results.rows | length > 0 %}
          {% do log("✅ " ~ full_table_name ~ " - CLASSIFIED", info=true) %}
          {% for row in results %}
            {% do log("    Column: " ~ row[1] ~ " → " ~ row[2] ~ " (" ~ row[3] ~ ")", info=true) %}
          {% endfor %}
        {% else %}
          {% do log("⏸️  " ~ full_table_name ~ " - NOT YET CLASSIFIED", info=true) %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro check_models_by_schema() %}
  {% if execute %}
    {% set model_schemas = {} %}
    
    {# Group models by schema #}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' %}
        {% set schema = node.schema %}
        {% if schema not in model_schemas %}
          {% do model_schemas.update({schema: []}) %}
        {% endif %}
        {% do model_schemas[schema].append(node.name) %}
      {% endif %}
    {% endfor %}
    
    {# Check classification for each schema #}
    {% for schema, models in model_schemas.items() %}
      {% do log("📊 Schema: " ~ schema ~ " (Profile: " ~ var('schema_classification_profiles', {}).get(schema, 'NONE') ~ ")", info=true) %}
      
      {% if models | length > 0 %}
        {% set schema_classification_sql %}
          SELECT 
            table_name,
            COUNT(DISTINCT column_name) as classified_columns,
            LISTAGG(DISTINCT semantic_category, ', ') as categories
          FROM snowflake.account_usage.data_classification_latest
          WHERE table_database = '{{ target.database }}'
          AND table_schema = '{{ schema }}'
          AND table_name IN ('{{ models | join("', '") }}')
          AND semantic_category IS NOT NULL
          GROUP BY table_name;
        {% endset %}
        
        {% set error_message = none %}
        {% set results = none %}
        
        {% do dbt_utils.safe_run_query(
          sql = schema_classification_sql,
          result_var_name = "results",
          error_message_var_name = "error_message"
        ) %}
        
        {% if error_message != none %}
          {% do log("❌ Error checking schema " ~ schema ~ ": " ~ error_message, info=true) %}
        {% elif results and results.rows | length > 0 %}
          {% for row in results %}
            {% do log("  ✅ " ~ row[0] ~ " - " ~ row[1] ~ " classified columns (" ~ row[2] ~ ")", info=true) %}
          {% endfor %}
        {% else %}
          {% do log("  ⏸️  No models classified yet in this schema", info=true) %}
        {% endif %}
      {% else %}
        {% do log("  ⚠️ No models found in this schema", info=true) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro check_specific_model(model_name) %}
  {% if execute %}
    {% set model_node = none %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' and node.name == model_name %}
        {% set model_node = node %}
        {% break %}
      {% endif %}
    {% endfor %}
    
    {% if not model_node %}
      {% do log("❌ Model not found: " ~ model_name, info=true) %}
      {% return %}
    {% endif %}
    
    {% set model_schema = model_node.schema %}
    {% set full_table = target.database ~ '.' ~ model_schema ~ '.' ~ model_name %}
    
    {% do log("🔍 Checking classification for model: " ~ model_name, info=true) %}
    {% do log("Full table name: " ~ full_table, info=true) %}
    
    {# Check if table exists #}
    {% set table_exists_sql %}
      SELECT COUNT(*) FROM {{ target.database }}.information_schema.tables
      WHERE table_schema = '{{ model_schema }}'
      AND table_name = '{{ model_name }}';
    {% endset %}
    
    {% set error_message = none %}
    {% set exists_result = none %}
    
    {% do dbt_utils.safe_run_query(
      sql = table_exists_sql,
      result_var_name = "exists_result",
      error_message_var_name = "error_message"
    ) %}
    
    {% if error_message != none %}
      {% do log("❌ Error checking table existence: " ~ error_message, info=true) %}
      {% return %}
    {% endif %}
    
    {% if exists_result.rows[0][0] == 0 %}
      {% do log("❌ Table does not exist. Run 'dbt run --select " ~ model_name ~ "' first.", info=true) %}
      {% return %}
    {% endif %}
    
    {# Check detailed classification with SYSTEM$GET_CLASSIFICATION_RESULT #}
    {% set classification_sql %}
      CALL SYSTEM$GET_CLASSIFICATION_RESULT('{{ full_table }}');
    {% endset %}
    
    {% set error_message = none %}
    {% set detailed_results = none %}
    
    {% do dbt_utils.safe_run_query(
      sql = classification_sql,
      result_var_name = "detailed_results",
      error_message_var_name = "error_message"
    ) %}
    
    {% if error_message != none %}
      {% do log("❓ No detailed classification results available via SYSTEM$GET_CLASSIFICATION_RESULT", info=true) %}
    {% elif detailed_results and detailed_results.rows | length > 0 %}
      {% do log("Detailed classification result:", info=true) %}
      {% do detailed_results.print_table() %}
    {% else %}
      {% do log("No detailed classification results found.", info=true) %}
    {% endif %}
    
    {# Check classification using account_usage view #}
    {% set account_usage_sql %}
      SELECT 
        column_name,
        semantic_category,
        privacy_category,
        classification_confidence
      FROM snowflake.account_usage.data_classification_latest
      WHERE table_database = '{{ target.database }}'
      AND table_schema = '{{ model_schema }}'
      AND table_name = '{{ model_name }}'
      AND semantic_category IS NOT NULL
      ORDER BY classification_confidence DESC;
    {% endset %}
    
    {% set error_message = none %}
    {% set results = none %}
    
    {% do dbt_utils.safe_run_query(
      sql = account_usage_sql,
      result_var_name = "results",
      error_message_var_name = "error_message"
    ) %}
    
    {% if error_message != none %}
      {% do log("❌ Error checking account_usage.data_classification_latest: " ~ error_message, info=true) %}
    {% elif results and results.rows | length > 0 %}
      {% do log("Account usage classification results:", info=true) %}
      {% do results.print_table() %}
    {% else %}
      {% do log("No classification data found in account_usage.data_classification_latest", info=true) %}
      {% do log("This typically means auto-classification hasn't completed yet (can take 1+ hour)", info=true) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro test_model_auto_classification() %}
  {% if execute %}
    {% do log("🧪 Testing auto-classification on your dbt models...", info=true) %}
    
    {# Get a few models from each classified schema #}
    {% set test_models = [] %}
    {% set schema_profiles = var('schema_classification_profiles', {}) %}
    
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' %}
        {% set node_schema = node.schema %}
        {% set schema_in_profiles = false %}
        
        {# Check if this schema or any variation is in schema_profiles #}
        {% for schema in schema_profiles.keys() %}
          {% if schema == node_schema or schema == node_schema.split('_')[-1] %}
            {% set schema_in_profiles = true %}
            {% set profile = schema_profiles[schema] %}
          {% endif %}
        {% endfor %}
        
        {% if schema_in_profiles %}
          {% do test_models.append({
            'name': node.name,
            'schema': node_schema,
            'profile': profile
          }) %}
          {% if test_models | length >= 5 %}
            {% break %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endfor %}
    
    {% if test_models | length == 0 %}
      {% do log("❌ No models found in classified schemas", info=true) %}
      {% return %}
    {% endif %}
    
    {% for model in test_models %}
      {% set full_table = target.database ~ '.' ~ model.schema ~ '.' ~ model.name %}
      
      {% do log("Testing: " ~ full_table ~ " (Profile: " ~ model.profile ~ ")", info=true) %}
      
      {# Check if it has been auto-classified #}
      {% set auto_check_sql %}
        SELECT COUNT(*) as classified_columns
        FROM snowflake.account_usage.data_classification_latest
        WHERE table_database = '{{ target.database }}'
        AND table_schema = '{{ model.schema }}'
        AND table_name = '{{ model.name }}'
        AND semantic_category IS NOT NULL;
      {% endset %}
      
      {% set error_message = none %}
      {% set auto_result = none %}
      
      {% do dbt_utils.safe_run_query(
        sql = auto_check_sql,
        result_var_name = "auto_result",
        error_message_var_name = "error_message"
      ) %}
      
      {% if error_message != none %}
        {% do log("  ❌ Error checking auto-classification: " ~ error_message, info=true) %}
      {% elif auto_result and auto_result.rows | length > 0 and auto_result.rows[0][0] > 0 %}
        {% do log("  ✅ Auto-classified: " ~ auto_result.rows[0][0] ~ " columns", info=true) %}
      {% else %}
        {% do log("  ⏸️  Not yet auto-classified (may need more time)", info=true) %}
        
        {# Try immediate classification for this model #}
        {% do log("  🚀 Triggering immediate classification for testing...", info=true) %}
        {% set immediate_sql %}
          CALL SYSTEM$CLASSIFY('{{ full_table }}', '{{ target.database }}.governance.{{ model.profile }}');
        {% endset %}
        
        {% set classify_error = none %}
        {% set classify_result = none %}
        
        {% do dbt_utils.safe_run_query(
          sql = immediate_sql,
          result_var_name = "classify_result",
          error_message_var_name = "classify_error"
        ) %}
        
        {% if classify_error != none %}
          {% do log("  ❌ Immediate classification failed: " ~ classify_error, info=true) %}
        {% else %}
          {% do log("  🔄 Immediate classification triggered, check results in a few minutes", info=true) %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro log_model_classification_summary() %}
  {% if execute %}
    {% set total_models = 0 %}
    {% set classified_models = 0 %}
    {% set schema_coverage = {} %}
    
    {% do log("📊 Model Classification Summary", info=true) %}
    
    {# Organize models by schema #}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' %}
        {% set total_models = total_models + 1 %}
        {% set node_schema = node.schema %}
        
        {# Initialize schema in coverage dict if needed #}
        {% if node_schema not in schema_coverage %}
          {% do schema_coverage.update({node_schema: {'total': 0, 'classified': 0}}) %}
        {% endif %}
        
        {# Increment schema total #}
        {% do schema_coverage[node_schema].update({'total': schema_coverage[node_schema]['total'] + 1}) %}
        
        {# Check if classified #}
        {% set check_sql %}
          SELECT COUNT(*) FROM snowflake.account_usage.data_classification_latest
          WHERE table_database = '{{ target.database }}'
          AND table_schema = '{{ node_schema }}'
          AND table_name = '{{ node.name }}'
          AND semantic_category IS NOT NULL;
        {% endset %}
        
        {% set error_message = none %}
        {% set result = none %}
        
        {% do dbt_utils.safe_run_query(
          sql = check_sql,
          result_var_name = "result",
          error_message_var_name = "error_message"
        ) %}
        
        {% if not error_message and result and result.rows[0][0] > 0 %}
          {% set classified_models = classified_models + 1 %}
          {% do schema_coverage[node_schema].update({'classified': schema_coverage[node_schema]['classified'] + 1}) %}
        {% endif %}
      {% endif %}
    {% endfor %}
    
    {# Calculate overall coverage percentage #}
    {% set overall_pct = 0 %}
    {% if total_models > 0 %}
      {% set overall_pct = (classified_models / total_models * 100) | round(1) %}
    {% endif %}
    
    {% do log("Overall: " ~ classified_models ~ "/" ~ total_models ~ " models classified (" ~ overall_pct ~ "%)", info=true) %}
    
    {# Report coverage by schema #}
    {% do log("Coverage by schema:", info=true) %}
    {% for schema_name, stats in schema_coverage.items() %}
      {% set schema_pct = 0 %}
      {% if stats.total > 0 %}
        {% set schema_pct = (stats.classified / stats.total * 100) | round(1) %}
      {% endif %}
      
      {# Get the profile assigned to this schema, if any #}
      {% set schema_profile = var('schema_classification_profiles', {}).get(schema_name, 'NONE') %}
      {% set profile_status = " 🟢 " if schema_profile != 'NONE' else " 🔴 " %}
      
      {% do log("  " ~ schema_name ~ profile_status ~ stats.classified ~ "/" ~ stats.total ~ 
               " (" ~ schema_pct ~ "%) - Profile: " ~ schema_profile, info=true) %}
    {% endfor %}
    
    {# Add recommendations #}
    {% if overall_pct < 10 %}
      {% do log("⚠️ Classification coverage is low. Suggestions:", info=true) %}
      {% do log("  - Check schema profile assignments in dbt_project.yml", info=true) %}
      {% do log("  - Run 'dbt run-operation immediate_classify_schemas' to speed up classification", info=true) %}
      {% do log("  - Wait at least 1 hour after profile assignment for auto-classification to start", info=true) %}
    {% elif overall_pct >= 10 and overall_pct < 50 %}
      {% do log("🔶 Classification in progress. To improve coverage:", info=true) %}
      {% do log("  - Ensure all schemas with sensitive data are assigned profiles", info=true) %}
      {% do log("  - Use 'dbt run-operation check_classification_queue' to monitor job status", info=true) %}
    {% else %}
      {% do log("✅ Good classification coverage detected", info=true) %}
    {% endif %}
  {% endif %}
{% endmacro %} 