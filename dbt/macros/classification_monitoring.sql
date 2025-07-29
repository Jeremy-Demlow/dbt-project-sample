{% macro check_model_classification_status() %}
  {% if execute %}
    {% do log("🔍 Checking classification status of actual dbt models...", info=true) %}
    
    {# Get all models in your project #}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' %}
        {% set model_schema = node.schema %}
        {% set model_name = node.name %}
        {% set full_table_name = target.database ~ '.' ~ model_schema ~ '.' ~ model_name %}
        
        {# Check if table exists #}
        {% set table_exists_sql %}
          SELECT COUNT(*) FROM {{ target.database }}.information_schema.tables
          WHERE table_schema = '{{ model_schema }}'
          AND table_name = '{{ model_name }}';
        {% endset %}
        
        {% set exists_result = run_query(table_exists_sql) %}
        {% if exists_result and exists_result.rows | length > 0 and exists_result.rows[0][0] > 0 %}
          {# Table exists, check classification #}
          {% set classification_sql %}
            CALL SYSTEM$GET_CLASSIFICATION_RESULT('{{ full_table_name }}');
          {% endset %}
          
          {% set results = run_query(classification_sql) %}
          
          {% if results and results.rows | length > 0 %}
            {% do log("✅ " ~ full_table_name ~ " - CLASSIFIED", info=true) %}
            {% do results.print_table() %}
          {% else %}
            {% do log("⏸️  " ~ full_table_name ~ " - NOT YET CLASSIFIED", info=true) %}
          {% endif %}
        {% else %}
          {% do log("⚠️  " ~ full_table_name ~ " - TABLE DOES NOT EXIST", info=true) %}
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
        {% for model_name in models %}
          {# Check if table exists #}
          {% set table_exists_sql %}
            SELECT COUNT(*) FROM {{ target.database }}.information_schema.tables
            WHERE table_schema = '{{ schema }}'
            AND table_name = '{{ model_name }}';
          {% endset %}
          
          {% set exists_result = run_query(table_exists_sql) %}
          {% if exists_result and exists_result.rows | length > 0 and exists_result.rows[0][0] > 0 %}
            {% set full_table = target.database ~ '.' ~ schema ~ '.' ~ model_name %}
            {% set classification_sql %}
              CALL SYSTEM$GET_CLASSIFICATION_RESULT('{{ full_table }}');
            {% endset %}
            
            {% set results = run_query(classification_sql) %}
            
            {% if results and results.rows | length > 0 %}
              {% set json_text = results.rows[0][0] %}
              {% if json_text %}
                {% set json_result = fromjson(json_text) %}
                {% if json_result is mapping and json_result.get('classification_results') is sequence and json_result.get('classification_results') | length > 0 %}
                  {% set count = json_result.get('classification_results') | length %}
                  {% do log("  ✅ " ~ model_name ~ " - " ~ count ~ " classified columns", info=true) %}
                {% else %}
                  {% do log("  ⏸️  " ~ model_name ~ " - No classifications yet", info=true) %}
                {% endif %}
              {% else %}
                {% do log("  ⏸️  " ~ model_name ~ " - No results available", info=true) %}
              {% endif %}
            {% else %}
              {% do log("  ⏸️  " ~ model_name ~ " - No results available", info=true) %}
            {% endif %}
          {% else %}
            {% do log("  ⚠️  " ~ model_name ~ " - Table does not exist", info=true) %}
          {% endif %}
        {% endfor %}
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
      {% do return(none) %}
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
    
    {% set exists_result = run_query(table_exists_sql) %}
    
    {% if exists_result.rows[0][0] == 0 %}
      {% do log("❌ Table does not exist. Run 'dbt run --select " ~ model_name ~ "' first.", info=true) %}
      {% do return(none) %}
    {% endif %}
    
    {# Check detailed classification with SYSTEM$GET_CLASSIFICATION_RESULT #}
    {% set classification_sql %}
      CALL SYSTEM$GET_CLASSIFICATION_RESULT('{{ full_table }}');
    {% endset %}
    
    {% do log("Checking detailed classification results...", info=true) %}
    {% set detailed_results = run_query(classification_sql) %}
    
    {% if detailed_results and detailed_results.rows | length > 0 %}
      {% do log("Detailed classification result:", info=true) %}
      {% do detailed_results.print_table() %}
    {% else %}
      {% do log("No detailed classification results found.", info=true) %}
      {% do log("This could mean:", info=true) %}
      {% do log("  - Classification hasn't run yet (wait 1+ hour after setup)", info=true) %}
      {% do log("  - No sensitive data detected in this model", info=true) %}
      {% do log("  - Schema doesn't have a classification profile assigned", info=true) %}
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
        {% set profile = 'unknown' %}
        
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
      {% do return(none) %}
    {% endif %}
    
    {% for model in test_models %}
      {# Check if table exists #}
      {% set table_exists_sql %}
        SELECT COUNT(*) FROM {{ target.database }}.information_schema.tables
        WHERE table_schema = '{{ model.schema }}'
        AND table_name = '{{ model.name }}';
      {% endset %}
      
      {% set exists_result = run_query(table_exists_sql) %}
      {% if exists_result and exists_result.rows | length > 0 and exists_result.rows[0][0] > 0 %}
        {% set full_table = target.database ~ '.' ~ model.schema ~ '.' ~ model.name %}
        
        {% do log("Testing: " ~ full_table ~ " (Profile: " ~ model.profile ~ ")", info=true) %}
        
        {# Check if it has been auto-classified using SYSTEM$GET_CLASSIFICATION_RESULT #}
        {% set classification_sql %}
          CALL SYSTEM$GET_CLASSIFICATION_RESULT('{{ full_table }}');
        {% endset %}
        
        {% set classify_result = run_query(classification_sql) %}
        
        {% if classify_result and classify_result.rows | length > 0 %}
          {% set json_text = classify_result.rows[0][0] %}
          {% if json_text %}
            {% set json_result = fromjson(json_text) %}
            {% if json_result is mapping and json_result.get('classification_results') is sequence and json_result.get('classification_results') | length > 0 %}
              {% set count = json_result.get('classification_results') | length %}
              {% do log("  ✅ Auto-classified: " ~ count ~ " columns", info=true) %}
            {% else %}
              {% do log("  ⏸️  Not yet auto-classified (may need more time)", info=true) %}
              {% do log("  🚀 Triggering immediate classification for testing...", info=true) %}
              
              {% set immediate_sql %}
                CALL SYSTEM$CLASSIFY('{{ full_table }}', '{{ target.database }}.governance.{{ model.profile }}');
              {% endset %}
              
              {% set classify_result = run_query(immediate_sql) %}
              {% do log("  🔄 Immediate classification triggered, check results in a few minutes", info=true) %}
            {% endif %}
          {% else %}
            {% do log("  ⏸️  Not yet classified (may need more time)", info=true) %}
          {% endif %}
        {% else %}
          {% do log("  ⏸️  Not yet classified (may need more time)", info=true) %}
          
          {% do log("  🚀 Triggering immediate classification...", info=true) %}
          {% set immediate_sql %}
            CALL SYSTEM$CLASSIFY('{{ full_table }}', '{{ target.database }}.governance.{{ model.profile }}');
          {% endset %}
          
          {% set classify_result = run_query(immediate_sql) %}
          {% do log("  🔄 Immediate classification triggered, check results in a few minutes", info=true) %}
        {% endif %}
      {% else %}
        {% do log("⚠️ Table does not exist for model: " ~ model.name, info=true) %}
        {% do log("Run 'dbt run --select " ~ model.name ~ "' to create the table first.", info=true) %}
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
        {% set model_name = node.name %}
        
        {# Initialize schema in coverage dict if needed #}
        {% if node_schema not in schema_coverage %}
          {% do schema_coverage.update({node_schema: {'total': 0, 'classified': 0}}) %}
        {% endif %}
        
        {# Increment schema total #}
        {% do schema_coverage[node_schema].update({'total': schema_coverage[node_schema]['total'] + 1}) %}
        
        {# Check if the model exists first #}
        {% set check_sql %}
          SELECT COUNT(*) FROM {{ target.database }}.information_schema.tables
          WHERE table_schema = '{{ node_schema }}'
          AND table_name = '{{ model_name }}';
        {% endset %}
        
        {% set check_result = run_query(check_sql) %}
        {% if check_result and check_result.rows | length > 0 and check_result.rows[0][0] > 0 %}
          {# Check if classified with SYSTEM$GET_CLASSIFICATION_RESULT #}
          {% set full_table = target.database ~ '.' ~ node_schema ~ '.' ~ model_name %}
          {% set classification_sql %}
            CALL SYSTEM$GET_CLASSIFICATION_RESULT('{{ full_table }}');
          {% endset %}
          
          {% set is_classified = false %}
          {% set result = run_query(classification_sql) %}
          
          {% if result and result.rows | length > 0 %}
            {% set json_text = result.rows[0][0] %}
            {% if json_text %}
              {% set json_result = fromjson(json_text) %}
              {% if json_result is mapping and json_result.get('classification_results') is sequence and json_result.get('classification_results') | length > 0 %}
                {% set is_classified = true %}
              {% endif %}
            {% endif %}
          {% endif %}
          
          {% if is_classified %}
            {% set classified_models = classified_models + 1 %}
            {% do schema_coverage[node_schema].update({'classified': schema_coverage[node_schema]['classified'] + 1}) %}
          {% endif %}
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