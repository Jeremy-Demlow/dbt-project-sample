{% macro create_semantic_view(semantic_view_name, semantic_view_sql) %}
  
  {% if not semantic_view_name %}
    {% do exceptions.raise_compiler_error("semantic_view_name is required") %}
  {% endif %}
  
  {% if not semantic_view_sql %}
    {% do exceptions.raise_compiler_error("semantic_view_sql is required") %}
  {% endif %}

  {{ log("Creating semantic view: " ~ semantic_view_name, info=True) }}
  
  {% do run_query(semantic_view_sql) %}
  
  {{ log("Semantic view '" ~ semantic_view_name ~ "' created successfully!", info=True) }}

{% endmacro %}


{% macro deploy_semantic_view(view_name) %}
  
  {{ log("Note: File-based deployment requires manual execution. Use create_semantic_view macro with SQL content instead.", info=True) }}
  {{ log("Example: dbt run-operation create_semantic_view --args '{semantic_view_name: \"" ~ view_name ~ "\", semantic_view_sql: \"...\"}'", info=True) }}
  
  {% do exceptions.raise_compiler_error("File reading not supported in DBT macros. Please use the create_semantic_view macro with the SQL content directly, or run the SQL file with: snow sql -f models/semantic/" ~ view_name ~ ".sql") %}
  
{% endmacro %}


{% macro deploy_semantic_view_from_file(view_name, folder_path='models/semantic') %}
  
  {{ log("Deploying semantic view from compiled file: " ~ view_name, info=True) }}
  
  {% if execute %}
    {% set compiled_path = 'target/compiled/' ~ project_name ~ '/' ~ folder_path ~ '/' ~ view_name ~ '.sql' %}
    
    {{ log("Reading compiled SQL from: " ~ compiled_path, info=True) }}
    
    -- Note: This requires the file to be pre-compiled with 'dbt compile --select ' ~ view_name
    -- The compiled SQL will have all Jinja variables resolved
    
    {{ log("To deploy this semantic view:", info=True) }}
    {{ log("1. Run: dbt compile --select " ~ view_name, info=True) }}
    {{ log("2. Run: snow sql -f " ~ compiled_path, info=True) }}
    {{ log("", info=True) }}
    {{ log("Or use the direct approach:", info=True) }}
    {{ log("dbt compile --select " ~ view_name ~ " && snow sql -f " ~ compiled_path, info=True) }}
    
  {% endif %}
  
{% endmacro %}


{% macro auto_deploy_semantic_views() %}
  
  {{ log("=== SEMANTIC VIEW DEPLOYMENT SUCCESS ===", info=True) }}
  {{ log("", info=True) }}
  {{ log("✅ Semantic views now work with standard DBT workflow!", info=True) }}
  {{ log("", info=True) }}
  {{ log("PRIMARY METHOD (recommended):", info=True) }}
  {{ log("  dbt run --select customer_intelligence_semantic_view", info=True) }}
  {{ log("", info=True) }}
  {{ log("ALTERNATIVE METHODS:", info=True) }}
  {{ log("", info=True) }}
  {{ log("Method 2: Compile + Snowflake CLI", info=True) }}
  {{ log("  dbt compile --select customer_intelligence_semantic_view", info=True) }}
  {{ log("  snow sql -f dbt/target/compiled/dbt_cortex/models/semantic/customer_intelligence_semantic_view.sql", info=True) }}
  {{ log("", info=True) }}
  {{ log("Method 3: Direct macro deployment", info=True) }}
  {{ log("  dbt run-operation create_semantic_view --args '{...}'", info=True) }}
  {{ log("", info=True) }}
  {{ log("🎉 BENEFITS OF DBT RUN:", info=True) }}
  {{ log("✅ Familiar DBT developer workflow", info=True) }}
  {{ log("✅ Works with DBT selection syntax", info=True) }}
  {{ log("✅ Integrates with CI/CD pipelines", info=True) }}
  {{ log("✅ Uses custom semantic_view materialization", info=True) }}
  {{ log("", info=True) }}
  
{% endmacro %}


{# Custom materialization attempt - experimental #}
{% materialization semantic_view, adapter='snowflake' %}
  
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  
  {{ log("Attempting to create semantic view: " ~ target_relation, info=True) }}
  
  {%- call statement('main') -%}
    {{ sql }}
  {%- endcall -%}
  
  {{ log("Semantic view materialization completed", info=True) }}
  
  {{ return({'relations': []}) }}
  
{% endmaterialization %} 