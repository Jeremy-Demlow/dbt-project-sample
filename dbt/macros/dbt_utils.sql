{% macro safe_run_query(sql, result_var_name, error_message_var_name) %}
  {% if execute %}
    {% if sql %}
      {# Initialize both variables to none #}
      {% set temp_result = none %}
      {% set temp_error = none %}
      
      {# Use dbt's run_query which handles errors gracefully #}
      {% set temp_result = run_query(sql) %}
      
      {# Update context with the result #}
      {% do context.update({result_var_name: temp_result}) %}
      {% do context.update({error_message_var_name: none}) %}
    {% else %}
      {# No SQL provided #}
      {% do context.update({result_var_name: none}) %}
      {% do context.update({error_message_var_name: "No SQL provided"}) %}
    {% endif %}
  {% else %}
    {# Not in execute mode #}
    {% do context.update({result_var_name: none}) %}
    {% do context.update({error_message_var_name: none}) %}
  {% endif %}
{% endmacro %}

{% macro simple_run_query(sql) %}
  {% if execute and sql %}
    {% set result = run_query(sql) %}
    {% do return(result) %}
  {% else %}
    {% do return(none) %}
  {% endif %}
{% endmacro %}

{% macro get_schema_info() %}
  {% set schema_sql %}
    SELECT 
      table_schema as schema_name,
      count(table_name) as table_count 
    FROM {{ target.database }}.information_schema.tables
    WHERE table_catalog = '{{ target.database }}'
    GROUP BY 1
    ORDER BY 2 DESC;
  {% endset %}
  
  {% set schema_result = run_query(schema_sql) %}
  {% if schema_result and schema_result.rows | length > 0 %}
    {% do log("Available schemas:", info=true) %}
    {% do schema_result.print_table() %}
  {% endif %}
{% endmacro %} 