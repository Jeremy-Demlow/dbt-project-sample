{% macro safe_run_query(sql, result_var_name, error_message_var_name) %}
  {% do context.update({
    result_var_name: none,
    error_message_var_name: none
  }) %}
  
  {% set result = none %}
  {% set error_message = none %}
  
  {% if execute %}
    {# Try to run the query and catch any errors #}
    {% set error_occurred = false %}
    {% set caught_exception = none %}
    
    {# We have to use a raw execute statement since try/except isn't available in Jinja #}
    {% set query_result = none %}
    {% if sql %}
      {% set query_result = run_query(sql) %}
      {% do context.update({result_var_name: query_result}) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro get_schema_info() %}
  {% set schema_sql %}
    SELECT 
      schema_name,
      count(table_name) as table_count 
    FROM {{ target.database }}.information_schema.tables
    GROUP BY 1
    ORDER BY 2 DESC;
  {% endset %}
  
  {% set schema_result = run_query(schema_sql) %}
  {% if schema_result and schema_result.rows | length > 0 %}
    {% do log("Available schemas:", info=true) %}
    {% do schema_result.print_table() %}
  {% endif %}
{% endmacro %} 