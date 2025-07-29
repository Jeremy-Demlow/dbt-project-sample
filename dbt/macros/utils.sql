{% macro run_sql_query(sql) %}
  {% if execute and sql %}
    {% do log("Running SQL query:", info=true) %}
    {% do log(sql, info=true) %}
    
    {% set result = run_query(sql) %}
    {% do log("Results:", info=true) %}
    {% do result.print_table() %}
  {% else %}
    {% do log("No SQL provided or not in execute mode", info=true) %}
  {% endif %}
{% endmacro %} 