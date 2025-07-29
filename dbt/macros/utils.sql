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
  
  {% do log("Available schemas:", info=true) %}
  {% set schema_result = run_query(schema_sql) %}
  
  {% if schema_result and schema_result.rows | length > 0 %}
    {% do schema_result.print_table() %}
    
    {# Get some sample tables for each schema #}
    {% for row in schema_result %}
      {% set schema_name = row[0] %}
      {% set tables_sql %}
        SELECT table_name
        FROM {{ target.database }}.information_schema.tables
        WHERE table_schema = '{{ schema_name }}'
        LIMIT 5;
      {% endset %}
      
      {% set tables_result = run_query(tables_sql) %}
      
      {% if tables_result and tables_result.rows | length > 0 %}
        {% do log("Sample tables in " ~ schema_name ~ ":", info=true) %}
        {% do tables_result.print_table() %}
      {% endif %}
    {% endfor %}
  {% else %}
    {% do log("No schemas found or error accessing information_schema", info=true) %}
  {% endif %}
{% endmacro %} 