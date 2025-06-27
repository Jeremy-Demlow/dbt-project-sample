{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {%- if custom_schema_name == 'STAGE' -%}
            STAGE
        {%- elif custom_schema_name == 'ANALYTICS' -%}
            ANALYTICS
        {%- elif custom_schema_name == 'SEMANTIC_MODELS' -%}
            SEMANTIC_MODELS
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %} 