-- Macro: Generate Surrogate Key
-- Creates a deterministic hash-based surrogate key from one or more columns.
-- Falls back to md5 for broad compatibility across DuckDB and BigQuery.

{% macro generate_surrogate_key(field_list) %}

    {% if field_list is string %}
        {% set field_list = [field_list] %}
    {% endif %}

    md5(
        {%- for field in field_list %}
            coalesce(cast({{ field }} as varchar), '_null_')
            {%- if not loop.last %} || '|' || {% endif -%}
        {%- endfor %}
    )

{% endmacro %}
