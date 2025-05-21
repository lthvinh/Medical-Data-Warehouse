{% macro enriched_table(table_name) %}
    deltaLakeS3(
        {{ "'" ~ var('enriched_url') ~ table_name ~ '/' ~ "'" }}
        , {{ var('user') }}
        , {{ var('password') }}
    )
{% endmacro %}