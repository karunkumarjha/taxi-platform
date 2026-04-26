{#
    swap_marts — atomic blue-green flip of the mart layer.

    Runs `ALTER SCHEMA MARTS_BUILD SWAP WITH MARTS` against `target.database`.
    Snowflake makes this a metadata-only, atomic operation: dashboards reading
    from MARTS see the old data right up until the SWAP and the new data
    immediately after. Zero half-built window.

    Usage from CLI:
        dbt run-operation swap_marts --profiles-dir .

    Wire into Airflow as a task that depends on `dbt_build_marts` succeeding —
    that way a test failure in the marts layer means the SWAP never fires and
    MARTS keeps the previous good build.

    Requires: both schemas ANALYTICS.MARTS and ANALYTICS.MARTS_BUILD must
    already exist (they're created by setup_snowflake_local.sql or Terraform).
#}
{% macro swap_marts(prod_schema='MARTS', build_schema='MARTS_BUILD') %}

    {% set sql -%}
        ALTER SCHEMA {{ target.database }}.{{ build_schema }}
            SWAP WITH {{ target.database }}.{{ prod_schema }}
    {%- endset %}

    {% do log("blue-green: " ~ sql, info=True) %}

    {% if execute %}
        {% do run_query(sql) %}
        {% do log("blue-green: SWAP complete — dashboards now read fresh marts", info=True) %}
    {% endif %}

{% endmacro %}
