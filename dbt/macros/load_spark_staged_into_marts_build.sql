{#
    load_spark_staged_into_marts_build — pull Spark-staged historical facts
    from the S3 Spark stage into MARTS_BUILD.FCT_TRIPS / FCT_TRIPS_QUARANTINED.

    Runs after reset_marts_build_from_marts (clone) and before dbt_seed in
    the dbt_pipeline DAG. The clone-then-COPY ordering means Spark data is
    layered on top of the cloned production state, so it persists across
    swap cycles (the next run's clone reads the previous SWAP's MARTS, which
    already includes prior Spark batches).

    Idempotent via Snowflake's COPY load history (filename-based, 64-day
    TTL). Re-running with no new staged files is a no-op. Snowflake's CLONE
    preserves load history, so already-loaded Spark batches don't reload
    after the schema gets cloned forward.

    Cold-start handling: if FCT_TRIPS / FCT_TRIPS_QUARANTINED don't exist in
    MARTS_BUILD yet (very first run after fresh terraform apply), the macro
    skips with a log message. dbt's first build creates the tables (empty);
    the next run's clone+COPY then loads any Spark-staged data normally.

    Runs as the DBT role (DBT owns MARTS_BUILD; reads from RAW.S3_SPARK_STAGE
    via the explicit grant in infra/rbac.tf).

    Usage from CLI:
        dbt run-operation load_spark_staged_into_marts_build --profiles-dir .
#}
{% macro load_spark_staged_into_marts_build(stage='S3_SPARK_STAGE', stage_schema='RAW') %}

    {% if not execute %}
        {# parse-time no-op #}
    {% else %}

        {% set targets = [
            ('FCT_TRIPS',             'fct_trips/'),
            ('FCT_TRIPS_QUARANTINED', 'fct_trips_quarantined/'),
        ] %}

        {# Cold-start guard: if the destination tables don't exist yet (very
           first dbt run after fresh terraform apply), skip silently. dbt's
           build steps later in the DAG will create the empty tables; the
           NEXT run's clone+COPY will load any Spark-staged data normally. #}
        {% set existence_sql %}
            select count(*)
            from {{ target.database }}.information_schema.tables
            where table_schema = 'MARTS_BUILD'
              and table_name in ('FCT_TRIPS', 'FCT_TRIPS_QUARANTINED')
        {% endset %}
        {% set existing = run_query(existence_sql).columns[0].values()[0] | int %}

        {% if existing < 2 %}
            {% do log(
                "spark-load: MARTS_BUILD.FCT_TRIPS / FCT_TRIPS_QUARANTINED "
                "don't exist yet (cold start) — skipping. dbt's first build "
                "will create them; next run will load any Spark batches.",
                info=True
            ) %}
        {% else %}
            {% for table_name, prefix in targets %}
                {% set sql -%}
                    COPY INTO {{ target.database }}.MARTS_BUILD.{{ table_name }}
                    FROM @{{ target.database }}.{{ stage_schema }}.{{ stage }}/{{ prefix }}
                    FILE_FORMAT  = (FORMAT_NAME = '{{ target.database }}.{{ stage_schema }}.PARQUET_FF')
                    PATTERN      = '.*\.parquet'
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR     = 'ABORT_STATEMENT'
                    PURGE        = FALSE
                {%- endset %}
                {% do log("spark-load: COPY INTO MARTS_BUILD." ~ table_name ~ " FROM @" ~ stage ~ "/" ~ prefix, info=True) %}
                {% set result = run_query(sql) %}
                {% if result.rows %}
                    {% set total_loaded = namespace(n=0) %}
                    {% for row in result.rows %}
                        {# COPY result rows: file, status, rows_parsed, rows_loaded, ... #}
                        {% if row[1] | lower == 'loaded' %}
                            {% set total_loaded.n = total_loaded.n + (row[3] | int) %}
                        {% endif %}
                    {% endfor %}
                    {% do log("  files=" ~ (result.rows | length) ~ "  rows_loaded=" ~ total_loaded.n, info=True) %}
                {% else %}
                    {% do log("  no new files staged (already-loaded files dedupe via Snowflake history)", info=True) %}
                {% endif %}
            {% endfor %}
        {% endif %}

    {% endif %}

{% endmacro %}
