{#
    drop_historical_iceberg — pre-destroy cleanup for the unmanaged
                              Iceberg tables.

    Why this exists: the three `ANALYTICS.HISTORICAL.{DAILY_AGG,
    SUPPLY_GAPS, TIP_BEHAVIOUR}` Iceberg tables are created out-of-band
    by the spark_historical DAG (CREATE ICEBERG TABLE IF NOT EXISTS), so
    Terraform doesn't track them. Without this drop, terraform destroy
    fails on the EXTERNAL VOLUME and CATALOG INTEGRATION because
    Snowflake refuses to drop either while ANY referencing Iceberg
    table still exists.

    Why a dbt macro (and not a snowflake_execute resource): the tables
    are OWNED by the DBT role. Terraform's snowflake provider runs as
    ACCOUNTADMIN — and in Snowflake even ACCOUNTADMIN can't DROP an
    object owned by another role without first taking ownership. Easier
    to invoke this macro from the DBT-credentialed `dbt` CLI (via
    `make infra-destroy`) than to wrangle ownership transfer in TF.

    Idempotent: IF EXISTS — safe on re-runs, on already-destroyed state,
    and on first-run-after-fresh-apply (when the DAG has never created
    the tables yet).

    Invocation:
        dbt run-operation drop_historical_iceberg \
            --project-dir dbt --profiles-dir dbt
#}
{% macro drop_historical_iceberg() %}

    {% if execute %}
        {% set tables = ['DAILY_AGG', 'SUPPLY_GAPS', 'TIP_BEHAVIOUR'] %}
        {% for tbl in tables %}
            {% set fqtn = target.database ~ '.HISTORICAL.' ~ tbl %}
            {% do run_query('DROP ICEBERG TABLE IF EXISTS ' ~ fqtn) %}
            {% do log("dropped " ~ fqtn ~ " (if it existed)", info=True) %}
        {% endfor %}
    {% endif %}

{% endmacro %}
