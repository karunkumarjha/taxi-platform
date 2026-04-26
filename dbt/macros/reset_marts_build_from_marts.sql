{#
    reset_marts_build_from_marts — refresh MARTS_BUILD to mirror production
                                    MARTS at the start of every dbt run.

    Why: every dbt model is incremental and uses count-divergence detection
    against {{ this }} = MARTS_BUILD.<table>. After swap_marts, MARTS_BUILD
    holds the OLD production state (one swap behind), so the next run would
    detect divergence against stale data and SWAP a corrupt result into MARTS
    — every run would produce a wrong-state MARTS.

    Solution: at the start of every run, clone each TABLE from MARTS into
    MARTS_BUILD so the incremental build extends current production state.
    After build + SWAP, MARTS gets the new state; MARTS_BUILD becomes the
    previous MARTS — and the next run starts the cycle again.

    Why table-level CLONE (and not `CREATE OR REPLACE SCHEMA … CLONE`):
        Schema-level CREATE OR REPLACE drops and recreates the schema object,
        stripping FUTURE-TABLE grants for ANALYST. Cloning the tables
        individually preserves all schema-level grants.

    Why we match the TRANSIENT keyword:
        dbt-snowflake materialises tables as TRANSIENT by default. CLONE
        requires the target's "transience" to match the source — otherwise
        Snowflake errors with "Transient object cannot be cloned to a
        permanent object." We read each source's is_transient flag from
        INFORMATION_SCHEMA and use the matching CREATE OR REPLACE syntax.

    `CREATE OR REPLACE [TRANSIENT] TABLE … CLONE …` is metadata-only in
    Snowflake: zero storage cost, sub-second per table.

    Failure-safe: if a build fails mid-way (e.g. a dbt test fails), the next
    run's clone wipes the polluted MARTS_BUILD tables and rebuilds cleanly
    from MARTS. Test failures self-heal on the next run; MARTS is never
    corrupted because SWAP only fires on dbt_build_marts success.

    Views (e.g. stg_yellow_trips) and seeds (dim_zones) are NOT cloned —
    dbt seed + dbt_build_staging recreate them on every run anyway.

    Must run BEFORE `dbt seed` and `dbt build` (the cloned tables become
    the {{ this }} target for the incremental models).

    Usage from CLI:
        dbt run-operation reset_marts_build_from_marts --profiles-dir .
#}
{% macro reset_marts_build_from_marts(prod_schema='MARTS', build_schema='MARTS_BUILD') %}

    {% if not execute %}
        {# parse-time no-op #}
    {% else %}

        {% set get_tables_sql %}
            select table_name, is_transient
            from {{ target.database }}.information_schema.tables
            where table_schema = '{{ prod_schema }}'
              and table_type   = 'BASE TABLE'
            order by table_name
        {% endset %}

        {% set results = run_query(get_tables_sql) %}
        {% set rows = results.rows %}

        {% if rows | length == 0 %}
            {% do log(
                "blue-green: " ~ prod_schema ~ " is empty — nothing to clone "
                "(first run after fresh terraform apply, expected)",
                info=True
            ) %}
        {% else %}
            {% do log(
                "blue-green: cloning " ~ (rows | length) ~ " table(s) from "
                ~ prod_schema ~ " → " ~ build_schema,
                info=True
            ) %}
            {% for row in rows %}
                {% set tbl         = row[0] %}
                {% set is_trans    = (row[1] | string | upper) == 'YES' %}
                {% set transient_kw = 'TRANSIENT ' if is_trans else '' %}
                {% set sql -%}
                    CREATE OR REPLACE {{ transient_kw }}TABLE {{ target.database }}.{{ build_schema }}.{{ tbl }}
                        CLONE {{ target.database }}.{{ prod_schema }}.{{ tbl }}
                {%- endset %}
                {% do run_query(sql) %}
                {% do log("  cloned: " ~ tbl ~ (" (transient)" if is_trans else " (permanent)"), info=True) %}
            {% endfor %}
            {% do log(
                "blue-green: clone complete — incremental models will build "
                "from production baseline",
                info=True
            ) %}
        {% endif %}

    {% endif %}

{% endmacro %}
