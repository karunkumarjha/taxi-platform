{#
    show_pending_rebuilds — audit aggregate vs FCT_TRIPS drift.
    Read-only. Empty output = marts in sync. Used by `make status`.
    All four marts are month-grain (tip_behaviour was promoted post-refactor).
#}
{% macro show_pending_rebuilds() %}

    {% if not execute %}
        {# parse-time no-op #}
    {% else %}

        {% set marts = [
            'AGG_HOURLY_DEMAND_MONTHLY',
            'AGG_ZONE_SUPPLY_GAPS_DAILY',
            'AGG_ZONE_REVENUE_MONTHLY',
            'AGG_ZONE_TIP_BEHAVIOUR_MONTHLY',
        ] %}

        {% do log("", info=True) %}
        {% do log("=== dbt status: months that would rebuild on next run ===", info=True) %}

        {% set total = namespace(divergent=0) %}

        {% for mart in marts %}
            {# Compare per-month fct count vs sum(trip_count) per month in mart.
               supply_gaps stores pickup_date (no year/month columns), so we
               derive year/month via extract() for that one. #}
            {% if mart == 'AGG_ZONE_SUPPLY_GAPS_DAILY' %}
                {% set agg_group = "extract(year from pickup_date)::int as pickup_year, extract(month from pickup_date)::int as pickup_month" %}
            {% else %}
                {% set agg_group = "pickup_year, pickup_month" %}
            {% endif %}
            {% set sql %}
                with fct_counts as (
                    select pickup_year, pickup_month, count(*) as fct_cnt
                    from {{ target.database }}.MARTS.FCT_TRIPS
                    group by 1, 2
                ),
                agg_counts as (
                    select {{ agg_group }}, sum(trip_count) as agg_cnt
                    from {{ target.database }}.MARTS.{{ mart }}
                    group by 1, 2
                )
                select
                    '{{ mart }}'                                  as mart,
                    f.pickup_year                                 as year,
                    f.pickup_month                                as month,
                    f.fct_cnt                                     as fct_count,
                    coalesce(a.agg_cnt, 0)                        as agg_count,
                    f.fct_cnt - coalesce(a.agg_cnt, 0)            as diff
                from fct_counts f
                left join agg_counts a using (pickup_year, pickup_month)
                where coalesce(a.agg_cnt, 0) != f.fct_cnt
                order by f.pickup_year, f.pickup_month
            {% endset %}

            {% set rows = run_query(sql).rows %}
            {% if rows %}
                {% do log("", info=True) %}
                {% do log(mart ~ " (month-grain)  — " ~ (rows | length) ~ " divergent slice(s):", info=True) %}
                {% do log("  year   month   fct_count    agg_count    diff", info=True) %}
                {% for row in rows %}
                    {% set total.divergent = total.divergent + 1 %}
                    {% do log(
                        "  " ~ row[1] ~ "    " ~ row[2] ~ "        "
                        ~ row[3] ~ "        " ~ row[4] ~ "        " ~ row[5],
                        info=True
                    ) %}
                {% endfor %}
            {% endif %}
        {% endfor %}

        {% do log("", info=True) %}
        {% if total.divergent == 0 %}
            {% do log(
                "✓ No pending rebuilds — every mart matches FCT_TRIPS for every "
                "(year, month). Next dbt run will be a near no-op.",
                info=True
            ) %}
        {% else %}
            {% do log(
                "→ " ~ total.divergent ~ " divergent slice(s) total. Run `make dbt` "
                "(or trigger dbt_pipeline) to rebuild.",
                info=True
            ) %}
        {% endif %}

    {% endif %}

{% endmacro %}
