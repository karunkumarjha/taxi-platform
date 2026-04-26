{#
    show_pending_rebuilds — preview what dbt would rebuild on the next run.

    Mirrors the count-divergence detection logic the incremental models use,
    but as a read-only query (no rebuild, no SWAP). Lets you see — without
    running dbt — which (year, month) tuples have row counts in FCT_TRIPS
    that don't match what's reflected in each aggregate.

    Output: one row per (mart, year, month) that would be rebuilt, with the
    fct count and the aggregate's summed trip_count side-by-side. Empty
    output means dbt has nothing to do.

    Used by `make status`.
#}
{% macro show_pending_rebuilds() %}

    {% if not execute %}
        {# parse-time no-op #}
    {% else %}

        {% set marts = [
            ('AGG_HOURLY_DEMAND',         'month'),
            ('AGG_ZONE_SUPPLY_GAPS',      'month'),
            ('AGG_ZONE_REVENUE_MONTHLY',  'year'),
            ('AGG_ZONE_TIP_BEHAVIOUR',    'year'),
        ] %}

        {% do log("", info=True) %}
        {% do log("=== dbt status: months/years that would rebuild on next run ===", info=True) %}

        {% set total = namespace(divergent=0) %}

        {% for mart, grain in marts %}
            {% if grain == 'month' %}
                {# Compare per-month fct count vs sum(trip_count) per month in mart #}
                {% if mart == 'AGG_ZONE_SUPPLY_GAPS' %}
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
                       or (a.agg_cnt is null and f.fct_cnt >= {{ var('phantom_month_threshold', 100000) }})
                    order by f.pickup_year, f.pickup_month
                {% endset %}
            {% else %}
                {# Year-grain: compare per-year totals #}
                {% set sql %}
                    with fct_counts as (
                        select pickup_year, count(*) as fct_cnt
                        from {{ target.database }}.MARTS.FCT_TRIPS
                        group by 1
                    ),
                    agg_counts as (
                        select pickup_year, sum(trip_count) as agg_cnt
                        from {{ target.database }}.MARTS.{{ mart }}
                        group by 1
                    )
                    select
                        '{{ mart }}'                                  as mart,
                        f.pickup_year                                 as year,
                        null                                          as month,
                        f.fct_cnt                                     as fct_count,
                        coalesce(a.agg_cnt, 0)                        as agg_count,
                        f.fct_cnt - coalesce(a.agg_cnt, 0)            as diff
                    from fct_counts f
                    left join agg_counts a using (pickup_year)
                    where coalesce(a.agg_cnt, 0) != f.fct_cnt
                       or (a.agg_cnt is null and f.fct_cnt >= {{ var('phantom_month_threshold', 100000) }})
                    order by f.pickup_year
                {% endset %}
            {% endif %}

            {% set rows = run_query(sql).rows %}
            {% if rows %}
                {% do log("", info=True) %}
                {% do log(mart ~ " (" ~ grain ~ "-grain)  — " ~ (rows | length) ~ " divergent slice(s):", info=True) %}
                {% do log("  year   month   fct_count    agg_count    diff", info=True) %}
                {% for row in rows %}
                    {% set total.divergent = total.divergent + 1 %}
                    {% set m_str = (row[2] | string) if row[2] is not none else 'all' %}
                    {% do log(
                        "  " ~ row[1] ~ "    " ~ m_str ~ "        "
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
