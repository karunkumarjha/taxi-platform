"""
NYC Taxi Pattern Analytics — Streamlit-in-Snowflake app.

Replaces the deprecated Snowsight Dashboards (retired April 2026).
Reads from the dbt-built MARTS layer:
    AGG_ZONE_REVENUE_MONTHLY, AGG_HOURLY_DEMAND,
    AGG_ZONE_SUPPLY_GAPS, AGG_ZONE_TIP_BEHAVIOUR.

App runs as the DASHBOARD role — read-only on MARTS, can't see RAW or
write anywhere. RBAC enforced at the Snowflake level.

Deployment: see scripts/deploy_streamlit.py.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from snowflake.snowpark.context import get_active_session

# --- Page config -----------------------------------------------------------
st.set_page_config(
    page_title="Taxi Pattern Analytics",
    page_icon="🚖",
    layout="wide",
)

session = get_active_session()


@st.cache_data(ttl=600)
def query(sql: str) -> pd.DataFrame:
    """Run SQL via Snowpark, return pandas DataFrame. Cached 10 min per
    distinct SQL string."""
    return session.sql(sql).to_pandas()


# Year filter (lets the same app serve multiple years as backfill catches up)
years_df = query(
    "SELECT DISTINCT pickup_year FROM ANALYTICS.MARTS.AGG_ZONE_REVENUE_MONTHLY ORDER BY 1 DESC"
)
years = years_df["PICKUP_YEAR"].tolist() or [2023]

st.title("🚖 NYC Taxi Pattern Analytics")
st.caption(
    "Backed by ANALYTICS.MARTS aggregates (dbt) + S3 daily aggregates (Spark/EMR). "
    "Pipeline: TLC → S3 → Snowflake → dbt + Spark, orchestrated by Airflow. "
    "Running as **DASHBOARD** role — read-only on MARTS by RBAC design."
)

YEAR = st.selectbox("Year", years, index=0)

st.divider()

# ============================================================================
# Q1 — Zone Revenue
# ============================================================================
st.header("Q1 · Zone Revenue by Month")
st.markdown(
    "Which pickup zones generate the most revenue, and does the ranking "
    "shift meaningfully month-to-month?"
)

q1 = query(f"""
    SELECT pickup_month,
           pu_zone,
           pu_borough,
           ROUND(gross_revenue, 2)              AS gross_revenue_usd,
           revenue_rank_in_month,
           rank_change_vs_prev_month
    FROM ANALYTICS.MARTS.AGG_ZONE_REVENUE_MONTHLY
    WHERE pickup_year = {YEAR}
    ORDER BY pickup_month, revenue_rank_in_month
""")

# Top-20 zones by full-year revenue
yearly = (
    q1.groupby("PU_ZONE")["GROSS_REVENUE_USD"]
    .sum()
    .reset_index()
    .sort_values("GROSS_REVENUE_USD", ascending=False)
)
top20 = yearly.head(20).copy()
top10_zones = yearly.head(10)["PU_ZONE"].tolist()

c1, c2 = st.columns([1, 2])

with c1:
    st.subheader(f"Top 20 zones · {YEAR}")
    st.dataframe(
        top20.rename(columns={
            "PU_ZONE": "Zone",
            "GROSS_REVENUE_USD": "Revenue (USD)",
        }),
        hide_index=True,
        use_container_width=True,
        column_config={
            "Revenue (USD)": st.column_config.NumberColumn(format="$%.0f"),
        },
    )

with c2:
    st.subheader("Monthly revenue trend — top 10 zones")
    line_df = q1[q1["PU_ZONE"].isin(top10_zones)].copy()
    # Preserve top-10 order in the legend so colours match revenue rank
    line_df["PU_ZONE"] = pd.Categorical(line_df["PU_ZONE"], categories=top10_zones, ordered=True)
    line_df = line_df.sort_values(["PU_ZONE", "PICKUP_MONTH"])
    fig = px.line(
        line_df,
        x="PICKUP_MONTH",
        y="GROSS_REVENUE_USD",
        color="PU_ZONE",
        labels={
            "PICKUP_MONTH": "Month",
            "GROSS_REVENUE_USD": "Revenue (USD)",
            "PU_ZONE": "Zone",
        },
        markers=True,
    )
    fig.update_layout(
        height=500,
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis=dict(tickmode="linear", dtick=1),
        legend=dict(title="Zone"),
    )
    st.plotly_chart(fig, use_container_width=True)

# Rank shift narrative
shifts = (
    q1[q1["PU_ZONE"].isin(top10_zones)]
    .dropna(subset=["RANK_CHANGE_VS_PREV_MONTH"])
)
if not shifts.empty:
    biggest_climb = shifts.loc[shifts["RANK_CHANGE_VS_PREV_MONTH"].idxmax()]
    biggest_drop = shifts.loc[shifts["RANK_CHANGE_VS_PREV_MONTH"].idxmin()]
    st.markdown(
        f"**Biggest single-month climb**: {biggest_climb['PU_ZONE']} "
        f"(+{int(biggest_climb['RANK_CHANGE_VS_PREV_MONTH'])} ranks "
        f"in month {int(biggest_climb['PICKUP_MONTH'])})  •  "
        f"**Biggest drop**: {biggest_drop['PU_ZONE']} "
        f"({int(biggest_drop['RANK_CHANGE_VS_PREV_MONTH'])} ranks "
        f"in month {int(biggest_drop['PICKUP_MONTH'])})"
    )

st.divider()

# ============================================================================
# Q2 — Demand Timing
# ============================================================================
st.header("Q2 · Demand Timing")
st.markdown(
    "How do trip volume and average fare vary across the hour-of-day "
    "× day-of-week grid?"
)

q2 = query(f"""
    SELECT pickup_dow,
           pickup_hour,
           SUM(trip_count)                                                  AS trips,
           ROUND(SUM(trip_count * avg_fare) / NULLIF(SUM(trip_count), 0), 2) AS avg_fare_usd
    FROM ANALYTICS.MARTS.AGG_HOURLY_DEMAND
    WHERE pickup_year = {YEAR}
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
q2["DAY"] = q2["PICKUP_DOW"].apply(lambda d: DAYS[int(d) - 1])

c1, c2 = st.columns(2)

with c1:
    st.subheader("Trip volume — hour × day")
    pivot_trips = (
        q2.pivot_table(index="DAY", columns="PICKUP_HOUR", values="TRIPS")
        .reindex(DAYS)
    )
    fig = px.imshow(
        pivot_trips,
        labels=dict(x="Hour", y="Day", color="Trips"),
        aspect="auto",
        color_continuous_scale="Blues",
    )
    fig.update_layout(height=380, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Avg fare — hour × day (USD)")
    pivot_fare = (
        q2.pivot_table(index="DAY", columns="PICKUP_HOUR", values="AVG_FARE_USD")
        .reindex(DAYS)
    )
    fig = px.imshow(
        pivot_fare,
        labels=dict(x="Hour", y="Day", color="Avg fare USD"),
        aspect="auto",
        color_continuous_scale="Reds",
    )
    fig.update_layout(height=380, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Peak/trough callouts
peak = q2.loc[q2["TRIPS"].idxmax()]
trough = q2.loc[q2["TRIPS"].idxmin()]
st.markdown(
    f"**Peak**: {peak['DAY']} {int(peak['PICKUP_HOUR']):02d}:00 — "
    f"{int(peak['TRIPS']):,} trips, ${peak['AVG_FARE_USD']:.2f} avg fare  •  "
    f"**Trough**: {trough['DAY']} {int(trough['PICKUP_HOUR']):02d}:00 — "
    f"{int(trough['TRIPS']):,} trips, ${trough['AVG_FARE_USD']:.2f} avg fare"
)

st.divider()

# ============================================================================
# Q3 — Supply Gaps
# ============================================================================
st.header("Q3 · Supply Gaps")
st.markdown(
    "Are there zones that regularly go extended periods with no pickups? "
    "What's the longest observed gap per zone per day?"
)

q3 = query(f"""
    SELECT pu_zone,
           pu_borough,
           DATE_TRUNC('month', pickup_date)::DATE AS month,
           AVG(longest_gap_min) / 60.0            AS avg_longest_gap_hr,
           SUM(gaps_gt_1h)                        AS gaps_gt_1h,
           SUM(gaps_gt_3h)                        AS gaps_gt_3h
    FROM ANALYTICS.MARTS.AGG_ZONE_SUPPLY_GAPS
    WHERE pickup_date BETWEEN '{YEAR}-01-01' AND '{YEAR}-12-31'
    GROUP BY 1, 2, 3
""")

zone_totals = (
    q3.groupby(["PU_ZONE", "PU_BOROUGH"])[["GAPS_GT_1H", "GAPS_GT_3H"]]
    .sum()
    .reset_index()
    .sort_values("GAPS_GT_1H", ascending=False)
)

c1, c2 = st.columns([1, 2])

with c1:
    st.subheader("Worst zones (most 1h+ gaps)")
    st.dataframe(
        zone_totals.head(20).rename(columns={
            "PU_ZONE": "Zone",
            "PU_BOROUGH": "Borough",
            "GAPS_GT_1H": "≥ 1h gaps",
            "GAPS_GT_3H": "≥ 3h gaps",
        }),
        hide_index=True,
        use_container_width=True,
    )

with c2:
    worst50 = zone_totals.head(50)["PU_ZONE"].tolist()
    st.subheader("Avg longest gap (hours) — month × zone")
    pivot_gaps = (
        q3[q3["PU_ZONE"].isin(worst50)]
        .pivot_table(index="PU_ZONE", columns="MONTH", values="AVG_LONGEST_GAP_HR")
        .reindex(worst50)
    )
    fig = px.imshow(
        pivot_gaps,
        labels=dict(x="Month", y="Zone", color="Avg longest gap (hr)"),
        aspect="auto",
        color_continuous_scale="Reds",
    )
    fig.update_layout(height=600, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ============================================================================
# Q4 — Tip Behaviour
# ============================================================================
st.header("Q4 · Tip Behaviour")
st.markdown(
    "Tip % by trip distance and payment type. Note: only credit-card trips "
    "(`payment_type = 1`) report tips — cash tips aren't recorded by the meter."
)

q4 = query(f"""
    SELECT distance_bucket,
           pu_zone,
           pu_borough,
           trip_count,
           ROUND(avg_tip_pct_credit * 100, 2)  AS avg_tip_pct_credit
    FROM ANALYTICS.MARTS.AGG_ZONE_TIP_BEHAVIOUR
    WHERE pickup_year = {YEAR}
      AND payment_type = 1
      AND trip_count >= 500
""")

BUCKET_ORDER = ["0-1mi", "1-3mi", "3-5mi", "5-10mi", "10-20mi", "20mi+"]

# Citywide avg tip % by distance
city_avg = (
    q4.groupby("DISTANCE_BUCKET")["AVG_TIP_PCT_CREDIT"]
    .mean()
    .round(2)
    .reset_index()
)
city_avg["DISTANCE_BUCKET"] = pd.Categorical(
    city_avg["DISTANCE_BUCKET"], categories=BUCKET_ORDER, ordered=True
)
city_avg = city_avg.sort_values("DISTANCE_BUCKET")

c1, c2 = st.columns(2)

with c1:
    st.subheader("Citywide avg tip % by distance (credit only)")
    fig = px.bar(
        city_avg,
        x="DISTANCE_BUCKET", y="AVG_TIP_PCT_CREDIT",
        labels={"DISTANCE_BUCKET": "Trip distance", "AVG_TIP_PCT_CREDIT": "Avg tip %"},
        text="AVG_TIP_PCT_CREDIT",
        color_discrete_sequence=["#4C9F70"],
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig.update_layout(height=380, margin=dict(l=0, r=0, t=20, b=0), yaxis_title="Avg tip %")
    st.plotly_chart(fig, use_container_width=True)

with c2:
    zone_tips = (
        q4.groupby(["PU_ZONE", "PU_BOROUGH"])["AVG_TIP_PCT_CREDIT"]
        .mean()
        .round(2)
        .reset_index()
    )
    top10 = zone_tips.nlargest(10, "AVG_TIP_PCT_CREDIT").assign(group="Top 10 (highest tippers)")
    bottom10 = zone_tips.nsmallest(10, "AVG_TIP_PCT_CREDIT").assign(group="Bottom 10 (lowest tippers)")
    combined = pd.concat([top10, bottom10])
    st.subheader("Top / bottom tipping zones (credit, all distances)")
    fig = px.bar(
        combined,
        x="AVG_TIP_PCT_CREDIT", y="PU_ZONE", color="group",
        orientation="h",
        labels={"AVG_TIP_PCT_CREDIT": "Avg tip %", "PU_ZONE": "Zone", "group": ""},
    )
    fig.update_layout(
        height=550, margin=dict(l=0, r=0, t=20, b=0),
        yaxis=dict(autorange="reversed"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.caption(
    "Pipeline: dbt builds atomic + aggregate marts via blue-green swap; "
    "Spark runs month-by-month on EMR Serverless for historical roll-ups; "
    "Airflow orchestrates both. RBAC: app runs as DASHBOARD role."
)
