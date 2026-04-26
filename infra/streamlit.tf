# =============================================================================
# Streamlit-in-Snowflake — replacement for the deprecated Snowsight Dashboards.
#
# Two Snowflake objects:
#   1. STAGE  STREAMLIT_APP_STAGE   — hosts the app source files (app.py +
#                                     environment.yml). Files are PUT here by
#                                     scripts/deploy_streamlit.py (Terraform
#                                     can't PUT local files into a stage).
#   2. STREAMLIT ANALYTICS_APP      — the actual Streamlit object. Reads the
#                                     stage at runtime, executes app.py, runs
#                                     queries on the QUERY_WAREHOUSE.
#
# RBAC:
#   - DASHBOARD role gets USAGE on the streamlit. The app then physically
#     can't read RAW or write anything (DASHBOARD only has SELECT on MARTS).
# =============================================================================

resource "snowflake_stage" "streamlit_app" {
  name     = "STREAMLIT_APP_STAGE"
  database = snowflake_database.analytics.name
  schema   = snowflake_schema.marts.name
  comment  = "Hosts the Streamlit app files (app.py + environment.yml)"
}

resource "snowflake_streamlit" "analytics_app" {
  name      = "ANALYTICS_APP"
  database  = snowflake_database.analytics.name
  schema    = snowflake_schema.marts.name
  stage     = "${snowflake_database.analytics.name}.${snowflake_schema.marts.name}.${snowflake_stage.streamlit_app.name}"
  main_file = "/app.py"

  query_warehouse = snowflake_warehouse.wh_xs.name

  title   = "Taxi Pattern Analytics"
  comment = "Replaces deprecated Snowsight Dashboards. Answers Q1-Q4 from the marts."

  depends_on = [snowflake_grant_ownership.marts_to_dbt]
}

# DASHBOARD role can use the app (which then runs queries with DASHBOARD's
# privileges — SELECT on MARTS only).
resource "snowflake_grant_privileges_to_account_role" "streamlit_dashboard_usage" {
  account_role_name = snowflake_account_role.dashboard.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STREAMLIT"
    object_name = "${snowflake_database.analytics.name}.${snowflake_schema.marts.name}.${snowflake_streamlit.analytics_app.name}"
  }
}

# ANALYST also gets USAGE so analysts can use the same app for ad-hoc viewing.
resource "snowflake_grant_privileges_to_account_role" "streamlit_analyst_usage" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STREAMLIT"
    object_name = "${snowflake_database.analytics.name}.${snowflake_schema.marts.name}.${snowflake_streamlit.analytics_app.name}"
  }
}
