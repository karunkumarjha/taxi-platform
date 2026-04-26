#!/usr/bin/env bash
# dbt parse — used by pre-commit + CI to check that every dbt model's Jinja
# + SQL compiles cleanly. dbt parse doesn't need a real Snowflake connection;
# it just renders templates and resolves refs. We pass fake env vars so
# profiles.yml's env_var() lookups succeed at parse time.
#
# Exit non-zero on any compilation error (broken Jinja, missing ref, etc.).

set -euo pipefail

# Ensure profiles.yml exists locally — pre-commit runs from repo root, so
# `dbt parse --project-dir dbt --profiles-dir dbt` is what we need.
if [[ ! -f dbt/profiles.yml ]]; then
    cp dbt/profiles.yml.example dbt/profiles.yml
fi

SNOWFLAKE_ACCOUNT=fake \
SNOWFLAKE_USER=fake \
SNOWFLAKE_PASSWORD=fake \
SNOWFLAKE_ROLE=DBT \
SNOWFLAKE_WAREHOUSE=WH_XS \
SNOWFLAKE_DATABASE=ANALYTICS \
SNOWFLAKE_DBT_SCHEMA=MARTS_BUILD \
SNOWFLAKE_RAW_SCHEMA=RAW \
    uv run python -m dbt.cli.main parse \
        --project-dir dbt \
        --profiles-dir dbt
