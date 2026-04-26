#!/usr/bin/env bash
# =============================================================================
# with_role.sh — run a command with the Snowflake env vars set for one of
# the three functional roles (loader / dbt / analyst), or tf for Terraform.
#
# Usage:
#   ./scripts/with_role.sh <role> <command...>
#
# Examples:
#   ./scripts/with_role.sh loader uv run python -m ingestion.load_snowflake
#   ./scripts/with_role.sh dbt    uv run dbt build --project-dir dbt --profiles-dir dbt
#
# Reads .env (via `set -a; . ./.env; set +a`) for the per-role credentials,
# then exports SNOWFLAKE_USER / SNOWFLAKE_PASSWORD / SNOWFLAKE_ROLE matching
# the requested functional role. Everything downstream (ingestion modules,
# dbt's profiles.yml) just reads those generic vars — same as before.
# =============================================================================

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "usage: $0 <role> <command...>"
    echo "  roles: loader | dbt | analyst | tf"
    exit 64
fi

role="$1"; shift

# Source .env if it exists (single source of truth for creds locally).
if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a
else
    echo "warning: .env not found in $(pwd) — relying on existing exports"
fi

case "$role" in
    loader)
        export SNOWFLAKE_USER="${SNOWFLAKE_LOADER_USER:?missing SNOWFLAKE_LOADER_USER}"
        export SNOWFLAKE_PASSWORD="${SNOWFLAKE_LOADER_PASSWORD:?missing SNOWFLAKE_LOADER_PASSWORD}"
        export SNOWFLAKE_ROLE="${SNOWFLAKE_LOADER_ROLE:-LOADER}"
        ;;
    dbt)
        export SNOWFLAKE_USER="${SNOWFLAKE_DBT_USER:?missing SNOWFLAKE_DBT_USER}"
        export SNOWFLAKE_PASSWORD="${SNOWFLAKE_DBT_PASSWORD:?missing SNOWFLAKE_DBT_PASSWORD}"
        export SNOWFLAKE_ROLE="${SNOWFLAKE_DBT_ROLE:-DBT}"
        ;;
    analyst)
        export SNOWFLAKE_USER="${SNOWFLAKE_ANALYST_USER:?missing SNOWFLAKE_ANALYST_USER}"
        export SNOWFLAKE_PASSWORD="${SNOWFLAKE_ANALYST_PASSWORD:?missing SNOWFLAKE_ANALYST_PASSWORD}"
        export SNOWFLAKE_ROLE="${SNOWFLAKE_ANALYST_ROLE:-ANALYST}"
        ;;
    tf)
        export SNOWFLAKE_USER="${SNOWFLAKE_TF_USER:?missing SNOWFLAKE_TF_USER}"
        export SNOWFLAKE_PASSWORD="${SNOWFLAKE_TF_PASSWORD:?missing SNOWFLAKE_TF_PASSWORD}"
        export SNOWFLAKE_ROLE="${SNOWFLAKE_TF_ROLE:-ACCOUNTADMIN}"

        # The Snowflake provider's new (non-deprecated) API wants the account
        # split into organization + account name. Users keep ONE value in
        # .env (SNOWFLAKE_ACCOUNT="ORG-ACCOUNT"); we split here for Terraform.
        if [[ -z "${SNOWFLAKE_ACCOUNT:-}" ]]; then
            echo "with_role.sh tf: SNOWFLAKE_ACCOUNT is required" >&2
            exit 65
        fi
        if [[ "$SNOWFLAKE_ACCOUNT" != *-* ]]; then
            echo "with_role.sh tf: SNOWFLAKE_ACCOUNT='$SNOWFLAKE_ACCOUNT' must be in ORG-ACCOUNT format" >&2
            exit 65
        fi
        export TF_VAR_snowflake_organization_name="${SNOWFLAKE_ACCOUNT%%-*}"
        export TF_VAR_snowflake_account_name="${SNOWFLAKE_ACCOUNT##*-}"

        # Unset the deprecated SNOWFLAKE_ACCOUNT so the Snowflake provider
        # doesn't emit a deprecation warning. Other commands (load_snowflake.py,
        # dbt) still see SNOWFLAKE_ACCOUNT — they read .env at their own start.
        unset SNOWFLAKE_ACCOUNT
        ;;
    *)
        echo "unknown role: $role"
        echo "valid roles: loader | dbt | analyst | tf"
        exit 64
        ;;
esac

exec "$@"
