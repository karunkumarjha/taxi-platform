#!/usr/bin/env bash
# =============================================================================
# bootstrap.sh — fresh-clone → triggered Airflow DAGs in one command.
#
# Usage (from repo root):
#   ./scripts/bootstrap.sh
#
# What it does, in order:
#   1.  Pre-flight checks (uv, terraform, astro, docker, aws CLI, .env)
#   2.  uv sync + install pre-commit hooks
#   3.  terraform init + apply (idempotent — Terraform handles diffs)
#   4.  Append generated outputs to .env (idempotent — checks marker line)
#   5.  Render airflow/airflow_settings.yaml from .example template +
#       terraform outputs
#   6.  astro dev kill && astro dev start (forces fresh metadata DB so
#       airflow_settings.yaml re-imports)
#   7.  Wait for the scheduler to be ready
#   8.  Trigger spark_pipeline + dbt_pipeline for January 2023 (the
#       smoke-test month — TLC has it published, dbt's count-divergence
#       picks it up, Spark stages it to S3 for dbt to absorb)
#
# Pre-requisites you must do manually first (one-time per Snowflake account):
#   • Create a Snowflake trial at signup.snowflake.com.
#   • In Snowsight as ACCOUNTADMIN, create the TF_USER service user
#     (see the SQL block in the README's "Setup" section).
#   • Copy .env.example → .env and fill the three SNOWFLAKE_TF_* values
#     plus SNOWFLAKE_ACCOUNT.
#
# Idempotent: running it twice does the right thing — Terraform shows
# "no changes", .env appends are skipped, astro restarts cleanly.
# =============================================================================

set -euo pipefail

# Colour helpers.
if [[ -t 1 ]]; then
    BOLD=$'\033[1m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RED=$'\033[31m'; NC=$'\033[0m'
else
    BOLD=""; GREEN=""; YELLOW=""; RED=""; NC=""
fi

step()  { echo "${BOLD}${GREEN}==>${NC} ${BOLD}$*${NC}"; }
warn()  { echo "${YELLOW}!! $*${NC}" >&2; }
die()   { echo "${RED}xx $*${NC}" >&2; exit 1; }

# Always run from repo root, regardless of where we were invoked.
cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

# ----------------------------------------------------------------------------
# 1. Pre-flight
# ----------------------------------------------------------------------------

step "1/8  Pre-flight checks"

for cmd in uv terraform astro docker aws; do
    command -v "$cmd" >/dev/null 2>&1 || die "missing required tool: $cmd
        Install: see README's Prerequisites section."
done

docker info >/dev/null 2>&1 || die "docker daemon not running. Start Docker Desktop."

aws sts get-caller-identity >/dev/null 2>&1 || die "AWS credentials not working.
        Run \`aws configure\` or set AWS_PROFILE in .env."

[[ -f .env ]] || die ".env not found. Copy .env.example to .env and fill the
        three SNOWFLAKE_TF_* values + SNOWFLAKE_ACCOUNT before running again."

# Source .env to validate required pre-apply values are set.
set -a
# shellcheck disable=SC1091
. ./.env
set +a

for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_TF_USER SNOWFLAKE_TF_PASSWORD AWS_REGION; do
    [[ -n "${!var:-}" ]] || die "$var is empty in .env. Fill it before re-running."
done

if [[ "$SNOWFLAKE_TF_PASSWORD" == "change-me" ]]; then
    die "SNOWFLAKE_TF_PASSWORD is still the placeholder ('change-me') in .env.
        Set it to the real password you used when creating TF_USER in Snowsight."
fi

echo "    .env looks good (SNOWFLAKE_ACCOUNT=$SNOWFLAKE_ACCOUNT, AWS_REGION=$AWS_REGION)"

# ----------------------------------------------------------------------------
# 2. Python deps + hooks
# ----------------------------------------------------------------------------

step "2/8  Installing Python deps + pre-commit hooks"

uv sync --all-groups --quiet
uv run pre-commit install --install-hooks >/dev/null

# ----------------------------------------------------------------------------
# 3. Terraform init + apply
# ----------------------------------------------------------------------------

step "3/8  Provisioning AWS + Snowflake via Terraform (~3 min on first run)"

# Non-interactive apply for the bootstrap path. Bypasses `make infra-apply`
# (which prompts for yes/no — appropriate for an engineer running it by
# hand, but a blocker for the bootstrap-as-script flow). The `-input=false`
# also short-circuits any "missing variable" prompt — variables.tf has
# defaults, and the TF_VAR_* env vars set by with_role.sh tf cover the
# Snowflake org/account split, so there are no required prompts.
make infra-init >/dev/null
./scripts/with_role.sh tf bash -c \
    'cd infra && terraform apply -auto-approve -input=false'

# ----------------------------------------------------------------------------
# 4. Append terraform outputs to .env (idempotent)
# ----------------------------------------------------------------------------

step "4/8  Capturing terraform outputs into .env"

OUTPUT_MARKER="# --- bootstrap.sh: terraform outputs ---"

if grep -qF "$OUTPUT_MARKER" .env; then
    echo "    .env already has terraform outputs — skipping append"
else
    cat <<EOF >> .env

$OUTPUT_MARKER
S3_BUCKET=$(terraform -chdir=infra output -raw s3_bucket)
EMR_APPLICATION_ID=$(terraform -chdir=infra output -raw emr_application_id)
EMR_EXEC_ROLE_ARN=$(terraform -chdir=infra output -raw emr_exec_role_arn)
SNOWFLAKE_LOADER_PASSWORD=$(terraform -chdir=infra output -raw snowflake_loader_password)
SNOWFLAKE_DBT_PASSWORD=$(terraform -chdir=infra output -raw snowflake_dbt_password)
SNOWFLAKE_ANALYST_PASSWORD=$(terraform -chdir=infra output -raw snowflake_analyst_password)
EOF
    echo "    appended terraform outputs to .env"
fi

# Re-source so subsequent steps see the new values.
set -a; . ./.env; set +a

# ----------------------------------------------------------------------------
# 5. Render airflow_settings.yaml
# ----------------------------------------------------------------------------

step "5/8  Rendering airflow/airflow_settings.yaml"

S3_BUCKET=$(terraform -chdir=infra output -raw s3_bucket)
EMR_APPLICATION_ID=$(terraform -chdir=infra output -raw emr_application_id)
EMR_EXEC_ROLE_ARN=$(terraform -chdir=infra output -raw emr_exec_role_arn)
LOADER_PWD=$(terraform -chdir=infra output -raw snowflake_loader_password)
DBT_PWD=$(terraform -chdir=infra output -raw snowflake_dbt_password)

# Use a sed-incompatible delimiter (|) since some values contain slashes.
sed \
    -e "s|__SNOWFLAKE_ACCOUNT__|${SNOWFLAKE_ACCOUNT}|g" \
    -e "s|__SNOWFLAKE_LOADER_PASSWORD__|${LOADER_PWD}|g" \
    -e "s|__SNOWFLAKE_DBT_PASSWORD__|${DBT_PWD}|g" \
    -e "s|__S3_BUCKET__|${S3_BUCKET}|g" \
    -e "s|__EMR_APPLICATION_ID__|${EMR_APPLICATION_ID}|g" \
    -e "s|__EMR_EXEC_ROLE_ARN__|${EMR_EXEC_ROLE_ARN}|g" \
    -e "s|__AWS_REGION__|${AWS_REGION}|g" \
    airflow/airflow_settings.yaml.example > airflow/airflow_settings.yaml

echo "    rendered airflow/airflow_settings.yaml from template"

# ----------------------------------------------------------------------------
# 6. Astro: kill (wipes metadata) + start (re-imports settings)
# ----------------------------------------------------------------------------

step "6/8  Bringing up Airflow via Astro (kill + start to refresh settings)"

# Astro's runtime base image has `ONBUILD COPY packages.txt requirements.txt .`
# instructions that fire unconditionally. The files must exist (even if empty)
# or the image build fails. Guard for fresh clones / accidental deletions.
[[ -f airflow/packages.txt ]]     || echo "# (empty)" > airflow/packages.txt
[[ -f airflow/requirements.txt ]] || echo "# (empty)" > airflow/requirements.txt

# `astro dev kill` wipes the metadata volume so airflow_settings.yaml
# re-imports cleanly. Without this, prior runs' connections + variables
# would persist with stale values.
(cd airflow && astro dev kill >/dev/null 2>&1 || true)
(cd airflow && astro dev start)

# ----------------------------------------------------------------------------
# 7. Wait for scheduler ready
# ----------------------------------------------------------------------------

step "7/8  Waiting for Airflow scheduler to be ready (~30s)"

# Poll the webserver until 200, max 90s.
for i in $(seq 1 30); do
    if curl -fsS http://localhost:8080/api/v1/health >/dev/null 2>&1 \
       || curl -fsS http://localhost:8080/health  >/dev/null 2>&1; then
        echo "    Airflow is up"
        break
    fi
    sleep 3
    if [[ $i -eq 30 ]]; then
        warn "Airflow didn't respond on /health within 90s — DAG triggers may fail.
        Try: cd airflow && astro dev logs"
    fi
done

# ----------------------------------------------------------------------------
# 8. Trigger both DAGs for Jan 2023 (the smoke-test month)
# ----------------------------------------------------------------------------

step "8/8  Triggering spark_pipeline (year=2023, 12 mapped tasks) + dbt_pipeline (2023-01)"

# spark_pipeline takes year (and optional month) Params — manual-trigger
# only. Year=2023 fans out into 12 mapped task instances (one per month).
# dbt_pipeline still uses logical_date semantics with lag_months=0 for
# backfill; it ingests + COPYs Spark's staged data + builds aggregates.
# Natural-key dedup ensures Spark and dbt rows for the same trip collapse
# to one row in MARTS.

(cd airflow && astro dev run dags trigger spark_pipeline \
    --conf '{"year": 2023, "force": true}')

(cd airflow && astro dev run dags trigger dbt_pipeline \
    --logical-date 2023-01-01 \
    --conf '{"lag_months": 0}')

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------

cat <<EOF

${BOLD}${GREEN}✓ bootstrap complete.${NC}

  Airflow UI :  http://localhost:8080  (admin / admin)

  DAGs to watch:
    • spark_pipeline   (12 mapped tasks for all of 2023, stages to s3://${S3_BUCKET}/staged-marts/)
    • dbt_pipeline     (ingest 2023-01 + COPY + dbt build + swap into MARTS)

  Once the dbt run completes, verify in Snowsight:
    USE ROLE ANALYST; USE WAREHOUSE WH_XS;
    SELECT COUNT(*) FROM ANALYTICS.MARTS.FCT_TRIPS;

  When you're done:
    cd airflow && astro dev stop
    make infra-destroy

EOF
