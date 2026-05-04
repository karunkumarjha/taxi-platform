#!/usr/bin/env bash
# =============================================================================
# bootstrap.sh — fresh-clone → triggered Airflow DAGs in one command.
#
# Usage (from repo root):
#   ./scripts/bootstrap.sh
#
# What it does, in order:
#   1.  Pre-flight checks (uv, terraform, astro, docker, aws CLI). If .env
#       doesn't exist, prompt interactively for the 5 required values and
#       create it. If .env exists, validate the values are non-placeholder.
#   2.  uv sync + install pre-commit hooks
#   3.  terraform init + apply — provisions AWS (S3, IAM, EMR Serverless app,
#       Glue Data Catalog) and Snowflake (DB, schemas including HISTORICAL
#       for Iceberg, warehouse, RBAC, EXTERNAL VOLUME, CATALOG INTEGRATION).
#   4.  Append generated outputs to .env (idempotent — checks marker line).
#       Captures S3, EMR, Glue, Snowflake EXTERNAL VOLUME / CATALOG
#       INTEGRATION names so CLI tools (make spark-deploy etc.) don't have
#       to re-query terraform.
#   5.  Render airflow/airflow_settings.yaml + airflow/.env from .example
#       templates. Substitutes Snowflake creds, EMR IDs, Glue DB, Iceberg
#       resource names, alert email, and Gmail app password.
#   6.  Deploy spark/process_historical.py to s3://<bucket>/spark-scripts/
#       so EMR Serverless can fetch it on job start. (Re-run via
#       `make spark-deploy` after script edits.)
#   7.  astro dev kill && astro dev start (forces fresh metadata DB so
#       airflow_settings.yaml re-imports cleanly).
#   8.  Wait for the scheduler to be ready.
#   9.  Unpause dbt_pipeline. With catchup=False + @monthly schedule,
#       Airflow creates exactly ONE scheduled DagRun for the most recent
#       cron tick — no backfill, no duplicate manual run. That run uses
#       lag_months=2, targeting the most recently published TLC month,
#       and walks the full medallion path: ingest TLC parquet → S3 →
#       COPY into RAW (Bronze) → snapshot (Silver SCD) → staging →
#       intermediate (merge on trip_bk) → marts (Gold) → blue-green
#       swap. spark_historical stays paused (no schedule; manual-trigger
#       from UI only).
#
# Pre-requisites you must do manually first (one-time):
#   • Snowflake trial at signup.snowflake.com.
#   • In Snowsight as ACCOUNTADMIN, create the TF_USER service user
#     (the SQL block is in the README's "Setup" section — also printed
#     when this script prompts you for the password if .env is missing).
#   • Generate a Gmail app password at https://myaccount.google.com/apppasswords
#     for Airflow failure-alert emails.
#   • `aws configure` so `aws sts get-caller-identity` works.
#
# Everything else — including .env creation, EMR Serverless app, AWS Glue
# Data Catalog, and the Snowflake EXTERNAL VOLUME / CATALOG INTEGRATION
# wiring for Iceberg — is automated. Re-running is safe: Terraform shows
# "no changes", .env appends are skipped, astro restarts cleanly, and the
# smoke-test trigger queues a fresh DAG run.
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

step "1/9  Pre-flight checks"

for cmd in uv terraform astro docker aws; do
    command -v "$cmd" >/dev/null 2>&1 || die "missing required tool: $cmd
        Install: see README's Prerequisites section."
done

docker info >/dev/null 2>&1 || die "docker daemon not running. Start Docker Desktop."

aws sts get-caller-identity >/dev/null 2>&1 || die "AWS credentials not working.
        Run \`aws configure\` or set AWS_PROFILE in .env."

# If .env doesn't exist, walk the user through creating it interactively.
# Prevents the "edit .env then re-run" two-step that bites first-timers.
if [[ ! -f .env ]]; then
    step "    .env not found — running interactive setup"

    cp .env.example .env

    cat <<'PROMPT'

I need 5 values. Press Enter on TF_USER to accept the default.
Passwords are read silently — paste and press Enter.

Don't have TF_USER yet? Run this in Snowsight as ACCOUNTADMIN, then come back:

    USE ROLE ACCOUNTADMIN;
    CREATE USER IF NOT EXISTS TF_USER
        PASSWORD             = '<choose-something-strong>'
        DEFAULT_ROLE         = ACCOUNTADMIN
        DEFAULT_WAREHOUSE    = COMPUTE_WH
        MUST_CHANGE_PASSWORD = FALSE;
    GRANT ROLE ACCOUNTADMIN TO USER TF_USER;

PROMPT

    read -rp "Snowflake account locator (e.g. ABCD-XY12345): " sf_account
    read -rp "Snowflake TF service user [TF_USER]: " sf_tf_user
    sf_tf_user="${sf_tf_user:-TF_USER}"
    read -rsp "Snowflake TF service password: " sf_tf_pwd; echo
    read -rp "Alert email (Gmail address for failure alerts): " alert_email
    read -rsp "Gmail app password (16 chars; spaces auto-stripped): " gmail_pwd; echo
    gmail_pwd="${gmail_pwd// /}"

    # Substitute values into .env. `|` is the sed delimiter so paths with
    # slashes are safe; passwords with literal `|` would break here, but
    # Snowflake passwords are typically alphanumeric so it's a non-issue
    # in practice.
    sed -i.bak \
        -e "s|^SNOWFLAKE_ACCOUNT=.*|SNOWFLAKE_ACCOUNT=${sf_account}|" \
        -e "s|^SNOWFLAKE_TF_USER=.*|SNOWFLAKE_TF_USER=${sf_tf_user}|" \
        -e "s|^SNOWFLAKE_TF_PASSWORD=.*|SNOWFLAKE_TF_PASSWORD=${sf_tf_pwd}|" \
        -e "s|^ALERT_EMAIL=.*|ALERT_EMAIL=${alert_email}|" \
        -e "s|^GMAIL_APP_PASSWORD=.*|GMAIL_APP_PASSWORD=${gmail_pwd}|" \
        .env
    rm -f .env.bak

    echo ""
    echo "    .env created. Continuing bootstrap..."
fi

# Source .env to validate required pre-apply values are set.
set -a
# shellcheck disable=SC1091
. ./.env
set +a

for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_TF_USER SNOWFLAKE_TF_PASSWORD AWS_REGION ALERT_EMAIL GMAIL_APP_PASSWORD; do
    [[ -n "${!var:-}" ]] || die "$var is empty in .env. Fill it before re-running."
done

if [[ "$SNOWFLAKE_TF_PASSWORD" == "change-me" ]]; then
    die "SNOWFLAKE_TF_PASSWORD is still the placeholder ('change-me') in .env.
        Set it to the real password you used when creating TF_USER in Snowsight."
fi

if [[ "$ALERT_EMAIL" == "you@example.com" ]]; then
    die "ALERT_EMAIL is still the placeholder ('you@example.com') in .env.
        Set it to the Gmail address that will send + receive failure alerts."
fi

if [[ "$GMAIL_APP_PASSWORD" == "change-me" ]]; then
    die "GMAIL_APP_PASSWORD is still the placeholder ('change-me') in .env.
        Generate one at https://myaccount.google.com/apppasswords and paste
        the 16-character password (spaces stripped) into .env."
fi

# Strip any spaces a user may have pasted from the Google UI.
GMAIL_APP_PASSWORD="${GMAIL_APP_PASSWORD// /}"

echo "    .env looks good (SNOWFLAKE_ACCOUNT=$SNOWFLAKE_ACCOUNT, AWS_REGION=$AWS_REGION)"

# ----------------------------------------------------------------------------
# 2. Python deps + hooks
# ----------------------------------------------------------------------------

step "2/9  Installing Python deps + pre-commit hooks"

uv sync --all-groups --quiet
uv run pre-commit install --install-hooks >/dev/null

# ----------------------------------------------------------------------------
# 3. Terraform init + apply
# ----------------------------------------------------------------------------

step "3/9  Provisioning AWS + Snowflake via Terraform (~5 min on first run — Glue + EMR + Snowflake)"

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

step "4/9  Capturing terraform outputs into .env"

OUTPUT_MARKER="# --- bootstrap.sh: terraform outputs ---"

# Self-heal: if the marker is already in .env from a previous bootstrap,
# strip it + everything below (those values may be stale after a
# `make infra-destroy + bootstrap` cycle — e.g. S3 bucket suffix,
# EMR app ID, and Snowflake passwords are all freshly regenerated).
# Then append the current outputs cleanly.
if grep -qF "$OUTPUT_MARKER" .env; then
    line=$(grep -nF "$OUTPUT_MARKER" .env | head -1 | cut -d: -f1)
    if [[ "$line" -gt 1 ]]; then
        head -n "$((line - 1))" .env > .env.tmp
    else
        : > .env.tmp
    fi
    mv .env.tmp .env
    # Trim trailing blank lines so we don't accumulate them across re-runs.
    awk 'NF{p=NR} {a[NR]=$0} END{for(i=1;i<=p;i++) print a[i]}' .env > .env.tmp \
        && mv .env.tmp .env
    echo "    .env had stale terraform outputs from a previous run — purged"
fi

cat <<EOF >> .env

$OUTPUT_MARKER
S3_BUCKET=$(terraform -chdir=infra output -raw s3_bucket)
EMR_APPLICATION_ID=$(terraform -chdir=infra output -raw emr_application_id)
EMR_EXEC_ROLE_ARN=$(terraform -chdir=infra output -raw emr_exec_role_arn)
GLUE_DATABASE=$(terraform -chdir=infra output -raw glue_database_name)
SNOWFLAKE_EXTERNAL_VOLUME=$(terraform -chdir=infra output -raw snowflake_external_volume)
SNOWFLAKE_CATALOG_INTEGRATION=$(terraform -chdir=infra output -raw snowflake_catalog_integration)
SNOWFLAKE_HISTORICAL_SCHEMA=$(terraform -chdir=infra output -raw snowflake_historical_schema)
SNOWFLAKE_LOADER_PASSWORD=$(terraform -chdir=infra output -raw snowflake_loader_password)
SNOWFLAKE_DBT_PASSWORD=$(terraform -chdir=infra output -raw snowflake_dbt_password)
SNOWFLAKE_ANALYST_PASSWORD=$(terraform -chdir=infra output -raw snowflake_analyst_password)
EOF
echo "    appended fresh terraform outputs to .env"

# Re-source so subsequent steps see the new values.
set -a; . ./.env; set +a

# ----------------------------------------------------------------------------
# 5. Render airflow_settings.yaml
# ----------------------------------------------------------------------------

step "5/9  Rendering airflow/airflow_settings.yaml + airflow/.env + dbt/profiles.yml"

S3_BUCKET=$(terraform -chdir=infra output -raw s3_bucket)
LOADER_PWD=$(terraform -chdir=infra output -raw snowflake_loader_password)
DBT_PWD=$(terraform -chdir=infra output -raw snowflake_dbt_password)
EMR_APP_ID=$(terraform -chdir=infra output -raw emr_application_id)
EMR_ROLE_ARN=$(terraform -chdir=infra output -raw emr_exec_role_arn)
GLUE_DB=$(terraform -chdir=infra output -raw glue_database_name)
SF_EXT_VOL=$(terraform -chdir=infra output -raw snowflake_external_volume)
SF_CAT_INT=$(terraform -chdir=infra output -raw snowflake_catalog_integration)

# Use a sed-incompatible delimiter (|) since some values contain slashes.
sed \
    -e "s|__SNOWFLAKE_ACCOUNT__|${SNOWFLAKE_ACCOUNT}|g" \
    -e "s|__SNOWFLAKE_LOADER_PASSWORD__|${LOADER_PWD}|g" \
    -e "s|__SNOWFLAKE_DBT_PASSWORD__|${DBT_PWD}|g" \
    -e "s|__S3_BUCKET__|${S3_BUCKET}|g" \
    -e "s|__EMR_APPLICATION_ID__|${EMR_APP_ID}|g" \
    -e "s|__EMR_EXEC_ROLE_ARN__|${EMR_ROLE_ARN}|g" \
    -e "s|__GLUE_DATABASE_NAME__|${GLUE_DB}|g" \
    -e "s|__SNOWFLAKE_EXTERNAL_VOLUME__|${SF_EXT_VOL}|g" \
    -e "s|__SNOWFLAKE_CATALOG_INTEGRATION__|${SF_CAT_INT}|g" \
    -e "s|__AWS_REGION__|${AWS_REGION}|g" \
    airflow/airflow_settings.yaml.example > airflow/airflow_settings.yaml

echo "    rendered airflow/airflow_settings.yaml from template"

# Render airflow/.env from .env.example, substituting the alert email and
# Gmail app password. Astro CLI auto-loads airflow/.env into the
# scheduler/worker/webserver containers, so AIRFLOW__SMTP__* + ALERT_EMAIL
# take effect at DAG parse time.
sed \
    -e "s|__ALERT_EMAIL__|${ALERT_EMAIL}|g" \
    -e "s|__GMAIL_APP_PASSWORD__|${GMAIL_APP_PASSWORD}|g" \
    airflow/.env.example > airflow/.env

echo "    rendered airflow/.env (Gmail SMTP for failure alerts)"

# Auto-create dbt/profiles.yml from the example if missing. Lets `make dbt`
# / direct dbt CLI calls work without a manual cp step. The file is
# gitignored (it can be customised per-machine) and the template is the
# canonical reference.
if [[ ! -f dbt/profiles.yml ]] && [[ -f dbt/profiles.yml.example ]]; then
    cp dbt/profiles.yml.example dbt/profiles.yml
    echo "    created dbt/profiles.yml from example"
fi

# ----------------------------------------------------------------------------
# 6. Deploy PySpark script to S3 (so EMR Serverless can fetch it)
# ----------------------------------------------------------------------------

step "6/9  Deploying spark/process_historical.py to S3"

# Idempotent: aws s3 cp overwrites by default. Re-run `make spark-deploy`
# after editing the script to push the new version without re-running the
# whole bootstrap. The spark_historical Airflow DAG submits jobs that
# point at s3://<bucket>/spark-scripts/process_historical.py.
aws s3 cp spark/process_historical.py "s3://${S3_BUCKET}/spark-scripts/process_historical.py" \
    --only-show-errors
echo "    deployed → s3://${S3_BUCKET}/spark-scripts/process_historical.py"

# ----------------------------------------------------------------------------
# 7. Astro: kill (wipes metadata) + start (re-imports settings)
# ----------------------------------------------------------------------------

step "7/9  Bringing up Airflow via Astro (kill + start to refresh settings)"

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
# 8. Wait for scheduler ready
# ----------------------------------------------------------------------------

step "8/9  Waiting for Airflow scheduler to be ready (~30s)"

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
# 9. Unpause dbt_pipeline (smoke test — latest scheduled run fires automatically)
# ----------------------------------------------------------------------------

step "9/9  Unpausing dbt_pipeline (latest scheduled @monthly run fires automatically)"

# Unpause instead of trigger. Why:
#   • catchup=False + @monthly + unpausing = exactly ONE scheduled DagRun
#     for the most recent cron tick. No backfill, no extra manual run.
#   • compute_target_month uses logical_date − lag_months=2, so that one
#     run targets the most recent month TLC has actually published.
#   • Triggering manually AND unpausing creates TWO runs (one manual, one
#     scheduled) — confusing for first-time setup. Unpause-only keeps it
#     to one.
#
# spark_historical stays paused — it's manual-trigger only and shouldn't
# fire a scheduled run on unpause (and it has no schedule anyway).

(cd airflow && astro dev run dags unpause dbt_pipeline)

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------

cat <<EOF

${BOLD}${GREEN}✓ bootstrap complete.${NC}

  Airflow UI :  http://localhost:8080  (admin / admin)

  DAGs:
    • dbt_pipeline     (UNPAUSED — Airflow's scheduler will fire one run
                        for the most recent @monthly tick; lag_months=2
                        targets the most recently published TLC month.)
    • spark_historical (PAUSED — manual-trigger only. Trigger from UI w/
                        year=YYYY conf to submit process_historical.py to
                        EMR Serverless.)

  Backfill (live-style months) is operator-driven via Airflow's UI
  Backfill or  make dbt-backfill START=2023-01 END=2023-12.

  Scale-time (1.5B rows / 14+ years): trigger spark_historical DAG once
  per year. The DAG submits to EMR Serverless and waits async (sensor in
  reschedule mode — no worker slot held).

  Failure alerts: emails go to the address in DAG DEFAULT_ARGS["email"]
  via Gmail SMTP (configured from .env's GMAIL_APP_PASSWORD).

  Once the dbt run completes, verify in Snowsight:
    USE ROLE ANALYST; USE WAREHOUSE WH_XS;

    -- Bronze (append-only, audit trail with batch IDs)
    SELECT _loaded_by, COUNT(*) FROM ANALYTICS.RAW.YELLOW_TRIPDATA GROUP BY 1;

    -- Silver (SCD Type 2)
    SELECT COUNT(*) FROM ANALYTICS.SNAPSHOTS.SNP_YELLOW_TRIPS WHERE dbt_valid_to IS NULL;

    -- Gold (live, dbt-built)
    SELECT COUNT(*) FROM ANALYTICS.MARTS.FCT_TRIPS;            -- ~3M for one month
    SELECT * FROM ANALYTICS.MARTS.AGG_HOURLY_DEMAND LIMIT 5;

    -- Gold (historical, Iceberg via Glue) — populated by spark_historical DAG.
    -- Empty until that DAG has been triggered for at least one year.
    SELECT COUNT(*) FROM ANALYTICS.HISTORICAL.HISTORICAL_DAILY_AGG;

  When you're done:
    cd airflow && astro dev stop
    cd ..                       # back to repo root — Makefile lives here
    make infra-destroy

EOF
