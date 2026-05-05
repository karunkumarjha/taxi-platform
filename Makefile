.PHONY: help setup fmt lint test hooks-install requirements status docs \
        infra-init infra-plan infra-apply infra-destroy \
        ingest load dbt dbt-backfill swap aws-all detect-drift \
        spark-historical spark-deploy

help:
	@echo "Common targets:"
	@echo "  setup            Install python deps via uv"
	@echo "  fmt              Format python + terraform"
	@echo "  lint             Lint python"
	@echo ""
	@echo "  --- Infra ---"
	@echo "  infra-init       terraform init"
	@echo "  infra-plan       terraform plan (uses TF service user)"
	@echo "  infra-apply      terraform apply"
	@echo "  infra-destroy    terraform destroy"
	@echo ""
	@echo "  --- Pipeline (manual / one-shot) ---"
	@echo "  ingest           Stream TLC parquet -> S3 raw/. Default: current year, only published months."
	@echo "                   Override: make ingest MONTHS=2023  or  MONTHS=2023-01,2023-02"
	@echo "  load             COPY INTO RAW.YELLOW_TRIPDATA as LOADER role."
	@echo "                   Default: load every staged file. Override: make load MONTH=2023-01 (one file only)"
	@echo "  dbt              Full dbt build + SWAP as DBT role (dev / smoke test)."
	@echo "                   Required: TARGET=YYYY-MM (the month to scope the merge to)."
	@echo "                   Steps: deps -> reset MARTS_BUILD -> snapshot -> seed -> staging ->"
	@echo "                          intermediate (merge on trip_bk) -> marts -> swap."
	@echo "  dbt-backfill     Backfill dbt_pipeline DAG over a month range, sequential."
	@echo "                   Required: START=YYYY-MM END=YYYY-MM"
	@echo "  aws-all          ingest + load + dbt in sequence for one month (TARGET=YYYY-MM)"
	@echo ""
	@echo "  --- DAG (Airflow UI is the primary interface) ---"
	@echo "  dbt_pipeline     Runs @monthly (live). Backfill via 'make dbt-backfill' or"
	@echo "                   'airflow dags backfill' — same DAG, operator-driven trigger."
	@echo ""
	@echo "  --- Scale-time (1.5B-row historical) ---"
	@echo "  spark-deploy     Push spark/process_historical.py to s3://<bucket>/spark-scripts/."
	@echo "                   Re-run after editing the script so EMR Serverless picks up changes."
	@echo "  spark-historical Run spark/process_historical.py locally (pyspark local[*])."
	@echo "                   Required: YEAR=YYYY  Optional: INPUT=path  OUTPUT=path"
	@echo "                   For production scale, trigger the spark_historical DAG instead —"
	@echo "                   it submits to EMR Serverless via boto3."
	@echo ""
	@echo "  --- Tests + hooks ---"
	@echo "  test             Run Airflow DAG-integrity tests (Astro pytest)"
	@echo "  hooks-install    Install pre-commit hooks (one-time per clone)"
	@echo "  requirements     Regenerate requirements.txt + requirements-dev.txt from pyproject.toml"
	@echo "  status           Show months/years where aggregates have drifted from FCT_TRIPS (DBT role)"
	@echo "  detect-drift     HEAD CloudFront ETag and compare vs RAW.SOURCE_FINGERPRINTS for one month."
	@echo "                   Prints: new | unchanged | drifted. Required: TARGET=YYYY-MM"
	@echo "  docs             Generate + serve dbt docs site at http://localhost:8081"
	@echo "                   (model lineage, test catalog, column descriptions)"

# All Snowflake-touching targets go through scripts/with_role.sh so the
# correct functional role is set per-command (LOADER / DBT / TF). The single
# .env file holds creds for every role; the wrapper picks the right one.
WITH_ROLE := ./scripts/with_role.sh

# Default month list for `make ingest` — current year. The ingest script's
# is_published_on_tlc HEAD-check skips months TLC hasn't released yet, so
# `make ingest` early in the year just downloads what's available.
# Override on command line, e.g.  make ingest MONTHS=2023  or  MONTHS=2023-01.
MONTHS ?= $(shell python3 -c "from datetime import date; print(date.today().year)")

setup:
	uv sync

fmt:
	uv run ruff format ingestion airflow/dags scripts spark
	cd infra && terraform fmt -recursive

lint:
	uv run ruff check ingestion airflow/dags scripts spark

# --- Infra ---------------------------------------------------------------

infra-init:
	cd infra && terraform init

# Terraform's snowflake provider reads SNOWFLAKE_USER / _PASSWORD / _ROLE
# from env. with_role.sh tf injects these from .env's SNOWFLAKE_TF_* set.
infra-plan:
	$(WITH_ROLE) tf bash -c 'cd infra && terraform plan'

infra-apply:
	$(WITH_ROLE) tf bash -c 'cd infra && terraform apply'

infra-destroy:
	@echo ">>> pre-destroy: dropping out-of-band Iceberg table as DBT role (if any)"
	-@$(WITH_ROLE) dbt uv run dbt run-operation drop_historical_iceberg \
	    --project-dir dbt --profiles-dir dbt 2>/dev/null \
	    || echo "    (skipped — DBT role / database / table not reachable)"
	@echo ">>> pre-destroy: clearing legacy cleanup resource from state (if present)"
	-@terraform -chdir=infra state rm 'snowflake_execute.historical_iceberg_table_cleanup' 2>/dev/null || true
	$(WITH_ROLE) tf bash -c 'cd infra && terraform destroy -auto-approve'

# --- Pipeline ------------------------------------------------------------

ingest:
	uv run python -m ingestion.ingest_tlc --months $(MONTHS)

load:
	@if [ -n "$(MONTH)" ]; then \
	    echo "loading file for $(MONTH) only"; \
	    $(WITH_ROLE) loader uv run python -m ingestion.load_snowflake --month $(MONTH); \
	else \
	    echo "loading every staged file"; \
	    $(WITH_ROLE) loader uv run python -m ingestion.load_snowflake; \
	fi

# Full dbt build mirroring the dbt_build TaskGroup in dbt_pipeline.py.
# Requires TARGET=YYYY-MM so the intermediate merge is scoped to one month
# (matches the var-based scoping in int_trips_enriched / int_trips_quarantined).
#
#   make dbt TARGET=2023-01
#
# Steps: deps -> reset MARTS_BUILD from MARTS -> dbt snapshot (Silver) ->
# seed -> staging -> intermediate (merge on trip_bk) -> marts -> swap.
dbt:
	@if [ -z "$(TARGET)" ]; then \
	    echo "ERROR: provide TARGET=YYYY-MM (the month to scope the merge to)"; \
	    exit 1; \
	fi
	@TARGET_YEAR=$$(echo $(TARGET) | cut -c1-4); \
	TARGET_MONTH=$$(echo $(TARGET) | cut -c6-7 | sed 's/^0//'); \
	echo "dbt build for $(TARGET) (target_year=$$TARGET_YEAR, target_month=$$TARGET_MONTH)"; \
	cd dbt && cp -n profiles.yml.example profiles.yml 2>/dev/null || true; \
	cd ..; \
	$(WITH_ROLE) dbt uv run dbt deps  --project-dir dbt --profiles-dir dbt; \
	$(WITH_ROLE) dbt uv run dbt run-operation reset_marts_build_from_marts --project-dir dbt --profiles-dir dbt; \
	$(WITH_ROLE) dbt uv run dbt snapshot --project-dir dbt --profiles-dir dbt; \
	$(WITH_ROLE) dbt uv run dbt seed  --project-dir dbt --profiles-dir dbt; \
	$(WITH_ROLE) dbt uv run dbt build --project-dir dbt --profiles-dir dbt \
	    --vars "{target_year: $$TARGET_YEAR, target_month: $$TARGET_MONTH}"; \
	$(WITH_ROLE) dbt uv run dbt run-operation swap_marts --project-dir dbt --profiles-dir dbt

# Manual swap helper (e.g. emergency rollback by running it once standalone).
swap:
	$(WITH_ROLE) dbt uv run dbt run-operation swap_marts --project-dir dbt --profiles-dir dbt

# Backfill dbt_pipeline over a month range. Each DAG run = one month;
# max_active_runs=1 → sequential. The DAG infers lag_months=0 from
# run_type=backfill, so START/END are the actual data months processed.
#   make dbt-backfill START=2023-01 END=2023-12
dbt-backfill:
	@DBT_START="$(START)"; DBT_END="$(END)"; \
	if [ -z "$$DBT_START" ] || [ -z "$$DBT_END" ]; then \
	    echo "ERROR: provide both START=YYYY-MM and END=YYYY-MM"; \
	    exit 1; \
	fi; \
	if [ "$$DBT_START" \> "$$DBT_END" ]; then \
	    echo "nothing to do: START=$$DBT_START is after END=$$DBT_END"; \
	    exit 0; \
	fi; \
	echo "backfilling dbt_pipeline: $$DBT_START → $$DBT_END"; \
	cd airflow && astro dev run dags backfill dbt_pipeline \
	    --start-date $${DBT_START}-01 --end-date $${DBT_END}-01

# Ad-hoc drift detection for a single month — same logic the
# detect_source_drift task runs at the head of every DagRun, exposed for
# operators who want to check whether TLC has republished a file outside
# the normal cadence. Reads RAW.SOURCE_FINGERPRINTS via the LOADER role.
#   make detect-drift TARGET=2023-03
detect-drift:
	@if [ -z "$(TARGET)" ]; then \
	    echo "ERROR: provide TARGET=YYYY-MM"; \
	    exit 1; \
	fi
	@TARGET_YEAR=$$(echo $(TARGET) | cut -c1-4); \
	TARGET_MONTH=$$(echo $(TARGET) | cut -c6-7 | sed 's/^0//'); \
	$(WITH_ROLE) loader uv run python -m ingestion.detect_drift \
	    --year $$TARGET_YEAR --month $$TARGET_MONTH

# One-shot end-to-end for a single month (TARGET=YYYY-MM): ingest TLC, COPY
# into RAW, run the full dbt build. Mirrors what one dbt_pipeline DAG run does.
aws-all:
	@if [ -z "$(TARGET)" ]; then \
	    echo "ERROR: provide TARGET=YYYY-MM"; \
	    exit 1; \
	fi
	$(MAKE) ingest MONTHS=$(TARGET)
	$(MAKE) load   MONTH=$(TARGET)
	$(MAKE) dbt    TARGET=$(TARGET)

# --- Tests + hooks -------------------------------------------------------

# Airflow DAG-integrity tests — runs inside the Astro container so Airflow's
# DagBag loader sees the same env the scheduler does. Verifies every DAG
# imports cleanly and has the expected task topology.
test:
	cd airflow && astro dev pytest tests/dags/

# Install the pre-commit hooks defined in .pre-commit-config.yaml.
# Run once per fresh clone. Hooks fire on every `git commit`.
hooks-install:
	uv run pre-commit install

# Regenerate requirements*.txt from pyproject.toml + uv.lock. The repo's
# canonical dep source-of-truth is pyproject.toml; the txt files are
# auto-generated mirrors for tools/reviewers that expect "standard" pip
# requirements. CI runs this and fails if the committed file would differ.
requirements:
	uv export --no-hashes --no-dev   > requirements.txt
	uv export --no-hashes --only-dev > requirements-dev.txt

# Audit query: shows months/years where aggregate row counts (sum of
# trip_count) have drifted from FCT_TRIPS row counts. Useful for spotting
# unbuilt aggregates after a partial dbt run. Empty output = marts are in sync.
status:
	$(WITH_ROLE) dbt uv run dbt run-operation show_pending_rebuilds --project-dir dbt --profiles-dir dbt

# Generate + serve the dbt documentation site locally. Renders model lineage,
# test catalog, column descriptions, and source freshness — everything dbt
# knows about the project as a browsable HTML app.
#
# Why local-only (not GitHub Pages auto-deploy): for a personal project, a
# hosted docs site is a write-only artefact (rarely read after first deploy)
# and adds operational surface (Snowflake creds in GitHub secrets, CI workflow
# to maintain). The on-demand local site delivers the same signal at 1/5th
# the cost. Production teams typically push these to internal Snowsight or
# a hosted dbt Cloud — that's the real upgrade path.
#
# Port 8081 to avoid clashing with Airflow's UI on 8080. Requires
# `make infra-apply` to have run (dbt introspects Snowflake's catalog).
docs:
	$(WITH_ROLE) dbt uv run dbt docs generate --project-dir dbt --profiles-dir dbt \
	    --vars '{target_year: 2023, target_month: 1}'
	@echo ""
	@echo "  → opening dbt docs at http://localhost:8081 (Ctrl-C to stop)"
	@echo ""
	$(WITH_ROLE) dbt uv run dbt docs serve   --project-dir dbt --profiles-dir dbt --port 8081

# --- Spark scale-time --------------------------------------------------------

# Push the script to S3 so EMR Serverless can fetch it. The spark_historical
# DAG submits jobs that point at s3://<bucket>/spark-scripts/process_historical.py.
# Bootstrap runs this once on initial setup; re-run after editing the script.
spark-deploy:
	@if [ -z "$$S3_BUCKET" ] && [ ! -f .env ]; then \
	    echo "ERROR: S3_BUCKET not set and no .env present"; \
	    echo "       run ./scripts/bootstrap.sh first, or  source .env"; \
	    exit 1; \
	fi
	@set -a; . ./.env 2>/dev/null || true; set +a; \
	aws s3 cp spark/process_historical.py "s3://$$S3_BUCKET/spark-scripts/process_historical.py"

# Run spark/process_historical.py locally via pyspark's local[*] master.
# For production scale (1.5B rows), submit to EMR Serverless instead — see
# the script's docstring for the full `aws emr-serverless start-job-run`
# command. This local target is for dev/correctness testing on a year or two
# of TLC data; it'll happily process 38M rows on a laptop in ~5 minutes.
#
# Examples:
#   make spark-historical YEAR=2023
#   make spark-historical YEAR=2023 INPUT=s3://nyc-tlc/trip\ data OUTPUT=s3://my-bucket/historical-daily
SPARK_INPUT  ?= ./data/raw
SPARK_OUTPUT ?= ./data/historical-daily
spark-historical:
	@if [ -z "$(YEAR)" ]; then \
	    echo "ERROR: provide YEAR=YYYY"; \
	    exit 1; \
	fi
	uv run python spark/process_historical.py \
	    --input  "$(SPARK_INPUT)" \
	    --output "$(SPARK_OUTPUT)" \
	    --year   $(YEAR)
