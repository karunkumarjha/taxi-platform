.PHONY: help setup fmt lint test hooks-install requirements status \
        infra-init infra-plan infra-apply infra-destroy \
        ingest load dbt dbt-backfill swap aws-all \
        spark-submit

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
	@echo "  load             COPY INTO from external S3 stage as LOADER role."
	@echo "                   Default: load every staged file. Override: make load MONTH=2023-01 (one file only)"
	@echo "  dbt              Self-healing dbt build + SWAP as DBT role (dev / smoke test)."
	@echo "                   No params — dbt detects what to rebuild via count-divergence."
	@echo "  dbt-backfill     Backfill dbt_pipeline DAG over a month range, sequential."
	@echo "                   Required: START=YYYY-MM END=YYYY-MM   Optional: FORCE=true"
	@echo "  aws-all          ingest + load + dbt in sequence"
	@echo ""
	@echo "  --- Spark / EMR Serverless (UI is the primary interface) ---"
	@echo "  spark-submit     Submit one EMR Serverless job ad-hoc (CLI args via ARGS=...)"
	@echo "                   Trigger spark_pipeline from the Airflow UI for whole-year"
	@echo "                   processing — pick year Param, mapped tasks fan out 12 months."
	@echo ""
	@echo "  --- Tests + hooks ---"
	@echo "  test             Run pyspark unit tests for the Spark staging logic"
	@echo "  hooks-install    Install pre-commit hooks (one-time per clone)"
	@echo "  requirements     Regenerate requirements.txt + requirements-dev.txt from pyproject.toml"
	@echo "  status           Show months/years dbt would rebuild on the next run (DBT role)"

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
	uv run ruff format ingestion spark airflow/dags scripts
	cd infra && terraform fmt -recursive

lint:
	uv run ruff check ingestion spark airflow/dags scripts

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
	$(WITH_ROLE) tf bash -c 'cd infra && terraform destroy'

# --- Pipeline ------------------------------------------------------------

ingest:
	uv run python -m ingestion.ingest_tlc --months $(MONTHS)

load:
	@if [ -n "$(MONTH)" ]; then \
	    echo "loading file for $(MONTH) only"; \
	    $(WITH_ROLE) loader uv run python -m ingestion.load_snowflake --month $(MONTH); \
	else \
	    echo "loading every staged file (Snowflake skips already-loaded ones)"; \
	    $(WITH_ROLE) loader uv run python -m ingestion.load_snowflake; \
	fi

# Self-healing dbt run — every model detects what to rebuild via count-divergence
# vs the current MARTS state. Mirrors the dbt_pipeline DAG's dbt_build group:
# deps → clone → load Spark-staged → seed → build → swap. No params needed.
dbt:
	@echo "dbt self-healing build (count-divergence detects months to rebuild)"
	cd dbt && cp -n profiles.yml.example profiles.yml 2>/dev/null || true
	$(WITH_ROLE) dbt uv run dbt deps  --project-dir dbt --profiles-dir dbt
	$(WITH_ROLE) dbt uv run dbt run-operation reset_marts_build_from_marts --project-dir dbt --profiles-dir dbt
	$(WITH_ROLE) dbt uv run dbt run-operation load_spark_staged_into_marts_build --project-dir dbt --profiles-dir dbt
	$(WITH_ROLE) dbt uv run dbt seed  --project-dir dbt --profiles-dir dbt
	$(WITH_ROLE) dbt uv run dbt build --project-dir dbt --profiles-dir dbt
	$(WITH_ROLE) dbt uv run dbt run-operation swap_marts --project-dir dbt --profiles-dir dbt

# Manual swap helper (e.g. emergency rollback by running it once standalone).
swap:
	$(WITH_ROLE) dbt uv run dbt run-operation swap_marts --project-dir dbt --profiles-dir dbt

# Backfill dbt_pipeline over a month range. Each DAG run = one month;
# max_active_runs=1 → sequential. The DAG infers lag_months=0 from
# run_type=backfill, so START/END are the actual data months processed.
#   make dbt-backfill START=2023-01 END=2023-12
#   make dbt-backfill START=2023-06 END=2023-08 FORCE=true
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
	if [ "$(FORCE)" = "true" ]; then \
	    CONF_ARG='--conf {"force": true}'; \
	else \
	    CONF_ARG=''; \
	fi; \
	echo "backfilling dbt_pipeline: $$DBT_START → $$DBT_END  (force=$(FORCE))"; \
	cd airflow && astro dev run dags backfill dbt_pipeline \
	    --start-date $${DBT_START}-01 --end-date $${DBT_END}-01 $$CONF_ARG

aws-all: ingest load dbt

# --- Spark ---------------------------------------------------------------

# One-off submission to EMR Serverless directly (bypasses Airflow). Useful
# when iterating on the Spark job locally; for normal historical processing
# trigger spark_pipeline from the Airflow UI (year Param + mapped tasks).
#   make spark-submit ARGS="--year 2023 --month 1"
spark-submit:
	uv run python -m spark.submit_emr $(ARGS)

# --- Tests + hooks -------------------------------------------------------

test:
	uv run python -m pytest spark/tests -v

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

# Show what dbt would rebuild on the next run. Runs the same count-divergence
# detection logic the models use, but as a read-only query so you can preview
# the work without actually building. Returns rows: (model, year, month, fct_count,
# agg_count) for any month where counts diverge.
status:
	$(WITH_ROLE) dbt uv run dbt run-operation show_pending_rebuilds --project-dir dbt --profiles-dir dbt
