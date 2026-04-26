.PHONY: help setup fmt lint \
        infra-init infra-plan infra-apply infra-destroy \
        ingest load dbt swap aws-all \
        spark-submit spark-backfill \
        streamlit-deploy

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
	@echo "  load             COPY INTO from external S3 stage as LOADER role"
	@echo "  dbt              dbt deps + seed + build + tests + blue-green swap as DBT role"
	@echo "  aws-all          ingest + load + dbt + streamlit-deploy in sequence"
	@echo ""
	@echo "  --- Spark / EMR Serverless ---"
	@echo "  spark-submit     Submit one EMR Serverless job ad-hoc (CLI args via ARGS=...)"
	@echo "  spark-backfill   Backfill spark_pipeline DAG. Defaults: current year up to (today - 2 months)."
	@echo "                   Override: make spark-backfill START=YYYY-MM END=YYYY-MM [FORCE=true]"
	@echo ""
	@echo "  --- Streamlit ---"
	@echo "  streamlit-deploy PUT streamlit/ files to the Snowflake stage"

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
	$(WITH_ROLE) loader uv run python -m ingestion.load_snowflake

dbt:
	cd dbt && cp -n profiles.yml.example profiles.yml 2>/dev/null || true
	$(WITH_ROLE) dbt uv run dbt deps  --project-dir dbt --profiles-dir dbt
	$(WITH_ROLE) dbt uv run dbt seed  --project-dir dbt --profiles-dir dbt
	$(WITH_ROLE) dbt uv run dbt build --project-dir dbt --profiles-dir dbt
	$(WITH_ROLE) dbt uv run dbt run-operation swap_marts --project-dir dbt --profiles-dir dbt

# Manual swap helper (e.g. emergency rollback by running it once standalone).
swap:
	$(WITH_ROLE) dbt uv run dbt run-operation swap_marts --project-dir dbt --profiles-dir dbt

aws-all: ingest load dbt streamlit-deploy

# --- Spark ---------------------------------------------------------------

# One-off submission. Pass args via ARGS, e.g.:
#   make spark-submit ARGS="--year 2023 --month 1"
spark-submit:
	uv run python -m spark.submit_emr $(ARGS)

# Backfill the spark_pipeline DAG over a month range, sequentially.
# Backfill always passes lag_months=0 so START / END are the actual DATA
# months processed (not subject to the 2-month publishing lag default).
#
# Defaults: START = January of the current year. END = (today - 2 months),
# matching TLC's typical publishing cadence. So a bare `make spark-backfill`
# processes "current year, everything published" — symmetric with ingest.
#   make spark-backfill                              # current year, latest published
#   make spark-backfill START=2020-01 END=2023-12    # explicit range
#   make spark-backfill START=2020-01 END=2023-12 FORCE=true   # re-process all
spark-backfill:
	@SPARK_START="$(START)"; SPARK_END="$(END)"; \
	if [ -z "$$SPARK_START" ] || [ -z "$$SPARK_END" ]; then \
	    DEFAULTS=$$(python3 -c "from datetime import date; t=date.today(); ye=t.year-(1 if t.month<=2 else 0); me=(t.month-3)%12+1; print(f'{t.year}-01 {ye}-{me:02d}')"); \
	    [ -z "$$SPARK_START" ] && SPARK_START=$$(echo $$DEFAULTS | cut -d' ' -f1); \
	    [ -z "$$SPARK_END" ]   && SPARK_END=$$(echo $$DEFAULTS | cut -d' ' -f2); \
	    echo "[defaults applied — override with START=YYYY-MM END=YYYY-MM]"; \
	fi; \
	if [ "$$SPARK_START" \> "$$SPARK_END" ]; then \
	    echo "nothing to do: START=$$SPARK_START is after END=$$SPARK_END (TLC may not have published any months of the current year yet)"; \
	    exit 0; \
	fi; \
	if [ "$(FORCE)" = "true" ]; then \
	    CONF='{"lag_months": 0, "force": true}'; \
	else \
	    CONF='{"lag_months": 0}'; \
	fi; \
	echo "backfilling spark_pipeline: $$SPARK_START → $$SPARK_END  (force=$(FORCE))"; \
	cd airflow && astro dev run dags backfill spark_pipeline \
	    --start-date $${SPARK_START}-01 --end-date $${SPARK_END}-01 \
	    --conf "$$CONF"

# --- Streamlit -----------------------------------------------------------

streamlit-deploy:
	$(WITH_ROLE) tf uv run python scripts/deploy_streamlit.py
