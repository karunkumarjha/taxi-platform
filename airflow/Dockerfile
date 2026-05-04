# Astro Runtime 3.x ships Apache Airflow 3.x. Newer registry path
# (astrocrpublic.azurecr.io vs the legacy quay.io) and version format
# (3.0-14 = "3.0 line, patch 14" → bundles Airflow 3.0.6).
# Pinned to 3.0-14 (most-mature 3.0.x line) over 3.1.x / 3.2.x for
# stability — AIP-78 (UI backfill) is in 3.0+.
FROM astrocrpublic.azurecr.io/runtime:3.0-14

USER root

# dbt, the ingestion package, and build deps live in requirements.txt.
# We install the repo's ingestion module in editable mode so DAGs can `import ingestion`.
COPY --chown=astro:0 . /usr/local/airflow/

USER astro
