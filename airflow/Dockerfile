FROM quay.io/astronomer/astro-runtime:12.1.1

USER root

# dbt, the ingestion package, and build deps live in requirements.txt.
# We install the repo's ingestion module in editable mode so DAGs can `import ingestion`.
COPY --chown=astro:0 . /usr/local/airflow/

USER astro
