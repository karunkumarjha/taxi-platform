# AWS Glue Data Catalog — shared metadata store for the Iceberg
# historical-daily table. Why Glue (vs Snowflake-managed Iceberg or
# HadoopCatalog):
#
# • Production-canonical pattern: Glue is the de-facto standard for
#   table metadata in AWS, supported by every modern engine
#   (Spark, Athena, Redshift Spectrum, Snowflake, Databricks, Trino).
# • True multi-engine interop: Spark writes the manifests to Glue;
#   Snowflake reads them via CATALOG INTEGRATION. Neither owns the
#   table — the catalog does.
# • Atomic Iceberg commits: Glue's UpdateTable API atomically replaces
#   the table's "current snapshot" pointer, so readers never see
#   partial writes (S3 + HadoopCatalog can't guarantee this without
#   external locking).
#
# The free tier covers 1M Glue requests/month, more than enough for
# this project's once-per-year-per-historical-year cadence.

resource "aws_glue_catalog_database" "taxi_iceberg" {
  name        = "taxi_iceberg"
  description = "Iceberg tables backing the historical-daily Spark output, read by Snowflake via CATALOG INTEGRATION"

  catalog_id = data.aws_caller_identity.current.account_id
}
