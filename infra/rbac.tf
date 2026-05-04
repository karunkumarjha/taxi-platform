# ============================================================================
# Snowflake RBAC — three functional roles + three matching users.
#
# Roles:
#   LOADER     — only role that writes RAW.YELLOW_TRIPDATA via COPY INTO.
#   DBT        — owns MARTS_BUILD + MARTS schemas (OWNERSHIP needed because
#                ALTER SCHEMA ... SWAP WITH ... requires it on both schemas
#                being swapped). Also owns SNAPSHOTS (Silver SCD layer),
#                clones MARTS into MARTS_BUILD on each run, and reads RAW.
#   ANALYST    — read-only across the board for ad-hoc debugging and BI.
#                Any external BI tool (Tableau, Looker, Superset, etc.) plugs
#                in as this role.
#
# The Terraform admin user (manually created in Snowflake, outside Terraform)
# keeps ACCOUNTADMIN — it's the role this provider authenticates as.
# ============================================================================

# ---- Random passwords -------------------------------------------------------

resource "random_password" "loader" {
  length  = 32
  special = false
}

resource "random_password" "dbt" {
  length  = 32
  special = false
}

resource "random_password" "analyst" {
  length  = 32
  special = false
}

# ---- Roles ------------------------------------------------------------------

resource "snowflake_account_role" "loader" {
  name    = "LOADER"
  comment = "Writes RAW.YELLOW_TRIPDATA via COPY INTO from @S3_TLC_STAGE."
}

resource "snowflake_account_role" "dbt" {
  name    = "DBT"
  comment = "Owns MARTS + MARTS_BUILD + SNAPSHOTS; runs dbt builds, snapshot, and the blue-green SWAP."
}

resource "snowflake_account_role" "analyst" {
  name    = "ANALYST"
  comment = "Read-only across all schemas — ad-hoc analyst queries + external BI tools."
}

# Make every functional role manageable from ACCOUNTADMIN, so Terraform (and
# anyone with ACCOUNTADMIN) can drop+recreate them and assume them when
# needed.
resource "snowflake_grant_account_role" "loader_to_admin" {
  role_name        = snowflake_account_role.loader.name
  parent_role_name = "ACCOUNTADMIN"
}
resource "snowflake_grant_account_role" "dbt_to_admin" {
  role_name        = snowflake_account_role.dbt.name
  parent_role_name = "ACCOUNTADMIN"
}
resource "snowflake_grant_account_role" "analyst_to_admin" {
  role_name        = snowflake_account_role.analyst.name
  parent_role_name = "ACCOUNTADMIN"
}

# ---- Users ------------------------------------------------------------------

resource "snowflake_user" "loader" {
  name              = "LOADER"
  password          = random_password.loader.result
  default_role      = snowflake_account_role.loader.name
  default_warehouse = snowflake_warehouse.wh_xs.name
  default_namespace = "${snowflake_database.analytics.name}.${snowflake_schema.raw.name}"
  comment           = "Service user for the COPY INTO loader (ingestion/load_snowflake.py)."
}

resource "snowflake_user" "dbt" {
  name              = "DBT"
  password          = random_password.dbt.result
  default_role      = snowflake_account_role.dbt.name
  default_warehouse = snowflake_warehouse.wh_xs.name
  default_namespace = "${snowflake_database.analytics.name}.${snowflake_schema.marts_build.name}"
  comment           = "Service user for dbt builds + the blue-green SWAP."
}

resource "snowflake_user" "analyst" {
  name              = "ANALYST"
  password          = random_password.analyst.result
  default_role      = snowflake_account_role.analyst.name
  default_warehouse = snowflake_warehouse.wh_xs.name
  default_namespace = snowflake_database.analytics.name
  comment           = "Read-only user for ad-hoc analyst queries + BI tools."
}

# ---- Role-to-user assignments ----------------------------------------------

resource "snowflake_grant_account_role" "loader_to_user" {
  role_name = snowflake_account_role.loader.name
  user_name = snowflake_user.loader.name
}
resource "snowflake_grant_account_role" "dbt_to_user" {
  role_name = snowflake_account_role.dbt.name
  user_name = snowflake_user.dbt.name
}
resource "snowflake_grant_account_role" "analyst_to_user" {
  role_name = snowflake_account_role.analyst.name
  user_name = snowflake_user.analyst.name
}

# ---- Locals: fully-qualified names (DRY) -----------------------------------

locals {
  db                = snowflake_database.analytics.name
  raw_schema        = "${local.db}.${snowflake_schema.raw.name}"
  build_schema      = "${local.db}.${snowflake_schema.marts_build.name}"
  marts_schema      = "${local.db}.${snowflake_schema.marts.name}"
  snapshots_schema  = "${local.db}.${snowflake_schema.snapshots.name}"
  historical_schema = "${local.db}.${snowflake_schema.historical.name}"

  all_functional_roles = [
    snowflake_account_role.loader.name,
    snowflake_account_role.dbt.name,
    snowflake_account_role.analyst.name,
  ]
}

# ---- Warehouse USAGE (everyone needs to compute) ---------------------------

resource "snowflake_grant_privileges_to_account_role" "wh_usage" {
  for_each          = toset(local.all_functional_roles)
  account_role_name = each.key
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.wh_xs.name
  }
}

# ---- Database USAGE (everyone needs to traverse the DB to reach schemas) ----

resource "snowflake_grant_privileges_to_account_role" "db_usage" {
  for_each          = toset(local.all_functional_roles)
  account_role_name = each.key
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.analytics.name
  }
}

# ============================================================================
# Schema + object grants
# ============================================================================

# ---- RAW schema ------------------------------------------------------------
# LOADER: USAGE + CREATE TABLE (load_snowflake.py uses CREATE TABLE IF NOT EXISTS).
# DBT:    USAGE only (reads from RAW.YELLOW_TRIPDATA via the source).
# ANALYST: USAGE only.

resource "snowflake_grant_privileges_to_account_role" "raw_loader_schema" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE"]
  on_schema {
    schema_name = local.raw_schema
  }
}

resource "snowflake_grant_privileges_to_account_role" "raw_dbt_schema" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = local.raw_schema
  }
}

resource "snowflake_grant_privileges_to_account_role" "raw_analyst_schema" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = local.raw_schema
  }
}

# Stage + file format USAGE for LOADER (needed for COPY INTO @stage).
# DBT also needs file format USAGE for source freshness checks.
resource "snowflake_grant_privileges_to_account_role" "stage_loader" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "${local.raw_schema}.${snowflake_stage.s3_tlc_stage.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "file_format_loader" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "FILE FORMAT"
    object_name = "${local.raw_schema}.${snowflake_file_format.parquet_ff.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "file_format_dbt" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "FILE FORMAT"
    object_name = "${local.raw_schema}.${snowflake_file_format.parquet_ff.name}"
  }
}

# RAW table-level grants:
#   LOADER  — INSERT (write data) + SELECT (the COPY INTO returns rows)
#   DBT     — SELECT (read source for staging models)
#   ANALYST — SELECT (debugging)
# Granted on FUTURE TABLES so they apply when the loader's CREATE TABLE
# runs for the first time, plus on ALL existing tables for re-applies.
resource "snowflake_grant_privileges_to_account_role" "raw_tables_loader_future" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["INSERT", "SELECT", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.raw_schema
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "raw_tables_loader_existing" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["INSERT", "SELECT", "DELETE", "TRUNCATE"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = local.raw_schema
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "raw_tables_dbt_future" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.raw_schema
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "raw_tables_dbt_existing" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = local.raw_schema
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "raw_tables_analyst_future" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.raw_schema
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "raw_tables_analyst_existing" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = local.raw_schema
    }
  }
}

# Note: there is no separate STAGING schema. Staging views, atomic facts/dims,
# and aggregate facts ALL live in MARTS_BUILD during build, and ALL move to
# MARTS in a single atomic SWAP. See dbt/dbt_project.yml.

# ---- MARTS_BUILD schema ----------------------------------------------------
# DBT owns it. ANALYST gets FUTURE-TABLE grants so the SWAP carries the
# SELECT permission with the moving tables.

resource "snowflake_grant_ownership" "marts_build_to_dbt" {
  account_role_name = snowflake_account_role.dbt.name
  on {
    object_type = "SCHEMA"
    object_name = local.build_schema
  }
  outbound_privileges = "COPY"
}

resource "snowflake_grant_privileges_to_account_role" "build_analyst_schema" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = local.build_schema
  }
  depends_on = [snowflake_grant_ownership.marts_build_to_dbt]
}

resource "snowflake_grant_privileges_to_account_role" "build_analyst_tables_future" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.build_schema
    }
  }
  depends_on = [snowflake_grant_ownership.marts_build_to_dbt]
}

# ---- MARTS schema ----------------------------------------------------------
# DBT owns it (SWAP requires OWNERSHIP on both swapped schemas).
# ANALYST reads everything (and is the role that BI tools authenticate as).

resource "snowflake_grant_ownership" "marts_to_dbt" {
  account_role_name = snowflake_account_role.dbt.name
  on {
    object_type = "SCHEMA"
    object_name = local.marts_schema
  }
  outbound_privileges = "COPY"
}

resource "snowflake_grant_privileges_to_account_role" "marts_analyst_schema" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = local.marts_schema
  }
  depends_on = [snowflake_grant_ownership.marts_to_dbt]
}

resource "snowflake_grant_privileges_to_account_role" "marts_analyst_tables_future" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.marts_schema
    }
  }
  depends_on = [snowflake_grant_ownership.marts_to_dbt]
}

# ---- SNAPSHOTS schema ------------------------------------------------------
# DBT owns it and writes the SCD snapshot table directly (not via blue-green
# swap). ANALYST gets SELECT on future tables for audit queries.

resource "snowflake_grant_ownership" "snapshots_to_dbt" {
  account_role_name = snowflake_account_role.dbt.name
  on {
    object_type = "SCHEMA"
    object_name = local.snapshots_schema
  }
  outbound_privileges = "COPY"
}

resource "snowflake_grant_privileges_to_account_role" "snapshots_dbt_schema" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE", "CREATE TABLE"]
  on_schema {
    schema_name = local.snapshots_schema
  }
  depends_on = [snowflake_grant_ownership.snapshots_to_dbt]
}

resource "snowflake_grant_privileges_to_account_role" "snapshots_dbt_tables_future" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["INSERT", "UPDATE", "SELECT", "DELETE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.snapshots_schema
    }
  }
  depends_on = [snowflake_grant_ownership.snapshots_to_dbt]
}

resource "snowflake_grant_privileges_to_account_role" "snapshots_analyst_schema" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = local.snapshots_schema
  }
  depends_on = [snowflake_grant_ownership.snapshots_to_dbt]
}

resource "snowflake_grant_privileges_to_account_role" "snapshots_analyst_tables_future" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = local.snapshots_schema
    }
  }
  depends_on = [snowflake_grant_ownership.snapshots_to_dbt]
}

# ---- HISTORICAL schema (Iceberg) -------------------------------------------
# DBT owns it (CREATE ICEBERG TABLE runs as DBT in spark_historical DAG).
# DBT also needs USAGE on the EXTERNAL VOLUME and CATALOG INTEGRATION at the
# account level to bind the Iceberg table to them. ANALYST gets SELECT.

resource "snowflake_grant_ownership" "historical_to_dbt" {
  account_role_name = snowflake_account_role.dbt.name
  on {
    object_type = "SCHEMA"
    object_name = local.historical_schema
  }
  outbound_privileges = "COPY"
}

resource "snowflake_grant_privileges_to_account_role" "historical_dbt_schema" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE", "CREATE ICEBERG TABLE"]
  on_schema {
    schema_name = local.historical_schema
  }
  depends_on = [snowflake_grant_ownership.historical_to_dbt]
}

# Account-level USAGE on the EXTERNAL VOLUME — required for any role that
# binds an Iceberg table to it (CREATE ICEBERG TABLE ... EXTERNAL_VOLUME = ...).
resource "snowflake_grant_privileges_to_account_role" "external_volume_usage_dbt" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "EXTERNAL VOLUME"
    object_name = snowflake_external_volume.historical.name
  }
}

# Account-level USAGE on the CATALOG INTEGRATION — same reason.
resource "snowflake_grant_privileges_to_account_role" "catalog_integration_usage_dbt" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "INTEGRATION"
    object_name = local.glue_catalog_integration_name
  }
  # The integration itself is created via snowflake_execute, so we depend on
  # that resource explicitly to ensure ordering.
  depends_on = [snowflake_execute.glue_catalog_integration]
}

resource "snowflake_grant_privileges_to_account_role" "historical_analyst_schema" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = local.historical_schema
  }
  depends_on = [snowflake_grant_ownership.historical_to_dbt]
}

# Iceberg tables are ICEBERG TABLES in Snowflake's grant grammar, but
# `SELECT` on `future ICEBERG TABLES` is the right grant. Plural object
# type is ICEBERG TABLES.
resource "snowflake_grant_privileges_to_account_role" "historical_analyst_iceberg_future" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "ICEBERG TABLES"
      in_schema          = local.historical_schema
    }
  }
  depends_on = [snowflake_grant_ownership.historical_to_dbt]
}
