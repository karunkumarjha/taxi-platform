# NYC TLC Yellow Taxi — Data Platform

End-to-end data platform over the NYC TLC Yellow Taxi 2023 dataset (~38M rows
across 12 monthly Parquet files, scaling to ~1.5B rows when historical years
are included). Built for the Firmable Data Engineer (Platform) take-home.

The whole platform — AWS, Snowflake, IAM, RBAC, EMR Serverless, the Streamlit
analytics app — is provisioned by a single `terraform apply`. Two Airflow
DAGs (one daily for the warehouse path, one monthly for Spark) drive
ingestion, transformation, and the historical Spark roll-up.

---

## Architecture

```
                        TLC CloudFront (parquet, monthly drops)
                                       │
                  ingestion/ingest_tlc.py  (stream, smart-resume)
                                       │
                                       ▼
                  ┌──── S3:  analytics-data-<suffix>  ────┐
                  │   raw/                                │
                  │   analytics/daily_zone_aggregates/    │
                  │   spark-scripts/                      │
                  │   spark-logs/                         │
                  └──┬─────────────────────────────────┬──┘
                     │ Snowflake STORAGE INTEGRATION   │ EMR Serverless
                     │ (assume IAM role; no AWS keys)  │ (boto3 submit)
                     ▼                                 ▼
       Snowflake @ANALYTICS.RAW.S3_TLC_STAGE     spark/process_historical.py
                     │                                 │
       COPY INTO     │                  one Spark job  │
       (LOADER role) │                  per month      │
                     ▼                                 ▼
       ANALYTICS.RAW.YELLOW_TRIPDATA       analytics/year=YYYY/month=MM/
                     │                       (partitioned daily-grain parquet)
                     │
                     │  dbt (DBT role; blue-green via SWAP)
                     ▼
       ANALYTICS.MARTS_BUILD     ──ALTER SCHEMA SWAP──>     ANALYTICS.MARTS
       ┌──────────────────────────┐                         ┌──────────────────────────┐
       │ STG_YELLOW_TRIPS  (view) │                         │ DIM_ZONES                │
       │ DIM_ZONES                │                         │ FCT_TRIPS                │
       │ FCT_TRIPS                │                         │ FCT_TRIPS_QUARANTINED    │
       │ FCT_TRIPS_QUARANTINED    │                         │ AGG_ZONE_REVENUE_MONTHLY │
       │ AGG_*                    │                         │ AGG_HOURLY_DEMAND        │
       └──────────────────────────┘                         │ AGG_ZONE_SUPPLY_GAPS     │
                                                            │ AGG_ZONE_TIP_BEHAVIOUR   │
                                                            └─────────────┬────────────┘
                                                                          │
                                                          DASHBOARD role  ▼
                                                          STREAMLIT  ANALYTICS_APP
                                                          (Snowsight → Streamlit)

Orchestration                                Snowflake RBAC
─────────────                                ──────────────
Airflow DAGs (Astro CLI):                     LOADER     — write RAW only
  dbt_pipeline    @daily                      DBT        — own STAGING + MARTS_*; SELECT on RAW
  spark_pipeline  @monthly                    DASHBOARD  — read MARTS + Streamlit
                                              ANALYST    — read everything (no writes)
                                              (TF_USER   — ACCOUNTADMIN, Terraform only)
```

---

## Repo layout

```
.
├── infra/                     Terraform — AWS + Snowflake (incl. RBAC + Streamlit)
│   ├── main.tf                providers + locals (predicted IAM ARN trick)
│   ├── s3.tf                  bucket + versioning + lifecycle
│   ├── iam.tf                 Snowflake-S3 trust role
│   ├── snowflake.tf           database + 3 schemas + warehouse + integration + stage
│   ├── rbac.tf                4 roles + 4 users + grants matrix
│   ├── emr.tf                 EMR Serverless app + exec role
│   ├── streamlit.tf           Streamlit STAGE + STREAMLIT object + grants
│   ├── outputs.tf             29 outputs (bucket, EMR ids, passwords sensitive)
│   └── variables.tf
│
├── ingestion/                 TLC → S3 → Snowflake
│   ├── ingest_tlc.py          stream parquet to S3, smart-resume (HEAD per month)
│   └── load_snowflake.py      COPY INTO RAW.YELLOW_TRIPDATA (LOADER role)
│
├── dbt/
│   ├── dbt_project.yml        2-layer project (staging → intermediate → marts)
│   ├── profiles.yml.example   env-var-driven Snowflake config
│   ├── packages.yml           dbt_utils + dbt_expectations
│   ├── seeds/dim_zones.csv    265-row TLC zone dim (loaded directly as MARTS.DIM_ZONES)
│   ├── macros/
│   │   ├── generate_schema_name.sql      override so +schema produces literal names
│   │   ├── swap_marts.sql                blue-green SWAP MARTS_BUILD ↔ MARTS
│   │   └── test_trip_duration_sane.sql   custom generic test
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml               raw.yellow_tripdata + freshness
│   │   │   ├── stg_yellow_trips.sql      cast + flag (13 invalid_reasons)
│   │   │   └── stg_yellow_trips.yml      schema tests
│   │   ├── intermediate/
│   │   │   ├── int_trips_enriched.sql    atomic fact (alias=fct_trips)
│   │   │   ├── int_trips_quarantined.sql audit fact (alias=fct_trips_quarantined)
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── agg_zone_revenue_monthly.sql   serves Q1
│   │       ├── agg_hourly_demand.sql          serves Q2
│   │       ├── agg_zone_supply_gaps.sql       serves Q3 (incremental)
│   │       ├── agg_zone_tip_behaviour.sql     serves Q4
│   │       └── schema.yml
│   └── tests/
│       └── assert_monthly_row_counts_sane.sql   singular test
│
├── airflow/                   Astro CLI project
│   ├── dags/
│   │   ├── dbt_pipeline.py    daily: ingest → COPY → dbt build → swap
│   │   └── spark_pipeline.py  monthly: ingest one month → EMR → analytics/
│   ├── include/
│   │   └── taxi_helpers.py    shared months_up_to helper
│   ├── tests/dags/test_dag_integrity.py
│   ├── docker-compose.override.yml   mounts repo source + ~/.aws into containers
│   ├── Dockerfile
│   └── requirements.txt
│
├── spark/
│   ├── process_historical.py  one-month Spark job (run on EMR Serverless)
│   └── submit_emr.py          boto3 helper: PUT artifacts + start_job_run + poll
│
├── streamlit/                 Streamlit-in-Snowflake (replaces deprecated Snowsight Dashboards)
│   ├── app.py                 4 sections, one per business question
│   └── environment.yml        conda env (snowflake channel only)
│
├── queries/                   Three SQL queries answering Q1, Q2, Q4
│   ├── 01_zone_revenue.sql
│   ├── 02_hourly_demand.sql
│   └── 03_tip_behaviour.sql
│
├── scripts/
│   ├── with_role.sh           wraps any command with the right Snowflake role's env vars
│   └── deploy_streamlit.py    PUT app.py + environment.yml to the Streamlit stage
│
├── Makefile                   one-stop targets (`make help`)
├── pyproject.toml             uv-managed deps
├── requirements.txt           runtime deps (uv export)
├── requirements-dev.txt       dev deps (uv export)
├── .env.example               template — copy to .env and fill
└── .gitignore
```

---

## Prerequisites

- **AWS account** with `aws sts get-caller-identity` working. IAM permissions
  to create S3 buckets, IAM roles, EMR Serverless apps.
- **Terraform ≥ 1.6** (`terraform -v`).
- **Snowflake trial** (Standard edition is enough). Sign up at
  [signup.snowflake.com](https://signup.snowflake.com/).
- **Docker Desktop** running (for Astro CLI).
- **Astro CLI** (`brew install astro`).
- **uv** (`brew install uv` or `pipx install uv`) — Python deps.
- **Python 3.11+**.

---

## Setup (one-time)

### 1. Snowflake — create the Terraform service user

Snowflake itself can't be created from scratch by Terraform — you need an
account first, plus a service user for Terraform to log in as.

Snowflake trial accounts come with `COMPUTE_WH` (X-Small) pre-created — we
re-use it as the bootstrap warehouse for Terraform's session, while the
project-specific `WH_XS` is created by Terraform.

In Snowsight as `ACCOUNTADMIN`:

```sql
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS TF_USER
    PASSWORD             = '<choose-something-strong>'
    DEFAULT_ROLE         = ACCOUNTADMIN
    DEFAULT_WAREHOUSE    = COMPUTE_WH
    MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE ACCOUNTADMIN TO USER TF_USER;
```

Find your **account locator** at Snowsight bottom-left (looks like
`ABCD-XY12345`).

> **Older / non-standard trial?** If `SHOW WAREHOUSES LIKE 'COMPUTE_WH';`
> returns no rows, create it once before continuing:
> `CREATE WAREHOUSE COMPUTE_WH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;`

### 2. Repo + env

```bash
git clone <this-repo> taxi-platform
cd taxi-platform

uv sync

cp .env.example .env
# Edit .env — fill in:
#   SNOWFLAKE_ACCOUNT       (your locator)
#   SNOWFLAKE_TF_USER       (TF_USER)
#   SNOWFLAKE_TF_PASSWORD   (the password you set above)
#   AWS_REGION              (your region; default us-east-1)
#   AWS_PROFILE             (your local AWS profile if not 'default')
```

### 3. Provision everything via Terraform

```bash
make infra-init           # one-time: download providers
make infra-apply          # creates ~75 resources, ~3 min
```

Pull the dynamic outputs back into `.env`:

```bash
cat <<EOF >> .env

# --- appended after make infra-apply ---
S3_BUCKET=$(terraform -chdir=infra output -raw s3_bucket)
EMR_APPLICATION_ID=$(terraform -chdir=infra output -raw emr_application_id)
EMR_EXEC_ROLE_ARN=$(terraform -chdir=infra output -raw emr_exec_role_arn)
SNOWFLAKE_LOADER_PASSWORD=$(terraform -chdir=infra output -raw snowflake_loader_password)
SNOWFLAKE_DBT_PASSWORD=$(terraform -chdir=infra output -raw snowflake_dbt_password)
SNOWFLAKE_DASHBOARD_PASSWORD=$(terraform -chdir=infra output -raw snowflake_dashboard_password)
SNOWFLAKE_ANALYST_PASSWORD=$(terraform -chdir=infra output -raw snowflake_analyst_password)
EOF
```

### 4. Smoke test the warehouse path (no Airflow yet)

```bash
make ingest MONTHS=2023-01    # ~30s, parquet → S3
make load                       # ~90s, COPY INTO RAW (LOADER role)
make dbt                        # ~3 min, dbt build + tests + atomic SWAP
```

Verify in Snowsight as `ANALYST`:

```sql
USE ROLE ANALYST; USE WAREHOUSE WH_XS;

SELECT 'raw'         t, COUNT(*) FROM ANALYTICS.RAW.YELLOW_TRIPDATA
UNION ALL
SELECT 'enriched',     COUNT(*) FROM ANALYTICS.MARTS.FCT_TRIPS
UNION ALL
SELECT 'quarantined',  COUNT(*) FROM ANALYTICS.MARTS.FCT_TRIPS_QUARANTINED
UNION ALL
SELECT 'top zone monthly mart', COUNT(*) FROM ANALYTICS.MARTS.AGG_ZONE_REVENUE_MONTHLY;
```

### 5. Deploy + open the Streamlit app

```bash
make streamlit-deploy
```

Open Snowsight → **Projects → Streamlit** → `ANALYTICS_APP`. First load is
30-60s while the conda env builds. The app has 4 sections answering Q1-Q4.

### 6. Bring up Airflow

```bash
cd airflow
cp airflow_settings.yaml.example airflow_settings.yaml
# Fill in the 6 placeholders (account locator + 2 passwords + bucket + 2 EMR ids)
astro dev start
```

Open `http://localhost:8080`, login `admin / admin`. You'll see two DAGs:

- **`dbt_pipeline`** (daily): ingest → COPY → dbt build → swap.
- **`spark_pipeline`** (monthly): one DAG run per month, processes
  `logical_date.month`'s data through EMR Serverless.

### 7. Run a Spark backfill (optional)

```bash
# Defaults: current year, January through (today − 2 months)
make spark-backfill

# Explicit range
make spark-backfill START=2023-01 END=2023-12

# Force-rebuild even months already in analytics/
make spark-backfill START=2023-01 END=2023-12 FORCE=true
```

See **Backfill cookbook** below for more patterns + how the warehouse path
backfills.

### 8. Teardown when done

```bash
cd airflow && astro dev stop
make infra-destroy
```

`force_destroy = true` on the S3 bucket lets `destroy` wipe staged parquet
without manual cleanup. The TF_USER (created manually) and the trial's
default COMPUTE_WH survive — they're outside Terraform's state.

---

## Architecture decisions

### Single S3 bucket, prefix-organised

`analytics-data-<6-char-suffix>` with `raw/`, `analytics/`, `spark-scripts/`,
`spark-logs/` inside. One bucket = one IAM policy, one lifecycle config, one
region. Random suffix satisfies S3's global-uniqueness rule without baking
account / region into the name.

### Raw parquet lives only in S3

No `data/raw/` directory anywhere in the repo. Ingestion streams
`requests.get(stream=True) → boto3.upload_fileobj`, chunked multipart, no
disk roundtrip. S3 is the canonical source of truth — eliminating "is the
laptop's copy current?" ambiguity.

### Single-apply Terraform across AWS + Snowflake

The classic Snowflake-storage-integration ↔ IAM-role circular dependency
(integration needs role ARN; role needs integration's external ID) is
broken by **predicting the IAM role ARN** and giving it to the integration
as a string at create time. Snowflake validates the trust policy only at
first stage USAGE, not at integration creation. Net: one `terraform apply`,
no targeted-apply two-phase dance.

### dbt — three logical layers, one Snowflake schema

```
staging       (views)        →  cast, rename, flag invalid (no rows dropped)
intermediate  (tables, +alias) → atomic facts: FCT_TRIPS, FCT_TRIPS_QUARANTINED
marts         (tables)       →  aggregate facts (AGG_*) for the business questions
```

Files are organised by layer; all three layers materialise into a single
`MARTS_BUILD` schema in Snowflake. The intermediate layer uses dbt's
`+alias` to expose its tables as `FCT_*` in `MARTS` (analyst-friendly
naming) while keeping `int_*` filenames (dbt convention).

Why one schema for all dbt-managed objects? It lets the **blue-green
swap** (next section) move the entire dbt-built world atomically.

### Blue-green mart deployment

`MARTS_BUILD` is the build schema; `MARTS` is the production schema
(dashboards + Streamlit read here). After a successful `dbt build`, a macro
runs:

```sql
ALTER SCHEMA ANALYTICS.MARTS_BUILD SWAP WITH ANALYTICS.MARTS;
```

This is atomic in Snowflake (metadata-only). What was being built becomes
production instantly; what was production becomes the next build's
starting point. **If any test in the marts layer fails, the swap never
runs** — Airflow's default `all_success` trigger rule sees the failed
test and skips the swap task, leaving `MARTS` on the previous good build.

This directly answers the Airflow brainstormer question (see below) and
the project's data-quality story isn't dependent on rolling back a
half-completed build — it's structurally impossible for a half-build to
reach production.

### RBAC — least privilege, four functional roles

| Role | Can do |
|---|---|
| `LOADER` | INSERT/SELECT/DELETE/TRUNCATE on `RAW.YELLOW_TRIPDATA` only. USAGE on the external stage. **Cannot read MARTS.** |
| `DBT` | SELECT on RAW; OWNERSHIP on `STAGING`-conceptual + `MARTS_BUILD` + `MARTS` (needed for SWAP). **Cannot write to RAW.** |
| `DASHBOARD` | SELECT on `MARTS` (and `MARTS_BUILD` so future-table grants survive SWAP). USAGE on the Streamlit app. **Cannot read RAW.** |
| `ANALYST` | SELECT on every dbt-managed schema. **No writes anywhere.** |
| `ACCOUNTADMIN` (TF_USER) | Everything. Used only by Terraform. |

Ingestion runs as `LOADER`, dbt runs as `DBT`, the Streamlit app runs as
`DASHBOARD`. Each is sandboxed: a buggy dbt model can't touch raw; a
malicious dashboard can't see raw; the loader can't accidentally publish to
marts.

### Snowflake `STORAGE INTEGRATION` over hardcoded keys

Snowflake assumes an IAM role to read S3. No AWS access keys stored in
Snowflake; trust is via `sts:AssumeRole` with an external ID Snowflake
generates. Rotation is automatic.

### Two Airflow DAGs — separation by concern

- **`dbt_pipeline`** (`@daily`): the warehouse path. Ingest TLC → load to
  Snowflake → run dbt build with layered fail-fast → swap. Runs every day;
  short-circuits cleanly if there's no new data.
- **`spark_pipeline`** (`@monthly`): the historical / scale path. Each
  scheduled run processes exactly **one month** of data via EMR Serverless,
  derived from `logical_date`. `max_active_runs=1` keeps the EMR cost
  bounded. Backfill via `make spark-backfill START=YYYY-MM END=YYYY-MM` —
  Airflow's native `dags backfill` command queues N runs (one per month)
  and runs them sequentially.

Why split? They have different cadences (daily vs monthly), different
resource profiles (Snowflake vs EMR), and different failure isolation
(dbt's mart-test failure shouldn't block Spark from processing new history).

### Smart-resume ingestion in both DAGs

`ingest_tlc.py.ingest_missing()` does an `S3 HEAD` per expected month;
uploads only what's missing. Both DAGs invoke this — the second to fire
gets a no-op fast path (~50ms per month). No coordination needed; no
duplicate downloads.

### Spark — one job per month, daily-grain output

The assignment names "daily aggregation output" — that's the *output
grain*, not the cadence. The Spark *input* boundary is monthly (one TLC
parquet per month), so each Spark job processes one input file → one output
month-partition. Output rows are at `(pickup_date, pu_location_id)` grain
(daily). Partitioned by `(year, month)` for downstream pruning.

This matches input file boundary (no wasted I/O), gives per-month failure
isolation, and supports clean backfill: 168 jobs (14 years × 12 months)
processed sequentially or via `--parallel`.

### Credentials — `with_role.sh` wrapper

Per-role Snowflake credentials live in `.env` as `SNOWFLAKE_LOADER_*`,
`SNOWFLAKE_DBT_*`, `SNOWFLAKE_DASHBOARD_*`, `SNOWFLAKE_ANALYST_*`,
`SNOWFLAKE_TF_*`. The `scripts/with_role.sh <role> <command>` wrapper
exports the right `SNOWFLAKE_USER/PASSWORD/ROLE` for one command and
`exec`s into it. The Makefile uses this wrapper so `make load` always
runs as `LOADER`, `make dbt` as `DBT`, `make infra-apply` as `TF_USER`.

In Airflow, two distinct Snowflake Connections (`snowflake_loader`,
`snowflake_dbt`) carry the per-role credentials.

### Streamlit-in-Snowflake replaces Snowsight Dashboards

Snowflake retired Snowsight Dashboards on April 20, 2026 (mid-project).
Pivoted to **Streamlit-in-Snowflake** — Snowflake's recommended migration
target. Same Snowsight UI surface, fully Terraform-managed via the
`snowflake_streamlit` resource, runs as `DASHBOARD` role (RBAC honoured).
Charts via Plotly. App code in `streamlit/app.py`; deploy via
`make streamlit-deploy`.

---

## How the platform answers the four business questions

| # | Question | Atomic source | Aggregate mart | SQL query | Streamlit |
|---|---|---|---|---|---|
| Q1 | Zone revenue + monthly rank shift | `MARTS.FCT_TRIPS` | `MARTS.AGG_ZONE_REVENUE_MONTHLY` (rank + MoM movement) | `queries/01_zone_revenue.sql` | "Q1 · Zone Revenue by Month" tab |
| Q2 | Demand timing (peaks/troughs) | `MARTS.FCT_TRIPS` | `MARTS.AGG_HOURLY_DEMAND` (hour × DOW × month grid) | `queries/02_hourly_demand.sql` | "Q2 · Demand Timing" tab |
| Q3 | Supply gaps per zone per day | `MARTS.FCT_TRIPS` | `MARTS.AGG_ZONE_SUPPLY_GAPS` (incremental, longest gap + count by threshold) | (computed via the mart; Streamlit + ad-hoc SQL) | "Q3 · Supply Gaps" tab |
| Q4 | Tip behaviour by distance × payment × zone | `MARTS.FCT_TRIPS` | `MARTS.AGG_ZONE_TIP_BEHAVIOUR` | `queries/03_tip_behaviour.sql` | "Q4 · Tip Behaviour" tab |

All atomic facts (`FCT_TRIPS`) and aggregates (`AGG_*`) are dbt models
described in `dbt/models/intermediate/schema.yml` and
`dbt/models/marts/schema.yml`.

---

## Data-quality narrative

### Categories of dirty records found in the raw

The `stg_yellow_trips` model classifies invalid rows by a `case` expression
into one of these `invalid_reason` values. Order matters — first match wins:

| `invalid_reason` | Catches | Typical share |
|---|---|---|
| `null_timestamp` | NULL pickup or dropoff | <<0.01% |
| `pickup_ge_dropoff` | Dropoff at or before pickup — clock/TZ glitches | ~0.04% |
| `duration_out_of_range` | Trip > 12h — meter left running, no real NYC trip is this long | ~0.08% |
| `pickup_year_mismatch` | Pickup in a year ≠ the year being processed (TLC files leak rows from adjacent months) | ~0.001% |
| `non_positive_distance` | `trip_distance ≤ 0` on metered trips — meter glitch | ~0.5% |
| `distance_out_of_range` | `trip_distance > 200 mi` — implausible for an NYC taxi | <0.01% |
| `negative_fare_or_total` | TLC uses the same schema for refunds | ~0.1% |
| `excessive_fare_or_total` | `fare_amount > $1000` or `total_amount > $1000` — meter glitch | ~0.0001% |
| `tip_exceeds_fare_or_total` | `tip > fare` (suspicious) or `tip > total` (mathematically impossible) | ~0.001% |
| `unknown_payment_type` | Outside `{1..6}` | negligible |
| `null_location_id` | Missing pickup or dropoff zone | ~0.05% |
| `duplicate_row` | Same `(vendor, pickup_ts, dropoff_ts, …)` recorded multiple times by TLC | ~few rows/month |

Total quarantine rate: **<1% across 2023**.

### What we do with dirty rows: quarantine, never delete

Invalid rows land in `MARTS.FCT_TRIPS_QUARANTINED` (persisted as a table)
with `invalid_reason` attached. Aggregates read only `FCT_TRIPS`
(`is_valid` rows). This means:

- **Audit trail**. Every row in `RAW.YELLOW_TRIPDATA` appears in exactly
  one of `FCT_TRIPS` or `FCT_TRIPS_QUARANTINED`. No silent drops.
- **Rule iteration is safe**. Tightening a validity rule moves rows from
  enriched to quarantine; no information is lost.
- **Investigation is one query**:

  ```sql
  SELECT invalid_reason, COUNT(*)
  FROM ANALYTICS.MARTS.FCT_TRIPS_QUARANTINED
  GROUP BY 1 ORDER BY 2 DESC;
  ```

### Tests reflect real risks

`dbt build` runs **60+ tests** layered:

- **Schema** — `not_null`, `unique`, `accepted_values` on payment type +
  invalid_reason, `relationships` on zone FKs.
- **Range** — `dbt_expectations.expect_column_values_to_be_between` on
  fare/tip/distance, scoped to `is_valid` rows so they only check what
  staging classed as legitimate.
- **Unique combination** (`dbt_utils.unique_combination_of_columns`) on
  every mart's grain.
- **Constraints** — `PRIMARY KEY` declared on `FCT_TRIPS.trip_sk` and
  `DIM_ZONES.location_id` (Snowflake metadata; tested at every dbt run).
- **Custom generic** — `trip_duration_sane` (`max_minutes=720`).
  Chosen because duration is the metric most likely to *silently* corrupt
  revenue answers — TLC has documented timezone bugs producing hundreds
  of hours that pass null/range checks.
- **Singular** — `assert_monthly_row_counts_sane` flags M-over-M trip
  counts that differ by more than 40% (with a min-volume floor to skip
  cross-month bleed). Catches partial ingests and TLC re-publishing with
  silent corrections.

---

## Brainstormer answers

### dbt — what dirty records did you find, and what did you decide?

Covered in the *Data-quality narrative* above. The deliberate design call
was **quarantine, not delete**. Cost: `FCT_TRIPS_QUARANTINED` is a small
secondary table. Benefit: audit trail; safe rule iteration; the "implicit
data loss" risk that haunts most ETL is structurally impossible here.

### Airflow — preventing corrupt dashboards if a DQ test fails mid-pipeline

We **prevent it structurally** via the blue-green swap pattern:

1. dbt builds every model into `MARTS_BUILD` (the "construction" schema).
2. `MARTS` (the production schema dashboards read) is untouched.
3. `dbt build` runs models + tests atomically per layer with `--fail-fast`.
4. The Airflow `swap_marts_blue_green` task only fires if `dbt_build_marts`
   succeeded (default `all_success` trigger rule). Test failure → task
   skipped → `MARTS` retains the previous good build.
5. The swap itself is `ALTER SCHEMA SWAP WITH` — atomic, metadata-only,
   instant. Dashboards never observe a half-built mart.

If a mart test fails: dashboards keep showing yesterday's correct data;
ops investigates; next successful run swaps in the fix. No rollback
needed because no bad data ever reached production.

What I'd add at scale (deferred): a singular test that pages on volume
anomalies (`assert_monthly_row_counts_sane` already exists; production
would route it to PagerDuty rather than email).

### SQL — most expensive query, and the production fix

**`AGG_ZONE_SUPPLY_GAPS`** is the most expensive operation in the project.
It computes `LAG(pickup_ts) OVER (PARTITION BY zone, day ORDER BY pickup_ts)`
across ~38M rows. The window's `PARTITION BY` doesn't parallelise across
zone boundaries, so one giant sort.

What I did about it:

1. **Materialised as `incremental` on `pickup_date`.** Daily reruns
   reprocess only one new day's rows (~100k), not the full year.
2. **Clustered the underlying `FCT_TRIPS` on `(pickup_date, pu_location_id)`.**
   Snowflake prunes micro-partitions on the two hottest predicates.
   Modest gain at 38M; significant at the historical scale (1.5B rows).
3. **`ANY_VALUE` over `MAX`** for grouping cols where order doesn't
   matter — Snowflake can short-circuit cheaper.
4. **Result-cache friendly**: dashboards re-querying the same date range
   get cache hits as long as the underlying mart hasn't changed (24h TTL
   in Snowflake).

What I'd add for true scale (1.5B rows, full history): switch from
`incremental` to streams + tasks for change-data-capture, so we only
recompute the gaps on changed `pickup_date` partitions.

### Spark — would deploy on EMR or Glue

We **already deploy on EMR Serverless** — `infra/emr.tf` provisions the
application + IAM exec role; `spark/submit_emr.py` submits jobs via
`boto3.client('emr-serverless').start_job_run`. EMR Serverless picked
because:

- **Pre-init capacity = 0** → zero idle cost. Perfect for the per-month
  cadence (sometimes weeks between runs).
- **No cluster lifecycle to manage** — no SSH keys, security groups,
  bootstrap actions.
- **Per-job IAM** — each run assumes the `analytics-emr-exec` role;
  scoped to S3 `raw/`, `analytics/`, `spark-logs/`, `spark-scripts/`.
- **Same `spark-submit` semantics** as EC2-based EMR — same job script
  works on both.

If the workload were chronic (running constantly), Glue would be the
alternative — managed Spark with similar cost profile but slightly
higher per-job overhead. For the monthly cadence + bursty backfills, EMR
Serverless wins.

---

## Operational reference

```bash
make help                         # all targets

# --- Setup / teardown ---
make infra-init                   # one-time
make infra-apply                  # ~3 min, creates ~75 resources
make infra-destroy                # ~1 min, removes everything

# --- Pipeline (manual) ---
make ingest MONTHS=2023-01        # TLC → S3 (any single month or full year)
make load                         # COPY INTO RAW (LOADER role)
make dbt                          # full dbt build + tests + blue-green swap
make swap                         # manual SWAP only (e.g., emergency rollback)
make aws-all                      # ingest + load + dbt + streamlit-deploy

# --- Spark (CLI / backfill) ---
make spark-submit ARGS="--year 2023 --month 6"
make spark-backfill START=2020-01 END=2023-12             # only missing months
make spark-backfill START=2020-01 END=2023-12 FORCE=true  # rebuild all

# --- Streamlit ---
make streamlit-deploy             # PUT app.py + environment.yml to the stage
                                  # → open Snowsight → Streamlit → ANALYTICS_APP

# --- Airflow (Astro) ---
cd airflow && astro dev start     # http://localhost:8080  admin/admin
cd airflow && astro dev stop      # preserve metadata
cd airflow && astro dev kill      # wipe metadata too

# --- Snowflake roles for ad-hoc ---
USE ROLE LOADER;     -- can write RAW only
USE ROLE DBT;        -- owns dbt-built schemas; reads RAW
USE ROLE DASHBOARD;  -- reads MARTS only (Streamlit's role)
USE ROLE ANALYST;    -- reads everything; no writes
```

---

## Backfill cookbook

### Two pipelines, two backfill mechanisms

The platform has two transformation paths and each backfills differently:

- **Warehouse path** (`dbt_pipeline`): TLC → S3 → Snowflake → dbt → MARTS.
  Backfilled by re-running ingest + load + dbt against the desired month range.
- **Spark path** (`spark_pipeline`): one DAG run per month, `logical_date`-driven.
  Backfilled via `airflow dags backfill`, wrapped in `make spark-backfill`.

Both follow smart-resume — work that's already done is skipped (HEAD checks
on S3 + Snowflake's COPY INTO load history + spark_pipeline's
`skip_if_already_processed` short-circuit).

### Warehouse path — historical backfill

The warehouse path doesn't have a "backfill DAG" — it processes whatever's
in `RAW.YELLOW_TRIPDATA` every run. To backfill years of historical data:

```bash
# 1. Pull the historical TLC files into S3 (any year format works)
make ingest MONTHS=2009                # all 12 months of 2009
make ingest MONTHS=2009,2010,2011      # multi-year
make ingest MONTHS=2023-06             # single month

# 2. Load whatever's new in S3 into Snowflake (idempotent)
make load

# 3. Rebuild marts on the cumulative dataset
make dbt
```

For a clean full historical backfill (e.g. 15 years):

```bash
# Loop with explicit year list. Each year is ~600 MB to S3 (~$0.04/month
# storage). Snowflake load is incremental — already-loaded files skipped.
for y in $(seq 2009 2023); do
    make ingest MONTHS=$y
done
make load
make dbt
```

Total time: ~30-60 min for 15 years (~7-9 GB total to S3, ~5 min Snowflake
load, ~5 min dbt build over the full dataset).

### Spark path — historical backfill

```bash
# Default — current year, all months TLC has published so far
make spark-backfill

# Explicit range, sequential (max_active_runs=1 throttles to 1 EMR job at a time)
make spark-backfill START=2023-01 END=2023-12

# Multi-year, sequential — 168 jobs for full 14-year history
make spark-backfill START=2009-01 END=2022-12

# Force-rebuild a range even if analytics/ output already exists
make spark-backfill START=2023-06 END=2023-08 FORCE=true
```

What happens under the hood:
1. Computes default range if `START` / `END` not provided
   (`START = current-year-Jan`, `END = today − 2 months`).
2. Calls `airflow dags backfill spark_pipeline --start-date X --end-date Y
   --conf '{"lag_months": 0}'` inside the Astro scheduler container.
3. Airflow enqueues N DAG runs (one per month). `max_active_runs=1` runs
   them sequentially.
4. Each DAG run:
   - Checks `analytics/year=Y/month=M/` — if exists and `force=false`,
     skip the rest of the DAG (cheap, ~1 S3 LIST call).
   - Else: ingest that month to S3 if not already there; submit one EMR
     Serverless job; wait; verify success.

Cost estimate: ~$0.05-0.20 per month × N. Full 14-year history ≈ $10-30
in EMR Serverless cost over ~10-15 hours of wall time.

### Single-month reprocess (Spark)

If one month's analytics output is corrupted and you want to rebuild
just that month:

```bash
make spark-backfill START=2023-06 END=2023-06 FORCE=true
```

Or via Airflow UI: trigger `spark_pipeline` with config dialog, override
`logical_date` to `2023-06-01`, set `lag_months=0` and `force=true`.

### Wipe-and-rebuild (nuclear option)

To rebuild every month's Spark output from scratch:

```bash
# Wipe the analytics output entirely
aws s3 rm "s3://$(terraform -chdir=infra output -raw s3_bucket)/analytics/" --recursive

# Then a normal backfill — every month looks "missing", so all are rebuilt
# (no FORCE needed, since nothing exists in analytics/ to skip)
make spark-backfill START=2009-01 END=2023-12
```

### TLC publishing lag (why scheduled runs use `lag_months=2`)

TLC publishes month M roughly **2 months** after M ends. The scheduled
`@monthly` Spark DAG fires for `logical_date = (previous month start)`,
but with `lag_months=2` it actually processes `(logical_date − 2 months)`
data — a month TLC has already published. So scheduled runs succeed
first try (no 15-day retry waits).

For backfill, this lag is meaningless (historical data is already
published). `make spark-backfill` always passes `lag_months=0` so the
START / END you specify are the actual data months processed.

### Backfill failure recovery

If `make spark-backfill` fails partway through (e.g., one EMR job times
out), in-flight runs continue to completion; remaining runs are not
enqueued. To resume:

```bash
# Re-run the same command — already-done months are skipped via
# skip_if_already_processed, only failed/missing months are retried.
make spark-backfill START=2023-01 END=2023-12
```

If you want to retry a specifically failed month:

```bash
make spark-backfill START=2023-06 END=2023-06 FORCE=true
```

---

## AI tools — what I used and how

Built entirely with **Claude Code** (Anthropic's agentic CLI; Claude Opus 4.7).
The whole repo is the result of a single multi-day Claude Code session,
visible in the commit history.

### Workflow

1. **Plan first.** Fed the assignment PDF and my constraints (Snowflake
   trial, Terraform-everything, Astro CLI, S3-only raw, real EMR run not
   just a script) and had Claude produce a layered end-to-end plan.
2. **Iterate on architecture decisions in conversation.** Every design
   call (single-apply Terraform via predicted ARN; quarantine vs drop;
   blue-green via SWAP; Streamlit replacing dashboards mid-project; one
   Spark job per month) started as a discussion, ended as committed code
   + a paragraph in this README.
3. **Code and validate together.** Claude wrote scaffolding, terraform,
   dbt models, Airflow DAGs, the Spark job, the Streamlit app, ran
   `terraform validate` / `dbt parse` / `ruff` after each change, and
   fixed errors before handing back.
4. **Pivot fluidly.** Mid-project Snowsight Dashboards got deprecated;
   we swapped to Streamlit-in-Snowflake without losing any work. When I
   asked to remove the STAGING schema, Claude propagated the change
   across Terraform + dbt configs + the DAG in one pass.

### What "agentic" bought me beyond autocomplete

- **Cross-file consistency.** The S3 bucket name is one variable in
  Terraform; it's referenced across 6 files. Claude kept them aligned.
  Same for role names, env vars, dbt refs.
- **Validation loops without prompting.** After every change Claude
  re-ran the relevant validators (`dbt parse`, `terraform validate`,
  `ruff`) and fixed issues silently before reporting back.
- **Decision compression.** The biggest accelerator wasn't code-gen —
  it was Claude raising "have you considered…" trade-offs *before* I
  built the wrong thing. Examples:
  - "Snowsight Dashboards is deprecated; here are 3 alternatives."
  - "TLC parquet uses INT64 microseconds; your `$1::TIMESTAMP_NTZ` cast
    will misinterpret it as nanoseconds → year 1970. Here's the fix."
  - "Your singular row-count test will trigger because Jan parquet has a
    handful of Feb-tail rows. Add a min-volume floor."
- **Pivot velocity.** Mid-build refactors (move to MARTS_BUILD/MARTS,
  switch to two DAGs, introduce dynamic task mapping, then later switch
  Spark to one-DAG-run-per-month) each took <30 min instead of a half-day.

### Time saved

Without agentic AI this is ~3-4 days of senior-engineer work. With Claude
Code, ~1 working day end-to-end (~8 hours of active iteration). The
multiplier wasn't 5×; it was closer to **3-4×** — but the quality is
higher because every architectural choice was deliberately surfaced
rather than implicit.

---

## Trade-offs and shortcuts

| Trade-off | Why we made it | Cost |
|---|---|---|
| **Snowflake trial (30-day, $400 credits)** | Free; sufficient for the project. | Reviewer needs to either use the trial or run `terraform apply` against their own account. Terraform setup = 5 min. |
| **Astro CLI (Docker required)** | Standard local Airflow; reviewer-reproducible. | Reviewer needs Docker Desktop. Without Docker, ingestion + dbt still run via the venv (steps 1-5 of setup) — only Airflow is gated. |
| **No CI** | Time. | Recommended next step: GitHub Actions running `dbt parse` + `terraform validate` + `ruff` + `pytest` on every PR. |
| **No EC2 deployment** | Adds ~3 hours of devops; doesn't change the Airflow scoring rubric. | DAG code is portable to MWAA / Astronomer / EC2. Documented in this README's architecture section. |
| **Spark validity rules slightly drift from dbt's** | Spark's filter is intentionally cruder (5 rules vs dbt's 13). | Macro totals match within ~0.1%. Bringing them to exact parity is a 10-line change in `spark/process_historical.py`. |
| **`FCT_TRIPS` materialised, not view** | Faster downstream queries, time-travel, clustering. | ~$0.06/month storage on 2023 (negligible). |
| **Dashboard is Streamlit, not Snowsight** | Forced — Snowsight Dashboards retired April 2026. | None — Streamlit is more flexible (Plotly, custom layout, parameters). |

---

## Future work

- **GitHub Actions CI**: `dbt parse` + `terraform validate` + `ruff` +
  `astro dev pytest` on every PR.
- **Streams + tasks for incremental marts**: replace dbt's `incremental`
  on `AGG_ZONE_SUPPLY_GAPS` with Snowflake-native CDC for true low-latency
  refresh.
- **Snowflake Iceberg tables** for `RAW`: get external-table flexibility
  with native-table query speed.
- **Sync Spark validity rules to dbt's** for bit-for-bit cross-validation.
- **Add MWAA / Astronomer Cloud Terraform module** for a production
  Airflow deploy story.

---

## Appendix — file-by-file purpose

`infra/` Terraform modules — see comments in each `.tf`.
`ingestion/ingest_tlc.py` — TLC → S3 streaming with smart-resume.
`ingestion/load_snowflake.py` — `COPY INTO RAW.YELLOW_TRIPDATA` from external stage.
`dbt/macros/swap_marts.sql` — atomic `ALTER SCHEMA SWAP WITH`.
`dbt/macros/test_trip_duration_sane.sql` — custom generic test.
`dbt/macros/generate_schema_name.sql` — override so `+schema:` produces literal Snowflake schema names.
`dbt/tests/assert_monthly_row_counts_sane.sql` — singular test on M-over-M trip count.
`airflow/dags/dbt_pipeline.py` — daily warehouse path.
`airflow/dags/spark_pipeline.py` — monthly Spark path with logical_date semantics.
`airflow/include/taxi_helpers.py` — shared `months_up_to` helper.
`spark/process_historical.py` — one-month Spark job.
`spark/submit_emr.py` — boto3 helper to submit + poll EMR Serverless jobs.
`streamlit/app.py` — Snowflake-native dashboard.
`scripts/with_role.sh` — Snowflake-role-aware command wrapper.
`scripts/deploy_streamlit.py` — PUT Streamlit source to its stage.
`Makefile` — see `make help`.
