"""Smoke test: every DAG imports without error and has the expected shape.

Run from the Astro project root:
    astro dev pytest tests/dags/

Note: Airflow 3.x's `DagBag.get_dag()` queries the metadata DB (which
isn't initialised in pytest). We access `dagbag.dags[dag_id]` directly
— pulls the parsed DAG from the in-memory dict that DagBag built on
construction. Same data, no DB round-trip.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    """Load all DAGs from airflow/dags/ once per test session."""
    dag_folder = Path(__file__).resolve().parents[2] / "dags"
    return DagBag(dag_folder=str(dag_folder), include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    """Every DAG file imports cleanly."""
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_registered_dags(dagbag: DagBag) -> None:
    """Exactly two DAGs: dbt_pipeline (live) + spark_historical (ad-hoc EMR)."""
    registered = set(dagbag.dags.keys())
    expected = {"dbt_pipeline", "spark_historical"}
    assert registered == expected, (
        f"unexpected DAG set. got: {registered}  expected: {expected}"
    )


def test_dbt_pipeline_exists(dagbag: DagBag) -> None:
    """dbt_pipeline is registered with the expected schedule/tags."""
    dag = dagbag.dags["dbt_pipeline"]
    # In Airflow 3.x `schedule_interval` is deprecated in favour of
    # `schedule`. We accept either to stay version-agnostic in tests.
    schedule = getattr(dag, "schedule", getattr(dag, "schedule_interval", None))
    assert schedule == "@monthly", f"unexpected schedule: {schedule!r}"
    assert dag.max_active_runs == 1
    assert "dbt" in dag.tags
    assert "snowflake" in dag.tags


def test_dbt_pipeline_one_month_per_run(dagbag: DagBag) -> None:
    """Each dbt_pipeline DAG run processes one month; snapshot runs before staging."""
    dag = dagbag.dags["dbt_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    expected = {
        "compute_target_month",
        "ingest_one_month",
        "load_one_month_to_snowflake",
        "dbt_build.dbt_snapshot",
        "dbt_build.reset_marts_build_from_marts",
        "dbt_build.swap_marts_blue_green",
    }
    assert expected.issubset(task_ids), f"missing tasks: {expected - task_ids}"


def test_dbt_pipeline_has_swap_task(dagbag: DagBag) -> None:
    """The blue-green swap task is wired into dbt_pipeline."""
    dag = dagbag.dags["dbt_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    # swap is the blue-green flip — critical, so explicitly assert presence
    assert "dbt_build.swap_marts_blue_green" in task_ids


def test_spark_historical_shape(dagbag: DagBag) -> None:
    """spark_historical: manual-trigger only, year Param required.
    Tasks: submit → wait → create_iceberg → refresh_iceberg."""
    dag = dagbag.dags["spark_historical"]
    schedule = getattr(dag, "schedule", getattr(dag, "schedule_interval", None))
    assert schedule is None
    assert dag.max_active_runs == 1
    assert "year" in dag.params

    task_ids = {t.task_id for t in dag.tasks}
    expected = {
        "ensure_year_in_s3",
        "submit_emr_job",
        "wait_for_emr",
        "create_iceberg_table",
        "refresh_iceberg",
    }
    assert expected.issubset(task_ids), f"missing tasks: {expected - task_ids}"
