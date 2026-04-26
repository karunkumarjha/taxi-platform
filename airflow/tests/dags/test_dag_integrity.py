"""Smoke test: every DAG imports without error and has the expected shape.

Run from the Astro project root:
    astro dev pytest tests/dags/
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


def test_dbt_pipeline_exists(dagbag: DagBag) -> None:
    """dbt_pipeline is registered with the expected schedule/tags."""
    dag = dagbag.get_dag("dbt_pipeline")
    assert dag is not None, "dbt_pipeline DAG missing"
    assert dag.schedule_interval == "@monthly"
    assert dag.max_active_runs == 1
    assert "dbt" in dag.tags
    assert "snowflake" in dag.tags


def test_dbt_pipeline_one_month_per_run(dagbag: DagBag) -> None:
    """Each dbt_pipeline DAG run processes one month derived from logical_date."""
    dag = dagbag.get_dag("dbt_pipeline")
    task_ids = {t.task_id for t in dag.tasks}
    expected = {
        "compute_target_month",
        "ingest_one_month",
        "load_one_month_to_snowflake",
        "dbt_build.reset_marts_build_from_marts",
        "dbt_build.load_spark_staged_into_marts_build",
        "dbt_build.swap_marts_blue_green",
        "notify_success",
    }
    assert expected.issubset(task_ids), f"missing tasks: {expected - task_ids}"


def test_spark_pipeline_exists(dagbag: DagBag) -> None:
    """spark_pipeline is registered with the expected (manual-only) shape."""
    dag = dagbag.get_dag("spark_pipeline")
    assert dag is not None, "spark_pipeline DAG missing"
    # schedule=None — Spark is historical / manual-trigger only
    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
    assert "spark" in dag.tags
    assert "emr" in dag.tags


def test_dbt_pipeline_has_swap_task(dagbag: DagBag) -> None:
    """The blue-green swap task is wired into dbt_pipeline."""
    dag = dagbag.get_dag("dbt_pipeline")
    task_ids = {t.task_id for t in dag.tasks}
    # swap is the blue-green flip — critical, so explicitly assert presence
    assert "dbt_build.swap_marts_blue_green" in task_ids


def test_spark_pipeline_year_fanout_shape(dagbag: DagBag) -> None:
    """spark_pipeline is a year-driven mapped DAG: enumerate_months emits
    the (year, month) list, process_one_month.expand() maps over it."""
    dag = dagbag.get_dag("spark_pipeline")
    task_ids = {t.task_id for t in dag.tasks}
    expected = {
        "enumerate_months",
        "process_one_month",
        "notify_success",
    }
    assert expected.issubset(task_ids), f"missing tasks: {expected - task_ids}"
    # Required Params for the UI form
    assert "year" in dag.params
    assert "month" in dag.params
    assert "force" in dag.params
