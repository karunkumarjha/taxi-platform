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
    assert dag.schedule_interval == "@daily"
    assert dag.max_active_runs == 1
    assert "dbt" in dag.tags
    assert "snowflake" in dag.tags


def test_spark_pipeline_exists(dagbag: DagBag) -> None:
    """spark_pipeline is registered with the expected schedule/tags."""
    dag = dagbag.get_dag("spark_pipeline")
    assert dag is not None, "spark_pipeline DAG missing"
    assert dag.schedule_interval == "@monthly"
    assert dag.max_active_runs == 1
    assert "spark" in dag.tags
    assert "emr" in dag.tags


def test_dbt_pipeline_has_swap_task(dagbag: DagBag) -> None:
    """The blue-green swap task is wired into dbt_pipeline."""
    dag = dagbag.get_dag("dbt_pipeline")
    task_ids = {t.task_id for t in dag.tasks}
    # swap is the blue-green flip — critical, so explicitly assert presence
    assert "dbt_build.swap_marts_blue_green" in task_ids


def test_spark_pipeline_one_month_per_run(dagbag: DagBag) -> None:
    """Each spark_pipeline DAG run processes one month derived from logical_date."""
    dag = dagbag.get_dag("spark_pipeline")
    task_ids = {t.task_id for t in dag.tasks}
    expected = {
        "compute_target_month",
        "skip_if_already_processed",
        "ingest_one_month",
        "process_one_month",
        "notify_success",
    }
    assert expected.issubset(task_ids), f"missing tasks: {expected - task_ids}"
