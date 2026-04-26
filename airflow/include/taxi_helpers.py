"""Shared helpers for both DAGs.

Anything that lives here MUST be importable without an Airflow context — it
runs at task-execution time inside the worker, not at DAG-parse time.
"""

from __future__ import annotations

from calendar import monthrange
from datetime import datetime, timedelta


def months_up_to(
    logical_date: datetime,
    *,
    lag_months: int = 2,
    year_gate: int = 2023,
    year_cap: int | None = None,
) -> list[str]:
    """Return YYYY-MM tags that should have been published by `logical_date`.

    Walks years from `year_gate` (start) through the cutoff. Optional
    `year_cap` lets you upper-bound the range (e.g., for testing on a
    single year without paying for years of bulk ingestion + Spark).

    TLC publishes month M roughly 2 months after M ends, so we trail
    `logical_date` by `lag_months`. Backfills work because we key off
    `logical_date`, not `datetime.now()`.
    """
    # Airflow's logical_date is tz-aware (UTC). Our `first_of_next_month`
    # constructions below are naive. Strip tz so comparisons don't error.
    cutoff = logical_date.replace(tzinfo=None) - timedelta(days=31 * lag_months)
    cutoff = cutoff.replace(day=monthrange(cutoff.year, cutoff.month)[1])

    upper_year = min(cutoff.year, year_cap) if year_cap is not None else cutoff.year

    months: list[str] = []
    for year in range(year_gate, upper_year + 1):
        for m in range(1, 13):
            first_of_next_month = datetime(year, m, 28) + timedelta(days=5)
            if first_of_next_month <= cutoff:
                months.append(f"{year:04d}-{m:02d}")
    return months


def split_yyyy_mm(tag: str) -> tuple[int, int]:
    """'2023-07' → (2023, 7)."""
    y, m = tag.split("-")
    return int(y), int(m)
