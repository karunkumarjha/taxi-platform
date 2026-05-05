"""Shared failure-alert callback for Airflow DAGs.

Hoisted out of the individual DAG files because both `dbt_pipeline` and
`spark_historical` need the same behaviour:

  • Sidestep Airflow 3.0.x's built-in `email_on_failure`, whose default
    Jinja template references `ti.mark_success_url` — an attribute that
    no longer exists on `RuntimeTaskInstance`, crashing the send.
  • Silently no-op when ALERT_EMAIL is unset, so the DAG still runs in
    environments without SMTP configured.
  • Swallow SMTP exceptions — a bad mailbox shouldn't break task failure
    handling.

Lives in `airflow/include/`, which Astro Runtime mounts onto PYTHONPATH
inside both the scheduler and worker containers. Import from a DAG via:

    from include.alerts import on_failure_callback, ALERT_EMAIL

    DEFAULT_ARGS = {
        ...,
        "on_failure_callback": [on_failure_callback] if ALERT_EMAIL else [],
    }
"""

from __future__ import annotations

import logging
import os
import smtplib
from email.mime.text import MIMEText

log = logging.getLogger(__name__)

ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "")


def on_failure_callback(context: dict) -> None:
    """Email ALERT_EMAIL once a task has terminally failed (post-retries)."""
    if not ALERT_EMAIL:
        return

    ti = context.get("task_instance") or context.get("ti")
    if ti is None:
        return

    body = (
        f"Task {ti.dag_id}.{ti.task_id} FAILED.\n\n"
        f"Run ID:        {getattr(ti, 'run_id', 'unknown')}\n"
        f"Try number:    {getattr(ti, 'try_number', '?')}\n"
        f"Logical date:  {context.get('logical_date', '?')}\n\n"
        f"Check the Airflow UI Grid view for the full task log."
    )
    msg = MIMEText(body)
    msg["Subject"] = f"[Airflow FAILED] {ti.dag_id}.{ti.task_id}"
    msg["From"] = os.environ.get("AIRFLOW__SMTP__SMTP_MAIL_FROM", ALERT_EMAIL)
    msg["To"] = ALERT_EMAIL

    try:
        host = os.environ["AIRFLOW__SMTP__SMTP_HOST"]
        port = int(os.environ.get("AIRFLOW__SMTP__SMTP_PORT", "587"))
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            if os.environ.get("AIRFLOW__SMTP__SMTP_STARTTLS", "True").lower() == "true":
                smtp.starttls()
            user = os.environ.get("AIRFLOW__SMTP__SMTP_USER")
            pwd = os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD")
            if user and pwd:
                smtp.login(user, pwd)
            smtp.send_message(msg)
    except Exception as e:  # noqa: BLE001 — last-ditch alert; don't propagate
        log.warning("failure-email send failed: %s", e)
