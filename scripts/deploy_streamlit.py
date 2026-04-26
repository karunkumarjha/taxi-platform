"""
Deploy the Streamlit-in-Snowflake app: PUT app.py + environment.yml to the
stage Terraform created. Snowflake reads the stage on next view, picking
up the new code.

Run via:
    make streamlit-deploy

Idempotent — uses OVERWRITE=TRUE so re-deploys just replace the files.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("deploy_streamlit")

STAGE = "ANALYTICS.MARTS.STREAMLIT_APP_STAGE"
APP_FILES = ("app.py", "environment.yml")


def main() -> int:
    """PUT app.py + environment.yml to the Streamlit stage as TF_USER (ACCOUNTADMIN)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = {
        "account":   os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user":      os.environ.get("SNOWFLAKE_TF_USER"),
        "password":  os.environ.get("SNOWFLAKE_TF_PASSWORD"),
        "role":      os.environ.get("SNOWFLAKE_TF_ROLE", "ACCOUNTADMIN"),
        "warehouse": "WH_XS",
        "database":  "ANALYTICS",
        "schema":    "MARTS",
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        sys.exit(f"missing env vars (check .env): {missing}")

    streamlit_dir = Path(__file__).resolve().parent.parent / "streamlit"
    for fname in APP_FILES:
        if not (streamlit_dir / fname).exists():
            sys.exit(f"missing source file: {streamlit_dir / fname}")

    log.info("connecting as %s/%s", cfg["user"], cfg["role"])
    conn = snowflake.connector.connect(**cfg)
    cur = conn.cursor()

    try:
        for fname in APP_FILES:
            path = streamlit_dir / fname
            sql = (
                f"PUT 'file://{path.resolve()}' @{STAGE} "
                "OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
            )
            log.info("PUT %s → @%s", fname, STAGE)
            cur.execute(sql)

        log.info("listing stage contents:")
        cur.execute(f"LIST @{STAGE}")
        for row in cur.fetchall():
            log.info("  %s  (%d bytes)", row[0], row[1])

        log.info("done. Open Snowsight → Streamlit → ANALYTICS_APP to view.")
        log.info("(refresh the browser if the app's already open — picks up new code)")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
