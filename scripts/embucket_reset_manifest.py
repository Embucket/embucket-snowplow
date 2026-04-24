#!/usr/bin/env python3
"""Reset Snowplow state tables on Embucket before a parity run.

Counterpart to snowflake_reset_manifest.py. Drops the three tables
dbt-snowplow-web uses to track processed windows so the next dbt run
starts clean.

Usage:
    uv run python scripts/embucket_reset_manifest.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import embucket_client

LAMBDA_ARN = (
    "arn:aws:lambda:us-east-2:767397688925:function:"
    "embucket-demo-embucket-demo-ramp-1775514830"
)

STATE_TABLES = [
    "snowplow_web_incremental_manifest",
    "snowplow_web_base_sessions_lifecycle_manifest",
    "snowplow_web_base_quarantined_sessions",
]


def main() -> None:
    client = embucket_client.lambda_client(LAMBDA_ARN)
    token = embucket_client.login(client, LAMBDA_ARN)
    for table in STATE_TABLES:
        sql = f"DROP TABLE IF EXISTS demo.atomic_snowplow_manifest.{table}"
        print(sql)
        embucket_client.run_sql(client, LAMBDA_ARN, token, sql)
    print("manifest state cleared")


if __name__ == "__main__":
    main()
