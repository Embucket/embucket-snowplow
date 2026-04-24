#!/usr/bin/env python3
"""Snapshot the three Snowplow golden tables from demo.atomic_derived into
a dedicated destination schema on Embucket via CTAS.

Embucket (S3 Tables / Iceberg) has no CLONE; CTAS is the portable path.
Cost is another write of each table - the three golden tables total a
few million rows, so this is acceptable for a post-run snapshot.

Usage:
    uv run python scripts/embucket_snapshot_derived.py --dst atomic_derived_baseline_fr
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import embucket_client

LAMBDA_ARN = (
    "arn:aws:lambda:us-east-2:767397688925:function:"
    "embucket-demo-embucket-demo-ramp-1775514830"
)

TABLES = [
    "snowplow_web_page_views",
    "snowplow_web_sessions",
    "snowplow_web_users",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", default="atomic_derived")
    parser.add_argument("--dst", required=True)
    args = parser.parse_args()

    client = embucket_client.lambda_client(LAMBDA_ARN)
    token = embucket_client.login(client, LAMBDA_ARN)

    embucket_client.run_sql(client, LAMBDA_ARN, token,
                            f"CREATE SCHEMA IF NOT EXISTS demo.{args.dst}")

    for tbl in TABLES:
        src_fqn = f"demo.{args.src}.{tbl}"
        dst_fqn = f"demo.{args.dst}.{tbl}"
        print(f"snapshotting {src_fqn} -> {dst_fqn}")
        embucket_client.run_sql(client, LAMBDA_ARN, token,
                                f"DROP TABLE IF EXISTS {dst_fqn}")
        embucket_client.run_sql(client, LAMBDA_ARN, token,
                                f"CREATE TABLE {dst_fqn} AS SELECT * FROM {src_fqn}")
    print("done")


if __name__ == "__main__":
    main()
