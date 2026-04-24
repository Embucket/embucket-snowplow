#!/usr/bin/env python3
"""Reset Snowplow web package state so the next dbt run starts from scratch.

Snowplow tracks processed event windows in three tables under
atomic_snowplow_manifest. `dbt run --full-refresh` does NOT truncate them
(incremental manifest has full_refresh=allow_refresh() which defaults to
false), so state leaks between pipeline variants and prevents apples-to-
apples incremental comparisons.

This script DROPs the three state tables. They are recreated empty on the
next dbt run. Seed-backed dim tables in the same schema are left alone.

Usage:
    uv run python scripts/snowflake_reset_manifest.py
"""

from __future__ import annotations

import argparse

import snowflake.connector

DATABASE = "sturukin_db"
MANIFEST_SCHEMA = "atomic_snowplow_manifest"
STATE_TABLES = [
    "snowplow_web_incremental_manifest",
    "snowplow_web_base_sessions_lifecycle_manifest",
    "snowplow_web_base_quarantined_sessions",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection", default="default")
    args = parser.parse_args()

    conn = snowflake.connector.connect(connection_name=args.connection)
    try:
        cur = conn.cursor()
        for table in STATE_TABLES:
            fqn = f"{DATABASE}.{MANIFEST_SCHEMA}.{table}"
            print(f"DROP TABLE IF EXISTS {fqn}")
            cur.execute(f"DROP TABLE IF EXISTS {fqn}")
        print("manifest state cleared")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
