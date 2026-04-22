#!/usr/bin/env python3
"""Refresh the Snowflake Iceberg view of atomic.events_0416 after an Athena write.

Snowflake-managed Iceberg tables over externally-written data do not
auto-refresh; this script issues ALTER ICEBERG TABLE ... REFRESH and prints
the post-refresh row count.

Usage:
    uv run python scripts/snowflake_refresh.py
"""

from __future__ import annotations

import argparse

import snowflake.connector

FQN = "sturukin_db.atomic.events_0416"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection", default="default")
    args = parser.parse_args()

    conn = snowflake.connector.connect(connection_name=args.connection)
    try:
        cur = conn.cursor()
        print(f"Refreshing {FQN}...")
        cur.execute(f"ALTER ICEBERG TABLE {FQN} REFRESH")
        cur.execute(f"SELECT COUNT(*) FROM {FQN}")
        count = cur.fetchone()[0]
        print(f"{FQN} row count: {count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
