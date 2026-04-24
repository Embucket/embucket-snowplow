#!/usr/bin/env python3
"""Zero-copy snapshot a Snowflake schema via CREATE OR REPLACE SCHEMA ... CLONE.

Usage:
    uv run python scripts/snowflake_clone_schema.py \\
        --src sturukin_db.atomic_derived \\
        --dst sturukin_db.atomic_derived_baseline_fr
"""

from __future__ import annotations

import argparse

import snowflake.connector


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", required=True, help="database.schema")
    parser.add_argument("--dst", required=True, help="database.schema")
    parser.add_argument("--connection", default="default")
    args = parser.parse_args()

    conn = snowflake.connector.connect(connection_name=args.connection)
    try:
        cur = conn.cursor()
        sql = f"CREATE OR REPLACE SCHEMA {args.dst} CLONE {args.src}"
        print(sql)
        cur.execute(sql)
        print(f"cloned {args.src} -> {args.dst}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
