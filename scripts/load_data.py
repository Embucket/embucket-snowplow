#!/usr/bin/env python3
"""Load Snowplow source data into Embucket Lambda."""

from __future__ import annotations

import argparse
from pathlib import Path
from embucket_client import ensure_snowplow_schemas, lambda_client, login, run_sql


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lambda_function", help="Lambda ARN or function name")
    parser.add_argument(
        "--copy-uri",
        help="Override the source parquet path instead of using scripts/copy_data.sql",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append into demo.atomic.events instead of dropping and recreating it",
    )
    return parser.parse_args()


def build_copy_sql(copy_uri: str) -> str:
    return f"""COPY INTO demo.atomic.events
FROM '{copy_uri}'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"""


def main():
    args = parse_args()
    fn = args.lambda_function
    client = lambda_client(fn)
    scripts_dir = Path(__file__).parent

    print("Logging in...")
    token = login(client, fn)

    print("Ensuring required schemas exist...")
    ensure_snowplow_schemas(client, fn, token)

    if not args.append:
        print("Dropping existing events table (if any)...")
        try:
            run_sql(client, fn, token, "DROP TABLE IF EXISTS demo.atomic.events")
        except RuntimeError:
            pass

    create_sql = (scripts_dir / "create_table.sql").read_text().strip()
    print("Ensuring events table exists...")
    run_sql(client, fn, token, create_sql)

    copy_sql = build_copy_sql(args.copy_uri) if args.copy_uri else (scripts_dir / "copy_data.sql").read_text().strip()
    print("Loading data (this may take a minute)...")
    run_sql(client, fn, token, copy_sql)

    print("Done! Source data loaded successfully.")


if __name__ == "__main__":
    main()
