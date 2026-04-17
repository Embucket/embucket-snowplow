#!/usr/bin/env python3
"""Load the Snowplow Glue Iceberg source into Embucket's S3 Tables bucket.

Runs an Athena CTAS that reads the Glue-managed Iceberg source and writes a
new Iceberg table into the S3 Tables bucket Embucket is volumed to. Embucket
and dbt-snowplow-web then read that table directly.

Usage:
    uv run python scripts/load_from_glue.py \\
        --query-output-location s3://athena-query-results-us-east-2-767397688925/
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import boto3


TARGET_CATALOG = "s3tablescatalog/snowplow"
TARGET_SCHEMA = "atomic"
TARGET_TABLE = "events_0416"
DEFAULT_QUERY_OUTPUT = "s3://athena-query-results-us-east-2-767397688925/embucket-snowplow/"
DEFAULT_WORKGROUP = "primary"
DEFAULT_REGION = "us-east-2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--query-output-location",
        default=DEFAULT_QUERY_OUTPUT,
        help=f"S3 URI for Athena query result metadata (default: {DEFAULT_QUERY_OUTPUT})",
    )
    parser.add_argument(
        "--workgroup",
        default=DEFAULT_WORKGROUP,
        help=f"Athena workgroup (default: {DEFAULT_WORKGROUP})",
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"AWS region (default: {DEFAULT_REGION})",
    )
    parser.add_argument(
        "--skip-drop",
        action="store_true",
        help="Do not drop the target table before CTAS (will fail if it exists)",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=None,
        help="Optional LIMIT on the CTAS SELECT (for smoke-testing on smaller slices)",
    )
    return parser.parse_args()


def run_query(
    client,
    sql: str,
    output_location: str,
    workgroup: str,
    catalog: str | None = None,
    database: str | None = None,
) -> str:
    query_context = {}
    if catalog:
        query_context["Catalog"] = catalog
    if database:
        query_context["Database"] = database
    kwargs = {
        "QueryString": sql,
        "ResultConfiguration": {"OutputLocation": output_location},
        "WorkGroup": workgroup,
    }
    if query_context:
        kwargs["QueryExecutionContext"] = query_context
    start = client.start_query_execution(**kwargs)
    query_id = start["QueryExecutionId"]
    print(f"  athena query id: {query_id}")

    while True:
        resp = client.get_query_execution(QueryExecutionId=query_id)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {state}: {reason}")

    return query_id


def main() -> None:
    args = parse_args()
    scripts_dir = Path(__file__).parent
    ctas_sql = (scripts_dir / "ctas_from_glue.sql").read_text()
    if args.row_limit:
        # Strip the trailing semicolon and wrap with LIMIT.
        ctas_sql = ctas_sql.rstrip().rstrip(";") + f"\nLIMIT {args.row_limit};"

    client = boto3.client("athena", region_name=args.region)

    target_fqn = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

    if not args.skip_drop:
        print(f"Dropping {target_fqn} if it exists...")
        run_query(
            client,
            f"DROP TABLE IF EXISTS {TARGET_TABLE}",
            args.query_output_location,
            args.workgroup,
            catalog=TARGET_CATALOG,
            database=TARGET_SCHEMA,
        )

    print(f"Running CTAS into {target_fqn}...")
    run_query(
        client,
        ctas_sql,
        args.query_output_location,
        args.workgroup,
        catalog=TARGET_CATALOG,
        database=TARGET_SCHEMA,
    )

    print(f"Counting rows in {target_fqn}...")
    count_qid = run_query(
        client,
        f"SELECT COUNT(*) FROM {TARGET_TABLE}",
        args.query_output_location,
        args.workgroup,
        catalog=TARGET_CATALOG,
        database=TARGET_SCHEMA,
    )
    result = client.get_query_results(QueryExecutionId=count_qid)
    rows = result["ResultSet"]["Rows"]
    # row 0 is the header, row 1 is the single count value
    count = rows[1]["Data"][0]["VarCharValue"]
    print(f"Done. events_0416 row count: {count}")


if __name__ == "__main__":
    main()
