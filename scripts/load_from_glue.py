#!/usr/bin/env python3
"""Load the Snowplow Glue Iceberg source into Embucket's S3 Tables bucket.

Two subcommands:
  init     drop the target table, CREATE TABLE with the correct schema + partitioning
           (via CTAS WHERE 1=0 so the SELECT drives column types) but zero rows.
  insert   INSERT INTO the target using the same projection filtered to a
           [start, end) load_tstamp window.

Both subcommands share scripts/events_0416_select.sql as the projection body.

Usage:
    uv run python scripts/load_from_glue.py init
    uv run python scripts/load_from_glue.py insert \\
        --start '2026-04-22 15:00:00' --end '2026-04-22 15:30:00'
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

ICEBERG_PROPS = """WITH (
    table_type = 'ICEBERG',
    is_external = false,
    format = 'PARQUET',
    write_compression = 'ZSTD',
    partitioning = ARRAY['day(load_tstamp)', 'event_name']
)"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-output-location", default=DEFAULT_QUERY_OUTPUT)
    parser.add_argument("--workgroup", default=DEFAULT_WORKGROUP)
    parser.add_argument("--region", default=DEFAULT_REGION)

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init", help="Drop and create empty events_0416")
    ins = sub.add_parser("insert", help="Insert a [start, end) load_tstamp window")
    ins.add_argument("--start", required=True, help="ISO timestamp, e.g. '2026-04-22 15:00:00'")
    ins.add_argument("--end", required=True, help="ISO timestamp, e.g. '2026-04-22 15:30:00'")
    return parser.parse_args()


def render_select(where_clause: str) -> str:
    body = (Path(__file__).parent / "events_0416_select.sql").read_text()
    return body.replace("{{where}}", where_clause)


def run_query(client, sql: str, output_location: str, workgroup: str,
              catalog: str | None = None, database: str | None = None) -> str:
    ctx = {}
    if catalog:
        ctx["Catalog"] = catalog
    if database:
        ctx["Database"] = database
    kwargs = {
        "QueryString": sql,
        "ResultConfiguration": {"OutputLocation": output_location},
        "WorkGroup": workgroup,
    }
    if ctx:
        kwargs["QueryExecutionContext"] = ctx
    qid = client.start_query_execution(**kwargs)["QueryExecutionId"]
    print(f"  athena query id: {qid}")
    while True:
        resp = client.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)
    if state != "SUCCEEDED":
        reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {state}: {reason}")
    return qid


def count_rows(client, output_location: str, workgroup: str) -> str:
    qid = run_query(
        client,
        f"SELECT COUNT(*) FROM {TARGET_TABLE}",
        output_location,
        workgroup,
        catalog=TARGET_CATALOG,
        database=TARGET_SCHEMA,
    )
    result = client.get_query_results(QueryExecutionId=qid)
    return result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]


def cmd_init(args, client) -> None:
    target_fqn = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"
    print(f"Dropping {target_fqn} if it exists...")
    run_query(
        client,
        f"DROP TABLE IF EXISTS {TARGET_TABLE}",
        args.query_output_location,
        args.workgroup,
        catalog=TARGET_CATALOG,
        database=TARGET_SCHEMA,
    )
    select_body = render_select("1=0")
    sql = f"CREATE TABLE {TARGET_TABLE}\n{ICEBERG_PROPS} AS\n{select_body}"
    print(f"Creating empty {target_fqn}...")
    run_query(
        client, sql, args.query_output_location, args.workgroup,
        catalog=TARGET_CATALOG, database=TARGET_SCHEMA,
    )
    print(f"Done. {target_fqn} row count: {count_rows(client, args.query_output_location, args.workgroup)}")


def cmd_insert(args, client) -> None:
    target_fqn = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"
    where = (
        f"load_tstamp >= TIMESTAMP '{args.start}' "
        f"AND load_tstamp <  TIMESTAMP '{args.end}'"
    )
    select_body = render_select(where)
    sql = f"INSERT INTO {TARGET_TABLE}\n{select_body}"
    print(f"Inserting into {target_fqn} for [{args.start}, {args.end})...")
    run_query(
        client, sql, args.query_output_location, args.workgroup,
        catalog=TARGET_CATALOG, database=TARGET_SCHEMA,
    )
    print(f"Done. {target_fqn} row count: {count_rows(client, args.query_output_location, args.workgroup)}")


def main() -> None:
    args = parse_args()
    client = boto3.client("athena", region_name=args.region)
    if args.cmd == "init":
        cmd_init(args, client)
    elif args.cmd == "insert":
        cmd_insert(args, client)
    else:
        raise SystemExit(f"unknown subcommand {args.cmd}")


if __name__ == "__main__":
    main()
