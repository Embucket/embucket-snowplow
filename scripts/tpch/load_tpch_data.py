#!/usr/bin/env python3
"""Load TPC-H SF10 data into Embucket Lambda.

Usage:
    python scripts/tpch/load_tpch_data.py <lambda_function_arn>

Creates the tpch schema, creates all 8 TPC-H tables, and loads
SF10 parquet data from s3://embucket-testdata/tpch/10/.
"""
import boto3
import json
import sys
import uuid
from pathlib import Path

TPCH_TABLES = [
    "nation",
    "region",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]

S3_PREFIX = "s3://embucket-testdata/tpch/10"


def invoke(client, fn, payload):
    resp = client.invoke(FunctionName=fn, Payload=json.dumps(payload))
    raw = resp["Payload"].read()
    if resp.get("FunctionError"):
        raise RuntimeError(f"Lambda error: {raw.decode()[:300]}")
    return json.loads(raw) if raw else {}


def login(client, fn):
    result = invoke(client, fn, {
        "version": "2.0",
        "rawPath": "/session/v1/login-request",
        "rawQueryString": "",
        "requestContext": {
            "http": {"method": "POST", "path": "/session/v1/login-request", "sourceIp": "127.0.0.1"},
            "accountId": "anonymous", "apiId": "anonymous",
        },
        "headers": {"content-type": "application/json", "host": "localhost", "x-forwarded-for": "127.0.0.1"},
        "body": json.dumps({"data": {
            "LOGIN_NAME": "demo_user", "PASSWORD": "demo_password_2026",
            "CLIENT_APP_ID": "tpch-loader", "CLIENT_APP_VERSION": "1.0",
            "ACCOUNT_NAME": "embucket", "CLIENT_ENVIRONMENT": {}, "SESSION_PARAMETERS": {},
        }}),
        "isBase64Encoded": False,
    })
    return json.loads(result["body"])["data"]["token"]


def run_sql(client, fn, token, sql):
    result = invoke(client, fn, {
        "version": "2.0",
        "rawPath": "/queries/v1/query-request",
        "rawQueryString": f"requestId={uuid.uuid4()}",
        "requestContext": {
            "http": {"method": "POST", "path": "/queries/v1/query-request", "sourceIp": "127.0.0.1"},
            "accountId": "anonymous", "apiId": "anonymous",
        },
        "headers": {
            "content-type": "application/json",
            "host": "localhost",
            "x-forwarded-for": "127.0.0.1",
            "authorization": f'Snowflake Token="{token}"',
        },
        "body": json.dumps({"sqlText": sql}),
        "isBase64Encoded": False,
    })
    body = json.loads(result.get("body", "{}"))
    if not body.get("success"):
        raise RuntimeError(f"SQL failed: {body.get('message', 'unknown error')}")
    return body


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/tpch/load_tpch_data.py <lambda_function_arn_or_name>")
        sys.exit(1)

    fn = sys.argv[1]
    region = "us-east-2"
    if ":lambda:" in fn:
        region = fn.split(":")[3]

    client = boto3.client("lambda", region_name=region)
    scripts_dir = Path(__file__).parent

    print("Logging in...")
    token = login(client, fn)

    # Create tpch schema
    print("Creating schema demo.tpch...")
    run_sql(client, fn, token, "CREATE SCHEMA IF NOT EXISTS demo.tpch")

    # Drop and recreate each table
    ddl_text = (scripts_dir / "create_tables.sql").read_text()
    # Remove SQL comments and split on semicolons
    import re
    ddl_clean = re.sub(r"--[^\n]*", "", ddl_text)
    statements = [s.strip() for s in ddl_clean.split(";") if s.strip()]

    for stmt in statements:
        # Extract table name for logging
        table_name = stmt.split("demo.tpch.")[1].split()[0] if "demo.tpch." in stmt else "unknown"
        print(f"  Dropping {table_name} (if exists)...")
        try:
            run_sql(client, fn, token, f"DROP TABLE IF EXISTS demo.tpch.{table_name}")
        except RuntimeError:
            pass
        print(f"  Creating {table_name}...")
        run_sql(client, fn, token, stmt)

    # Load data from S3 parquet files
    for table in TPCH_TABLES:
        s3_path = f"{S3_PREFIX}/{table}.parquet"
        copy_sql = (
            f"COPY INTO demo.tpch.{table} "
            f"FROM '{s3_path}' "
            f"FILE_FORMAT = (TYPE = PARQUET)"
        )
        print(f"Loading {table} from {s3_path}...")
        run_sql(client, fn, token, copy_sql)
        print(f"  {table} loaded.")

    print("\nDone! All TPC-H SF10 tables loaded successfully.")


if __name__ == "__main__":
    main()
