#!/usr/bin/env python3
"""Load source data into Embucket Lambda.

Usage:
    python scripts/load_data.py <lambda_function_arn>

Reads SQL from scripts/create_table.sql and scripts/copy_data.sql,
then executes them against the Lambda via boto3 invoke.
"""
import boto3
import json
import sys
import uuid
from pathlib import Path


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
            "LOGIN_NAME": "embucket", "PASSWORD": "embucket",
            "CLIENT_APP_ID": "setup", "CLIENT_APP_VERSION": "1.0",
            "ACCOUNT_NAME": "test", "CLIENT_ENVIRONMENT": {}, "SESSION_PARAMETERS": {},
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
        print("Usage: python scripts/load_data.py <lambda_function_arn_or_name>")
        sys.exit(1)

    fn = sys.argv[1]
    region = "us-east-2"
    if ":lambda:" in fn:
        region = fn.split(":")[3]

    client = boto3.client("lambda", region_name=region)
    scripts_dir = Path(__file__).parent

    print("Logging in...")
    token = login(client, fn)

    # Create required schemas for snowplow_web
    schemas = [
        "CREATE SCHEMA IF NOT EXISTS demo.atomic",
        "CREATE SCHEMA IF NOT EXISTS demo.atomic_scratch",
        "CREATE SCHEMA IF NOT EXISTS demo.atomic_derived",
        "CREATE SCHEMA IF NOT EXISTS demo.atomic_snowplow_manifest",
    ]
    for sql in schemas:
        print(f"  {sql}")
        run_sql(client, fn, token, sql)

    # Create events table
    create_sql = (scripts_dir / "create_table.sql").read_text().strip()
    print("Creating events table...")
    run_sql(client, fn, token, create_sql)

    # Load data
    copy_sql = (scripts_dir / "copy_data.sql").read_text().strip()
    print("Loading data (this may take a minute)...")
    run_sql(client, fn, token, copy_sql)

    print("Done! Source data loaded successfully.")


if __name__ == "__main__":
    main()
