#!/usr/bin/env python3
"""Shared helpers for invoking Embucket through AWS Lambda."""

from __future__ import annotations

import json
import uuid

import boto3
from botocore.config import Config

LOGIN_PAYLOAD = {
    "data": {
        "LOGIN_NAME": "demo_user",
        "PASSWORD": "demo_password_2026",
        "CLIENT_APP_ID": "setup",
        "CLIENT_APP_VERSION": "1.0",
        "ACCOUNT_NAME": "embucket",
        "CLIENT_ENVIRONMENT": {},
        "SESSION_PARAMETERS": {},
    }
}


def lambda_client(function_name: str):
    region = "us-east-2"
    if ":lambda:" in function_name:
        region = function_name.split(":")[3]

    return boto3.client(
        "lambda",
        region_name=region,
        config=Config(read_timeout=900, connect_timeout=60, retries={"max_attempts": 1}),
    )


def invoke(client, function_name: str, payload: dict) -> dict:
    response = client.invoke(FunctionName=function_name, Payload=json.dumps(payload))
    raw_body = response["Payload"].read()
    if response.get("FunctionError"):
        raise RuntimeError(f"Lambda error: {raw_body.decode()[:300]}")
    return json.loads(raw_body) if raw_body else {}


def login(client, function_name: str) -> str:
    result = invoke(
        client,
        function_name,
        {
            "version": "2.0",
            "rawPath": "/session/v1/login-request",
            "rawQueryString": "",
            "requestContext": {
                "http": {
                    "method": "POST",
                    "path": "/session/v1/login-request",
                    "sourceIp": "127.0.0.1",
                },
                "accountId": "anonymous",
                "apiId": "anonymous",
            },
            "headers": {
                "content-type": "application/json",
                "host": "localhost",
                "x-forwarded-for": "127.0.0.1",
            },
            "body": json.dumps(LOGIN_PAYLOAD),
            "isBase64Encoded": False,
        },
    )
    return json.loads(result["body"])["data"]["token"]


def run_sql(client, function_name: str, token: str, sql: str) -> dict:
    result = invoke(
        client,
        function_name,
        {
            "version": "2.0",
            "rawPath": "/queries/v1/query-request",
            "rawQueryString": f"requestId={uuid.uuid4()}",
            "requestContext": {
                "http": {
                    "method": "POST",
                    "path": "/queries/v1/query-request",
                    "sourceIp": "127.0.0.1",
                },
                "accountId": "anonymous",
                "apiId": "anonymous",
            },
            "headers": {
                "content-type": "application/json",
                "host": "localhost",
                "x-forwarded-for": "127.0.0.1",
                "authorization": f'Snowflake Token="{token}"',
            },
            "body": json.dumps({"sqlText": sql}),
            "isBase64Encoded": False,
        },
    )
    body = json.loads(result.get("body", "{}"))
    if not body.get("success"):
        raise RuntimeError(f"SQL failed: {body.get('message', 'unknown error')}")
    return body


def ensure_snowplow_schemas(client, function_name: str, token: str) -> None:
    statements = [
        "CREATE SCHEMA IF NOT EXISTS demo.atomic",
        "CREATE SCHEMA IF NOT EXISTS demo.atomic_scratch",
        "CREATE SCHEMA IF NOT EXISTS demo.atomic_derived",
        "CREATE SCHEMA IF NOT EXISTS demo.atomic_snowplow_manifest",
    ]
    for sql in statements:
        run_sql(client, function_name, token, sql)
