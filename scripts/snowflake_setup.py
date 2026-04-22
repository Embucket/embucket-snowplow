#!/usr/bin/env python3
"""Idempotent Snowflake setup for the parity harness.

Prereqs (already on this account):
  * IAM role 'snowflake-table-bucket-access' with glue:Get* permissions on
    the 'snowplow' federated catalog and s3tables:* on the bucket. Versioned
    inline in specs/2026-04-22-embucket-snowflake-parity-design.md.

This script:
  1. Creates catalog integration SNOWPLOW_S3T (REST/Glue federated) if missing.
  2. Creates database sturukin_db / schema atomic if missing.
  3. (Re)creates the managed Iceberg table sturukin_db.atomic.events_0416
     pointing at s3tablescatalog/snowplow.atomic.events_0416.

The Lake Formation DESCRIBE+SELECT grant on the source table is assumed to be
in place (granted manually to snowflake-table-bucket-access). If the CREATE
ICEBERG TABLE step fails with a Lake Formation permission error, run:

    aws lakeformation grant-permissions \\
      --principal DataLakePrincipalIdentifier=arn:aws:iam::767397688925:role/snowflake-table-bucket-access \\
      --resource '{"Table":{"CatalogId":"767397688925:s3tablescatalog/snowplow","DatabaseName":"atomic","Name":"events_0416"}}' \\
      --permissions DESCRIBE SELECT

Usage:
    uv run python scripts/snowflake_setup.py
"""

from __future__ import annotations

import argparse

import snowflake.connector

DATABASE = "sturukin_db"
SCHEMA = "atomic"
ICEBERG_TABLE = "events_0416"
CATALOG_INTEGRATION_NAME = "SNOWPLOW_S3T"
CATALOG_NAME = "767397688925:s3tablescatalog/snowplow"
SIGV4_ROLE = "arn:aws:iam::767397688925:role/snowflake-table-bucket-access"


def exec_fetchall(conn, sql: str):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall() if cur.description else []
    finally:
        cur.close()


def integration_exists(conn) -> bool:
    rows = exec_fetchall(conn, "SHOW CATALOG INTEGRATIONS")
    return any(r[0].upper() == CATALOG_INTEGRATION_NAME for r in rows)


def ensure_integration(conn):
    if integration_exists(conn):
        print(f"  catalog integration {CATALOG_INTEGRATION_NAME} already exists -- reusing")
        return
    print(f"  creating catalog integration {CATALOG_INTEGRATION_NAME}...")
    exec_fetchall(conn, f"""
        CREATE CATALOG INTEGRATION {CATALOG_INTEGRATION_NAME}
          CATALOG_SOURCE = ICEBERG_REST
          CATALOG_NAMESPACE = '{SCHEMA}'
          TABLE_FORMAT = ICEBERG
          REST_CONFIG = (
            CATALOG_URI = 'https://glue.us-east-2.amazonaws.com/iceberg'
            CATALOG_API_TYPE = AWS_GLUE
            CATALOG_NAME = '{CATALOG_NAME}'
            ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
          )
          REST_AUTHENTICATION = (
            TYPE = SIGV4
            SIGV4_IAM_ROLE = '{SIGV4_ROLE}'
            SIGV4_SIGNING_REGION = 'us-east-2'
          )
          ENABLED = TRUE
    """)


def ensure_db_schema(conn):
    exec_fetchall(conn, f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    exec_fetchall(conn, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}")


def recreate_iceberg_table(conn):
    fqn = f"{DATABASE}.{SCHEMA}.{ICEBERG_TABLE}"
    print(f"  (re)creating iceberg table {fqn}...")
    exec_fetchall(conn, f"""
        CREATE OR REPLACE ICEBERG TABLE {fqn}
          CATALOG = '{CATALOG_INTEGRATION_NAME}'
          CATALOG_TABLE_NAME = '{ICEBERG_TABLE}'
          AUTO_REFRESH = FALSE
    """)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection", default="default",
                        help="snow CLI connection name (default: default)")
    args = parser.parse_args()

    conn = snowflake.connector.connect(connection_name=args.connection)
    try:
        print("== Snowflake parity setup ==")
        ensure_integration(conn)
        ensure_db_schema(conn)
        recreate_iceberg_table(conn)
        count = exec_fetchall(conn, f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{ICEBERG_TABLE}")[0][0]
        print(f"\n{DATABASE}.{SCHEMA}.{ICEBERG_TABLE} row count after setup: {count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
