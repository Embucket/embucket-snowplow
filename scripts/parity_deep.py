#!/usr/bin/env python3
"""Deep parity between Embucket and Snowflake derived tables.

Compares each headline table with server-side aggregation (1 row returned
per table per engine), so Lambda payload size is not an issue:

  * COUNT(*), COUNT(DISTINCT <natural_key>)
  * MIN/MAX of start_tstamp (time range coverage)
  * SUM of a few numeric metrics (engaged_time_in_s, page_views, sessions)
  * 16 order-independent checksums: for each of 16 hex positions in the
    MD5 of each row's column-concatenated text, SUM(ASCII of that hex
    char) across all rows. A single-character divergence in any one
    column, on any row, changes the MD5 and perturbs at least one of the
    16 sums. Full agreement across all 16 sums + counts is strong
    evidence of byte-for-byte content parity.

Usage:
    uv run python scripts/parity_deep.py

Exit 0 on full parity, 1 on any divergence. Divergence output pinpoints
which check failed per table.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent))
import embucket_client  # noqa: E402
from parity import TABLES, TableSpec  # noqa: E402

EMBUCKET_DERIVED = "demo.atomic_derived"
SNOWFLAKE_DERIVED = "sturukin_db.atomic_derived"

EMBUCKET_LAMBDA_ARN = (
    "arn:aws:lambda:us-east-2:767397688925:function:"
    "embucket-demo-embucket-demo-ramp-1775514830"
)

# Aggregated metrics to check per table. (column, agg_fn)
PER_TABLE_METRICS = {
    "snowplow_web_page_views": [
        ("engaged_time_in_s", "SUM"),
        ("absolute_time_in_s", "SUM"),
        ("page_views_in_session", "SUM"),
        ("doc_width", "SUM"),
        ("doc_height", "SUM"),
    ],
    "snowplow_web_sessions": [
        ("engaged_time_in_s", "SUM"),
        ("page_views", "SUM"),
        ("total_events", "SUM"),
        ("absolute_time_in_s", "SUM"),
    ],
    "snowplow_web_users": [
        ("engaged_time_in_s", "SUM"),
        ("page_views", "SUM"),
        ("sessions", "SUM"),
    ],
}


TIMESTAMP_COLS = {
    "dvce_created_tstamp", "collector_tstamp", "derived_tstamp",
    "start_tstamp", "end_tstamp", "model_tstamp",
}


def ts_to_micros(col: str, engine: str) -> str:
    """Portable cast of a timestamp column to epoch-microseconds BIGINT."""
    if engine == "snowflake":
        return f"CAST(EXTRACT(EPOCH_MICROSECOND FROM {col}) AS BIGINT)"
    # embucket / datafusion
    return f"CAST(EXTRACT(EPOCH FROM {col}) * 1000000 AS BIGINT)"


def col_to_md5_input(col: str, engine: str) -> str:
    """Serialize one column into a canonical null-safe VARCHAR for hashing."""
    if col in TIMESTAMP_COLS:
        # Convert the timestamp to a fixed integer representation (microseconds
        # since epoch) so both engines produce byte-identical hash input.
        return f"COALESCE(CAST({ts_to_micros(col, engine)} AS VARCHAR), 'NULL')"
    return f"COALESCE(CAST({col} AS VARCHAR), 'NULL')"


def build_query(fqn: str, spec: TableSpec, engine: str) -> str:
    row_md5 = "MD5(CONCAT_WS('|', " + ", ".join(
        col_to_md5_input(c, engine) for c in spec.columns
    ) + "))"

    metric_exprs = ", ".join(
        f"{agg}({col}) AS {agg.lower()}_{col}"
        for col, agg in PER_TABLE_METRICS[spec.name]
    )

    checksum_exprs = ", ".join(
        f"SUM(ASCII(SUBSTR(row_md5, {i + 1}, 1))) AS md5_pos{i:02d}"
        for i in range(16)
    )

    # Normalize min/max start_tstamp to microseconds so the return values
    # are directly comparable as integers across engines.
    min_start = ts_to_micros("start_tstamp", engine)
    max_start = ts_to_micros("start_tstamp", engine).replace("EXTRACT", "EXTRACT")  # no-op, keep shape

    return f"""
    WITH hashed AS (
        SELECT *, {row_md5} AS row_md5
        FROM {fqn}
    )
    SELECT
        COUNT(*) AS n,
        COUNT(DISTINCT {spec.natural_key}) AS n_distinct_key,
        MIN({min_start}) AS min_start_micros,
        MAX({max_start}) AS max_start_micros,
        {metric_exprs},
        {checksum_exprs}
    FROM hashed
    """.strip()


def sf_connect():
    return snowflake.connector.connect(connection_name="default")


def sf_query(conn, sql: str) -> dict:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        row = cur.fetchone()
        return dict(zip(cols, row))
    finally:
        cur.close()


def emb_session():
    client = embucket_client.lambda_client(EMBUCKET_LAMBDA_ARN)
    token = embucket_client.login(client, EMBUCKET_LAMBDA_ARN)
    return client, token


def emb_query(client, token, sql: str) -> dict:
    body = embucket_client.run_sql(client, EMBUCKET_LAMBDA_ARN, token, sql)
    row = body["data"]["rowset"][0]
    cols = [c["name"].lower() for c in body["data"]["rowtype"]]
    return dict(zip(cols, row))


def diff_dicts(emb: dict, sf: dict) -> list[str]:
    """Return list of 'key: emb_val != sf_val' strings for diverging keys."""
    diffs = []
    keys = set(emb) | set(sf)
    for k in sorted(keys):
        e, s = emb.get(k), sf.get(k)
        # numeric coercion where useful
        try:
            if e is not None: e = str(e)
            if s is not None: s = str(s)
        except Exception:
            pass
        if e != s:
            diffs.append(f"  {k}: embucket={e!r}  snowflake={s!r}")
    return diffs


def main() -> None:
    sf_conn = sf_connect()
    emb_client, emb_token = emb_session()

    any_fail = False
    for spec in TABLES:
        emb_fqn = f"{EMBUCKET_DERIVED}.{spec.name}"
        sf_fqn = f"{SNOWFLAKE_DERIVED}.{spec.name}"

        print(f"\n=== {spec.name} ===")
        emb_sql = build_query(emb_fqn, spec, "embucket")
        sf_sql = build_query(sf_fqn, spec, "snowflake")
        try:
            emb = emb_query(emb_client, emb_token, emb_sql)
        except Exception as ex:
            print(f"  embucket query FAILED: {ex}")
            any_fail = True
            continue
        try:
            sf = sf_query(sf_conn, sf_sql)
        except Exception as ex:
            print(f"  snowflake query FAILED: {ex}")
            any_fail = True
            continue

        diffs = diff_dicts(emb, sf)
        if diffs:
            print(f"  DIVERGE ({len(diffs)} fields):")
            for d in diffs:
                print(d)
            any_fail = True
        else:
            print(f"  PARITY: n={emb.get('n')} "
                  f"n_distinct_key={emb.get('n_distinct_key')} -- "
                  "all metrics + 16 md5 checksums match")

    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()
