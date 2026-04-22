#!/usr/bin/env python3
"""Compare Embucket and Snowflake outputs of dbt-snowplow-web.

For each of snowplow_web_page_views, snowplow_web_sessions, snowplow_web_users:
  * rowcount on both sides must match
  * MD5 hash of every row (by the model's declared column list) must match
    when keyed by the table's natural key

Exits non-zero on any mismatch. Prints up to 10 example natural keys per
mismatch bucket.

Usage:
    uv run python scripts/parity.py               # full check
    uv run python scripts/parity.py --source-only # just events_0416 rowcount + schema
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass, field
from pathlib import Path

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent))
import embucket_client  # noqa: E402

EMBUCKET_DERIVED = "demo.atomic_derived"
SNOWFLAKE_DERIVED = "sturukin_db.atomic_derived"

EMBUCKET_LAMBDA_ARN = (
    "arn:aws:lambda:us-east-2:767397688925:function:"
    "embucket-demo-embucket-demo-ramp-1775514830"
)


@dataclass
class TableSpec:
    name: str
    natural_key: str
    columns: list[str]


TABLES = [
    TableSpec(
        name="snowplow_web_page_views",
        natural_key="page_view_id",
        columns=[
            "page_view_id", "event_id", "app_id", "user_id", "domain_userid",
            "stitched_user_id", "network_userid", "domain_sessionid",
            "domain_sessionidx", "page_view_in_session_index",
            "page_views_in_session", "dvce_created_tstamp", "collector_tstamp",
            "derived_tstamp", "start_tstamp", "end_tstamp", "model_tstamp",
            "engaged_time_in_s", "absolute_time_in_s",
            "horizontal_pixels_scrolled", "vertical_pixels_scrolled",
            "horizontal_percentage_scrolled", "vertical_percentage_scrolled",
            "doc_width", "doc_height", "page_title", "page_url",
            "page_urlscheme", "page_urlhost", "page_urlpath", "page_urlquery",
            "page_urlfragment", "mkt_medium", "mkt_source", "mkt_term",
            "mkt_content", "mkt_campaign", "mkt_clickid", "mkt_network",
            "page_referrer", "refr_urlscheme", "refr_urlhost", "refr_urlpath",
            "refr_urlquery", "refr_urlfragment", "refr_medium", "refr_source",
            "refr_term", "geo_country", "geo_region", "geo_region_name",
            "geo_city", "geo_zipcode", "geo_latitude", "geo_longitude",
            "geo_timezone", "user_ipaddress", "useragent", "br_lang",
            "br_viewwidth", "br_viewheight", "br_colordepth", "br_renderengine",
            "os_timezone",
        ],
    ),
    TableSpec(
        name="snowplow_web_sessions",
        natural_key="domain_sessionid",
        columns=[
            "app_id", "domain_sessionid", "domain_sessionidx", "start_tstamp",
            "end_tstamp", "model_tstamp", "user_id", "domain_userid",
            "stitched_user_id", "network_userid", "page_views",
            "engaged_time_in_s", "total_events", "is_engaged",
            "absolute_time_in_s", "first_page_title", "first_page_url",
            "last_page_title", "last_page_url", "referrer",
            "geo_country", "geo_region", "geo_city", "geo_timezone",
            "user_ipaddress", "useragent", "br_lang",
        ],
    ),
    TableSpec(
        name="snowplow_web_users",
        natural_key="domain_userid",
        columns=[
            "user_id", "domain_userid", "network_userid", "start_tstamp",
            "end_tstamp", "model_tstamp", "page_views", "sessions",
            "engaged_time_in_s", "first_page_title", "first_page_url",
            "first_geo_country", "first_geo_city",
            "last_page_title", "last_page_url", "last_geo_country",
            "last_geo_city", "referrer",
        ],
    ),
]


@dataclass
class DiffResult:
    matched: int
    mismatched: list[str] = field(default_factory=list)
    only_left: list[str] = field(default_factory=list)
    only_right: list[str] = field(default_factory=list)


def row_hash(values: list) -> str:
    """Hash a row's values deterministically, distinguishing NULL from string."""
    parts = []
    for v in values:
        if v is None:
            parts.append("\x00NULL\x00")
        else:
            parts.append(str(v))
    joined = "\x01".join(parts)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def diff_sides(left: dict, right: dict) -> DiffResult:
    matched = 0
    mismatched: list[str] = []
    only_left: list[str] = []
    only_right: list[str] = []
    for key, lhash in left.items():
        if key not in right:
            only_left.append(key)
        elif right[key] != lhash:
            mismatched.append(key)
        else:
            matched += 1
    for key in right:
        if key not in left:
            only_right.append(key)
    return DiffResult(matched=matched, mismatched=mismatched,
                      only_left=only_left, only_right=only_right)


# --- DB access (integration-level, not unit-tested) --------------------------

def sf_connect():
    return snowflake.connector.connect(connection_name="default")


def emb_session():
    client = embucket_client.lambda_client(EMBUCKET_LAMBDA_ARN)
    token = embucket_client.login(client, EMBUCKET_LAMBDA_ARN)
    return client, token


def sf_rowcount(conn, fqn: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {fqn}")
        return int(cur.fetchone()[0])
    finally:
        cur.close()


def emb_rowcount(client, token, fqn: str) -> int:
    body = embucket_client.run_sql(client, EMBUCKET_LAMBDA_ARN, token,
                                   f"SELECT COUNT(*) FROM {fqn}")
    return int(body["data"]["rowset"][0][0])


def sf_hashes(conn, fqn: str, spec: TableSpec) -> dict:
    cols = ", ".join(spec.columns)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT {spec.natural_key}, {cols} FROM {fqn}")
        return {row[0]: row_hash(list(row[1:])) for row in cur.fetchall()}
    finally:
        cur.close()


def emb_hashes(client, token, fqn: str, spec: TableSpec) -> dict:
    cols = ", ".join(spec.columns)
    body = embucket_client.run_sql(
        client, EMBUCKET_LAMBDA_ARN, token,
        f"SELECT {spec.natural_key}, {cols} FROM {fqn}",
    )
    out = {}
    for row in body["data"]["rowset"]:
        out[row[0]] = row_hash(list(row[1:]))
    return out


# --- Orchestration ----------------------------------------------------------

def print_diff(spec: TableSpec, diff: DiffResult) -> None:
    print(f"  matched:     {diff.matched}")
    print(f"  mismatched:  {len(diff.mismatched)}")
    print(f"  only_embucket:  {len(diff.only_left)}")
    print(f"  only_snowflake: {len(diff.only_right)}")
    for bucket_name, keys in [
        ("mismatched", diff.mismatched),
        ("only_embucket", diff.only_left),
        ("only_snowflake", diff.only_right),
    ]:
        if keys:
            sample = keys[:10]
            print(f"  first {len(sample)} {bucket_name} {spec.natural_key}:")
            for k in sample:
                print(f"    {k}")


def run(source_only: bool) -> int:
    any_fail = False
    sf_conn = sf_connect()
    emb_client, emb_token = emb_session()

    emb_src = emb_rowcount(emb_client, emb_token, "demo.atomic.events_0416")
    sf_src = sf_rowcount(sf_conn, "sturukin_db.atomic.events_0416")
    print(f"source events_0416:  embucket={emb_src}  snowflake={sf_src}")
    if emb_src != sf_src:
        print("  FAIL: source rowcount mismatch")
        any_fail = True

    if source_only:
        return 1 if any_fail else 0

    for spec in TABLES:
        emb_fqn = f"{EMBUCKET_DERIVED}.{spec.name}"
        sf_fqn = f"{SNOWFLAKE_DERIVED}.{spec.name}"
        emb_count = emb_rowcount(emb_client, emb_token, emb_fqn)
        sf_count = sf_rowcount(sf_conn, sf_fqn)
        print(f"\n{spec.name}:  embucket={emb_count}  snowflake={sf_count}")
        if emb_count != sf_count:
            any_fail = True
        emb = emb_hashes(emb_client, emb_token, emb_fqn, spec)
        sf = sf_hashes(sf_conn, sf_fqn, spec)
        diff = diff_sides(emb, sf)
        print_diff(spec, diff)
        if diff.mismatched or diff.only_left or diff.only_right:
            any_fail = True

    return 1 if any_fail else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-only", action="store_true",
                        help="Only diff events_0416 source rowcount, skip derived tables")
    args = parser.parse_args()
    sys.exit(run(args.source_only))


if __name__ == "__main__":
    main()
