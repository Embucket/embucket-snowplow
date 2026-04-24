#!/usr/bin/env python3
"""Snowflake-only two-schema parity check.

Diffs two Snowflake schemas containing the three Snowplow golden tables.
Same rowcount + MD5-row-hash methodology as parity.py, but both sides are
Snowflake, and `model_tstamp` is excluded from every hash (it is
current_timestamp() at dbt run time and will differ by design).

Usage:
    uv run python scripts/parity_self.py \\
        --left  sturukin_db.atomic_derived_baseline_fr \\
        --right sturukin_db.atomic_derived_patched_fr
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass, field

import snowflake.connector


@dataclass
class TableSpec:
    name: str
    natural_key: str
    columns: list[str]


# Columns mirror parity.py but with model_tstamp removed.
TABLES = [
    TableSpec(
        name="snowplow_web_page_views",
        natural_key="page_view_id",
        columns=[
            "page_view_id", "event_id", "app_id", "user_id", "domain_userid",
            "stitched_user_id", "network_userid", "domain_sessionid",
            "domain_sessionidx", "page_view_in_session_index",
            "page_views_in_session", "dvce_created_tstamp", "collector_tstamp",
            "derived_tstamp", "start_tstamp", "end_tstamp",
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
            "end_tstamp", "user_id", "domain_userid",
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
            "end_tstamp", "page_views", "sessions",
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
    parts = []
    for v in values:
        if v is None:
            parts.append("\x00NULL\x00")
        else:
            parts.append(str(v))
    return hashlib.md5("\x01".join(parts).encode("utf-8")).hexdigest()


def fetch_hashes(conn, fqn: str, spec: TableSpec) -> dict:
    cols = ", ".join(spec.columns)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT {spec.natural_key}, {cols} FROM {fqn}")
        return {row[0]: row_hash(list(row[1:])) for row in cur.fetchall()}
    finally:
        cur.close()


def rowcount(conn, fqn: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {fqn}")
        return int(cur.fetchone()[0])
    finally:
        cur.close()


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


def print_diff(spec: TableSpec, diff: DiffResult, left_label: str,
               right_label: str) -> None:
    print(f"  matched:     {diff.matched}")
    print(f"  mismatched:  {len(diff.mismatched)}")
    print(f"  only_{left_label}:  {len(diff.only_left)}")
    print(f"  only_{right_label}: {len(diff.only_right)}")
    for bucket_name, keys in [
        ("mismatched", diff.mismatched),
        (f"only_{left_label}", diff.only_left),
        (f"only_{right_label}", diff.only_right),
    ]:
        if keys:
            sample = keys[:10]
            print(f"  first {len(sample)} {bucket_name} {spec.natural_key}:")
            for k in sample:
                print(f"    {k}")


def run(left_schema: str, right_schema: str) -> int:
    any_fail = False
    conn = snowflake.connector.connect(connection_name="default")
    try:
        for spec in TABLES:
            left_fqn = f"{left_schema}.{spec.name}"
            right_fqn = f"{right_schema}.{spec.name}"
            lc = rowcount(conn, left_fqn)
            rc = rowcount(conn, right_fqn)
            print(f"\n{spec.name}:  left={lc}  right={rc}")
            if lc != rc:
                print("  ROWCOUNT DIFFERS -- skipping hash diff")
                any_fail = True
                continue
            if lc == 0:
                print("  both sides empty; nothing to hash")
                continue
            lh = fetch_hashes(conn, left_fqn, spec)
            rh = fetch_hashes(conn, right_fqn, spec)
            diff = diff_sides(lh, rh)
            print_diff(spec, diff, "left", "right")
            if diff.mismatched or diff.only_left or diff.only_right:
                any_fail = True
    finally:
        conn.close()
    return 1 if any_fail else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left", required=True,
                        help="Left-side database.schema")
    parser.add_argument("--right", required=True,
                        help="Right-side database.schema")
    args = parser.parse_args()
    sys.exit(run(args.left, args.right))


if __name__ == "__main__":
    main()
