# Embucket vs Snowflake parity harness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run dbt-snowplow-web on both Embucket and Snowflake against the same S3 Tables Iceberg source, loaded in two 30-minute Athena batches, and diff the three headline derived tables after each `dbt run` to surface semantic drift.

**Architecture:** Single dbt project, two targets (`embucket` default, `snowflake` added) under the existing `embucket_demo` profile. Both read `atomic.events_0416` — Embucket natively; Snowflake via a Snowflake-managed Iceberg table over S3 Tables. Athena CTAS is split into idempotent `init` (empty CREATE TABLE via CTAS-with-WHERE-1=0) + parametrized `insert` (INSERT INTO with load_tstamp range) so the source can be batch-loaded. Parity script rowcount- and MD5-diffs `snowplow_web_page_views`, `snowplow_web_sessions`, `snowplow_web_users` on both sides.

**Tech Stack:** Python 3.10+ (uv), boto3 (Athena), snowflake-connector-python, dbt-core, dbt-embucket, dbt-snowflake, Athena SQL (Iceberg/Trino), Snowflake SQL.

**Reference spec:** `specs/2026-04-22-embucket-snowflake-parity-design.md`.

---

## Task 1: Bump `snowplow__start_date` to match the fresh data window

**Files:**
- Modify: `dbt_project.yml`

- [ ] **Step 1: Edit `dbt_project.yml`**

Replace the line `  snowplow__start_date: '2026-04-13'` with:

```yaml
  snowplow__start_date: '2026-04-22'
```

- [ ] **Step 2: Verify**

Run: `grep snowplow__start_date dbt_project.yml`
Expected: `  snowplow__start_date: '2026-04-22'`

- [ ] **Step 3: Commit**

```bash
git add dbt_project.yml
git commit -m "chore: bump snowplow__start_date to 2026-04-22 for parity batches"
```

---

## Task 2: Add `dbt-snowflake` and `snowflake-connector-python` dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Edit `pyproject.toml`**

Replace the `dependencies = [...]` block with:

```toml
dependencies = [
    "boto3>=1.34.0",
    "dbt-core>=1.11.0,<2.0",
    "dbt-embucket>=0.1.2",
    "dbt-snowflake>=1.8.0,<2.0",
    "fastavro>=1.9.0",
    "pyarrow>=20.0.0",
    "s3fs>=2025.3.0",
    "snowflake-connector-python>=3.7.0",
]
```

- [ ] **Step 2: Sync dependencies**

Run: `uv sync`
Expected: exits 0; `uv.lock` updated; `dbt-snowflake` and `snowflake-connector-python` present in `uv.lock`.

- [ ] **Step 3: Verify dbt-snowflake is importable**

Run: `uv run python -c "import dbt.adapters.snowflake; print(dbt.adapters.snowflake.__version__)"`
Expected: a version string (e.g. `1.8.x` or later). No errors.

- [ ] **Step 4: Verify snowflake-connector-python is importable**

Run: `uv run python -c "import snowflake.connector; print(snowflake.connector.__version__)"`
Expected: a version string. No errors.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "build: add dbt-snowflake and snowflake-connector-python deps"
```

---

## Task 3: Extract the projection SELECT into a shared SQL file

The current `scripts/ctas_from_glue.sql` couples `CREATE TABLE AS` with the projection and a hardcoded filter. Split so `init` and `insert` reuse one SELECT body.

**Files:**
- Create: `scripts/events_0416_select.sql`

- [ ] **Step 1: Create `scripts/events_0416_select.sql`**

Copy the body of the existing `scripts/ctas_from_glue.sql` **starting at the `SELECT` keyword** (line 30 in the current file) through the `FROM "awsdatacatalog"."analytics_glue"."hooli_events_0417_v2"` line. Do **not** include the `CREATE TABLE events_0416 WITH (...) AS` header or the existing `WHERE load_tstamp >= TIMESTAMP '2026-04-17 19:47:00 UTC'` filter. After the `FROM` line, append a single placeholder line:

```sql
WHERE {{where}}
```

The resulting file starts with `SELECT` and ends with `WHERE {{where}}`. Preserve all casts exactly as in `ctas_from_glue.sql`. This is the canonical projection Python will wrap at render time.

- [ ] **Step 2: Verify line count and boundaries**

Run:
```bash
head -1 scripts/events_0416_select.sql
tail -2 scripts/events_0416_select.sql
wc -l scripts/events_0416_select.sql
```
Expected: first line starts with `SELECT`; last two lines are the `FROM "awsdatacatalog"...` line and `WHERE {{where}}`; line count roughly 145 (the current CTAS body minus the 4-line CREATE TABLE header and the filter lines, plus the 1-line placeholder).

- [ ] **Step 3: Commit**

```bash
git add scripts/events_0416_select.sql
git commit -m "refactor: extract events_0416 projection SELECT into reusable file"
```

---

## Task 4: Refactor `scripts/load_from_glue.py` into `init` / `insert` subcommands

Replace the single CTAS flow with two idempotent subcommands that share the projection from Task 3.

**Files:**
- Replace: `scripts/load_from_glue.py`
- Delete: `scripts/ctas_from_glue.sql`

- [ ] **Step 1: Replace `scripts/load_from_glue.py` with the subcommand version**

Overwrite the file with:

```python
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
```

- [ ] **Step 2: Delete the old `ctas_from_glue.sql`**

Run: `git rm scripts/ctas_from_glue.sql`

- [ ] **Step 3: Smoke-test `--help`**

Run: `uv run python scripts/load_from_glue.py --help`
Expected: usage text showing `init` and `insert` subcommands.

Run: `uv run python scripts/load_from_glue.py insert --help`
Expected: shows required `--start` and `--end` args.

- [ ] **Step 4: Smoke-test `init` against the real Athena**

Run: `uv run python scripts/load_from_glue.py init`
Expected: prints "Dropping ... / Creating empty ... / Done. ... row count: 0". Takes 30-90s. On failure, inspect the printed Athena query id in the Athena console.

- [ ] **Step 5: Verify table exists and is empty**

Run:
```bash
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM \"s3tablescatalog/snowplow\".\"atomic\".\"events_0416\"" \
  --work-group primary \
  --result-configuration OutputLocation=s3://athena-query-results-us-east-2-767397688925/embucket-snowplow/ \
  --query-execution-context Database=atomic \
  --query 'QueryExecutionId' --output text
```
Wait a few seconds, then `aws athena get-query-results --query-execution-id <id>`. Expected: count = 0.

- [ ] **Step 6: Smoke-test `insert` with a tiny window (5 minutes)**

Run:
```bash
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:00:00' --end '2026-04-22 15:05:00'
```
Expected: prints a non-zero row count on completion (roughly 500K rows for 5 min). If it fails with a schema-mismatch error, the SELECT projection in `events_0416_select.sql` is out of sync with the CREATE's inferred schema — re-run Task 3 and confirm only the `CREATE TABLE ... AS` header and the WHERE line were modified.

- [ ] **Step 7: Re-run `init` to reset**

Run: `uv run python scripts/load_from_glue.py init`
Expected: row count 0.

- [ ] **Step 8: Commit**

```bash
git add scripts/load_from_glue.py scripts/ctas_from_glue.sql
git commit -m "refactor: split load_from_glue into init/insert subcommands"
```

---

## Task 5: Add `snowflake` output to `profiles.yml.example` and local `profiles.yml`

**Files:**
- Modify: `profiles.yml.example`
- Modify: `profiles.yml` (gitignored, not committed)

- [ ] **Step 1: Edit `profiles.yml.example`**

Replace its contents with:

```yaml
embucket_demo:
  target: dev
  outputs:
    dev:
      type: embucket
      function_arn: "YOUR_LAMBDA_ARN_HERE"
      account: "embucket"
      user: "demo_user"
      password: "demo_password_2026"
      database: "demo"
      schema: "atomic"
      threads: 1
    snowflake:
      type: snowflake
      account: YOUR_SNOWFLAKE_ACCOUNT
      user: YOUR_SNOWFLAKE_USER
      password: YOUR_SNOWFLAKE_PASSWORD
      role: YOUR_SNOWFLAKE_ROLE
      warehouse: YOUR_SNOWFLAKE_WAREHOUSE
      database: sturukin
      schema: atomic
      threads: 4
```

Note: the previous `dev` output is kept; the new `snowflake` output is added as a sibling so `dbt run --target snowflake` works alongside the default `dev` (Embucket) target.

- [ ] **Step 2: Edit local `profiles.yml` with real values**

Append a `snowflake:` block under `outputs:` in the gitignored `profiles.yml` so it reads:

```yaml
embucket_demo:
  target: dev
  outputs:
    dev:
      type: embucket
      function_arn: "arn:aws:lambda:us-east-2:767397688925:function:embucket-demo-embucket-demo-ramp-1775514830"
      account: "embucket"
      user: "demo_user"
      password: "demo_password_2026"
      database: "demo"
      schema: "atomic"
      threads: 1
    snowflake:
      type: snowflake
      account: aa06228.us-east-2.aws
      user: rampage644
      password: "9i8u7y6T?"
      role: ACCOUNTADMIN
      warehouse: compute_wh
      database: sturukin
      schema: atomic
      threads: 4
```

(Existing `dev` block's `function_arn` is preserved — only the `snowflake` block is added.)

- [ ] **Step 3: Verify `profiles.yml` is gitignored**

Run: `git check-ignore profiles.yml`
Expected: `profiles.yml` (the file is ignored — no accidental credential commit).

- [ ] **Step 4: Verify dbt sees both targets**

Run: `uv run dbt debug --profiles-dir . --target snowflake`
Expected: "All checks passed!" including connection to Snowflake. (If the Snowflake `sturukin` database does not exist yet, dbt debug may still pass the auth check but fail the "database exists" check — that is fine; Task 7 creates it.)

- [ ] **Step 5: Commit only the example file**

```bash
git add profiles.yml.example
git commit -m "chore: add snowflake output template to profiles.yml.example"
```

---

## Task 6: Write `scripts/parity.py` — the diff harness (TDD)

Pure hash/diff logic is unit-tested; the DB-connection layer is a thin wrapper exercised in the end-to-end task.

**Files:**
- Create: `scripts/parity.py`
- Create: `tests/test_parity.py`
- Create: `tests/__init__.py`

- [ ] **Step 1: Create empty test package marker**

Run: `touch tests/__init__.py`

- [ ] **Step 2: Write the failing test for `diff_sides`**

Create `tests/test_parity.py`:

```python
"""Unit tests for the pure diff logic in scripts/parity.py."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from parity import DiffResult, diff_sides, row_hash


def test_diff_sides_all_match():
    left = {"k1": "h1", "k2": "h2"}
    right = {"k1": "h1", "k2": "h2"}
    result = diff_sides(left, right)
    assert result == DiffResult(
        matched=2, mismatched=[], only_left=[], only_right=[]
    )


def test_diff_sides_hash_mismatch():
    left = {"k1": "h1", "k2": "h2"}
    right = {"k1": "h1", "k2": "DIFFERENT"}
    result = diff_sides(left, right)
    assert result.matched == 1
    assert result.mismatched == ["k2"]
    assert result.only_left == []
    assert result.only_right == []


def test_diff_sides_only_in_left():
    left = {"k1": "h1", "k2": "h2"}
    right = {"k1": "h1"}
    result = diff_sides(left, right)
    assert result.matched == 1
    assert result.only_left == ["k2"]
    assert result.only_right == []


def test_diff_sides_only_in_right():
    left = {"k1": "h1"}
    right = {"k1": "h1", "k2": "h2"}
    result = diff_sides(left, right)
    assert result.matched == 1
    assert result.only_left == []
    assert result.only_right == ["k2"]


def test_diff_sides_empty_both_sides():
    result = diff_sides({}, {})
    assert result == DiffResult(matched=0, mismatched=[], only_left=[], only_right=[])


def test_row_hash_deterministic_and_null_handling():
    h1 = row_hash(["a", None, 1, 2.5])
    h2 = row_hash(["a", None, 1, 2.5])
    assert h1 == h2
    # None is distinguishable from the string 'None'
    assert row_hash([None]) != row_hash(["None"])
    # Order matters
    assert row_hash(["a", "b"]) != row_hash(["b", "a"])
```

- [ ] **Step 3: Run the test — it must fail with an import error**

Run: `uv run python -m pytest tests/test_parity.py -v`
Expected: FAILS with `ModuleNotFoundError: No module named 'parity'` (the file doesn't exist yet).

- [ ] **Step 4: Create `scripts/parity.py` with the pure logic**

```python
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

# dbt default schemas: <profile schema>_derived
EMBUCKET_DERIVED = "demo.atomic_derived"
SNOWFLAKE_DERIVED = "sturukin.atomic_derived"

EMBUCKET_LAMBDA_ARN = (
    "arn:aws:lambda:us-east-2:767397688925:function:"
    "embucket-demo-embucket-demo-ramp-1775514830"
)


@dataclass
class TableSpec:
    name: str
    natural_key: str
    columns: list[str]


# Column lists come from the dbt-snowplow-web model yaml files.
# Keep in sync with dbt_packages/snowplow_web/models/<area>/<area>.yml.
# (Comparison uses the intersection of columns present on both engines at
# runtime — see fetch_rows — so extra engine-specific metadata columns like
# _dbt_inserted_at do not break the diff.)
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

    # Source-level check: events_0416 rowcount and (implicit) presence.
    emb_src = emb_rowcount(emb_client, emb_token, "demo.atomic.events_0416")
    sf_src = sf_rowcount(sf_conn, "sturukin.atomic.events_0416")
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
```

- [ ] **Step 5: Run the tests — they must pass**

Run: `uv run python -m pytest tests/test_parity.py -v`
Expected: 6 passed.

- [ ] **Step 6: Smoke-test `--help`**

Run: `uv run python scripts/parity.py --help`
Expected: usage text including `--source-only`. No import errors.

- [ ] **Step 7: Commit**

```bash
git add scripts/parity.py tests/__init__.py tests/test_parity.py
git commit -m "feat: add parity.py — rowcount + MD5 diff across Embucket and Snowflake"
```

---

## Task 7: Write `scripts/snowflake_setup.py`

One-shot idempotent setup of the Snowflake external volume, catalog integration, database/schema, and the Iceberg table reading `events_0416` from the S3 Tables bucket.

**Files:**
- Create: `scripts/snowflake_setup.py`

- [ ] **Step 1: Create `scripts/snowflake_setup.py`**

```python
#!/usr/bin/env python3
"""Idempotent Snowflake setup for the parity harness.

Probes for existing external volume and catalog integration; creates them if
missing; ensures sturukin.atomic exists; (re)creates the managed Iceberg
table pointing at the S3 Tables bucket's atomic.events_0416 table.

Usage:
    uv run python scripts/snowflake_setup.py
"""

from __future__ import annotations

import argparse
import sys

import snowflake.connector

DATABASE = "sturukin"
SCHEMA = "atomic"
ICEBERG_TABLE = "events_0416"

# S3 Tables bucket Embucket is volumed to.
S3_TABLES_ARN = "arn:aws:s3tables:us-east-2:767397688925:bucket/snowplow"
NAMESPACE = "atomic"  # S3 Tables namespace inside the bucket
EXTERNAL_VOLUME_NAME = "snowplow_vol"
CATALOG_INTEGRATION_NAME = "snowplow_s3t"


def exec_one(conn, sql: str):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall() if cur.description else []
    finally:
        cur.close()


def volume_exists(conn) -> bool:
    rows = exec_one(conn, "SHOW EXTERNAL VOLUMES")
    names = [r[0] for r in rows]  # column 0 is the volume name
    return any(n.upper() == EXTERNAL_VOLUME_NAME.upper() for n in names)


def integration_exists(conn) -> bool:
    rows = exec_one(conn, "SHOW CATALOG INTEGRATIONS")
    names = [r[0] for r in rows]
    return any(n.upper() == CATALOG_INTEGRATION_NAME.upper() for n in names)


def ensure_volume(conn):
    if volume_exists(conn):
        print(f"  external volume {EXTERNAL_VOLUME_NAME} already exists — reusing")
        return
    print(f"  creating external volume {EXTERNAL_VOLUME_NAME}...")
    exec_one(conn, f"""
        CREATE EXTERNAL VOLUME {EXTERNAL_VOLUME_NAME}
        STORAGE_LOCATIONS = (
          (
            NAME = 's3tables_snowplow'
            STORAGE_PROVIDER = 'S3TABLES'
            STORAGE_BASE_URL = 's3tables://{S3_TABLES_ARN}'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767397688925:role/SnowflakeS3TablesAccessRole'
          )
        )
        ALLOW_WRITES = FALSE
    """)
    # After creation, fetch DESC output so the operator can copy the IAM user
    # Snowflake wants trusted; if the trust policy is already in place this is a
    # no-op, but the operator may need to paste STORAGE_AWS_IAM_USER_ARN and
    # STORAGE_AWS_EXTERNAL_ID into the role's trust policy.
    desc = exec_one(conn, f"DESC EXTERNAL VOLUME {EXTERNAL_VOLUME_NAME}")
    print("  DESC EXTERNAL VOLUME output (copy STORAGE_AWS_IAM_USER_ARN / "
          "STORAGE_AWS_EXTERNAL_ID into the target role's trust policy if not already):")
    for row in desc:
        print(f"    {row}")


def ensure_integration(conn):
    if integration_exists(conn):
        print(f"  catalog integration {CATALOG_INTEGRATION_NAME} already exists — reusing")
        return
    print(f"  creating catalog integration {CATALOG_INTEGRATION_NAME}...")
    exec_one(conn, f"""
        CREATE CATALOG INTEGRATION {CATALOG_INTEGRATION_NAME}
          CATALOG_SOURCE = ICEBERG_REST
          TABLE_FORMAT = ICEBERG
          CATALOG_NAMESPACE = '{NAMESPACE}'
          REST_CONFIG = (
            CATALOG_URI = 'https://s3tables.us-east-2.amazonaws.com/iceberg'
            CATALOG_NAME = 's3tablescatalog/snowplow'
          )
          REST_AUTHENTICATION = (
            TYPE = SIGV4
            SIGV4_IAM_ROLE = 'arn:aws:iam::767397688925:role/SnowflakeS3TablesAccessRole'
            SIGV4_SIGNING_REGION = 'us-east-2'
          )
          ENABLED = TRUE
    """)


def ensure_db_schema(conn):
    exec_one(conn, f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    exec_one(conn, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}")
    # dbt creates its derived/scratch/manifest schemas on first run; no-op here.


def recreate_iceberg_table(conn):
    fqn = f"{DATABASE}.{SCHEMA}.{ICEBERG_TABLE}"
    print(f"  (re)creating iceberg table {fqn}...")
    exec_one(conn, f"""
        CREATE OR REPLACE ICEBERG TABLE {fqn}
          EXTERNAL_VOLUME = '{EXTERNAL_VOLUME_NAME}'
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
        ensure_volume(conn)
        ensure_integration(conn)
        ensure_db_schema(conn)
        recreate_iceberg_table(conn)

        count = exec_one(conn, f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{ICEBERG_TABLE}")[0][0]
        print(f"\n{DATABASE}.{SCHEMA}.{ICEBERG_TABLE} row count after setup: {count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-test `--help`**

Run: `uv run python scripts/snowflake_setup.py --help`
Expected: usage text. No import errors.

- [ ] **Step 3: Run against the real Snowflake account**

Run: `uv run python scripts/snowflake_setup.py`

Expected outcomes, in order of likelihood:
  1. **Success on first run**: volume + integration get created (or reused), iceberg table is created, row count prints (matches current `events_0416` state — 0 if Task 4 Step 7 reset it).
  2. **Failure at `ensure_volume` with a trust-policy error**: the `SnowflakeS3TablesAccessRole` IAM role does not exist or does not trust Snowflake. Two fallbacks:
     - If an external volume already exists under a different name for this bucket, rename `EXTERNAL_VOLUME_NAME` in the script to match (reuses it) and re-run. `SHOW EXTERNAL VOLUMES` output from the failure lists candidates.
     - If no existing volume or role, stop — IAM role creation is outside the scope of this harness. Document as a prerequisite and revisit.
  3. **Failure at `ensure_integration` with `CATALOG_SOURCE` / `TABLE_FORMAT` unsupported**: the Snowflake account does not have S3 Tables REST integration enabled. Fall back to Glue-federated integration by replacing the `ensure_integration` body with:
     ```python
     exec_one(conn, f"""
         CREATE CATALOG INTEGRATION {CATALOG_INTEGRATION_NAME}
           CATALOG_SOURCE = GLUE
           CATALOG_NAMESPACE = 's3tablescatalog/snowplow.atomic'
           TABLE_FORMAT = ICEBERG
           GLUE_AWS_ROLE_ARN = 'arn:aws:iam::767397688925:role/SnowflakeS3TablesAccessRole'
           GLUE_CATALOG_ID = '767397688925'
           GLUE_REGION = 'us-east-2'
           ENABLED = TRUE
     """)
     ```
     and re-run.

On success, verify by running:
```bash
uv run python -c "
import snowflake.connector
c = snowflake.connector.connect(connection_name='default')
cur = c.cursor()
cur.execute('SELECT COUNT(*) FROM sturukin.atomic.events_0416')
print(cur.fetchone())
"
```
Expected: the same count printed by the setup script.

- [ ] **Step 4: Commit**

```bash
git add scripts/snowflake_setup.py
git commit -m "feat: add snowflake_setup.py — external volume, catalog integration, iceberg table"
```

---

## Task 8: Write `scripts/snowflake_refresh.py`

**Files:**
- Create: `scripts/snowflake_refresh.py`

- [ ] **Step 1: Create `scripts/snowflake_refresh.py`**

```python
#!/usr/bin/env python3
"""Refresh the Snowflake Iceberg view of atomic.events_0416 after an Athena write.

Snowflake-managed Iceberg tables over externally-written data do not
auto-refresh; this script issues ALTER ICEBERG TABLE ... REFRESH and prints
the post-refresh row count.

Usage:
    uv run python scripts/snowflake_refresh.py
"""

from __future__ import annotations

import argparse

import snowflake.connector

FQN = "sturukin.atomic.events_0416"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection", default="default")
    args = parser.parse_args()

    conn = snowflake.connector.connect(connection_name=args.connection)
    try:
        cur = conn.cursor()
        print(f"Refreshing {FQN}...")
        cur.execute(f"ALTER ICEBERG TABLE {FQN} REFRESH")
        cur.execute(f"SELECT COUNT(*) FROM {FQN}")
        count = cur.fetchone()[0]
        print(f"{FQN} row count: {count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke-test `--help`**

Run: `uv run python scripts/snowflake_refresh.py --help`
Expected: usage text.

- [ ] **Step 3: Run against Snowflake**

Run: `uv run python scripts/snowflake_refresh.py`
Expected: prints a row count (0 if still reset from Task 4, otherwise whatever rows are present).

- [ ] **Step 4: Commit**

```bash
git add scripts/snowflake_refresh.py
git commit -m "feat: add snowflake_refresh.py — ALTER ICEBERG TABLE REFRESH wrapper"
```

---

## Task 9: README addendum for the parity flow

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Read current README end**

Run: `tail -30 README.md`

- [ ] **Step 2: Append a new section**

Append to `README.md`:

````markdown
## Comparing Embucket vs Snowflake

This harness runs the same dbt-snowplow-web models on both engines against
the same S3 Tables Iceberg source, loaded in two 30-minute batches, and
diffs the three headline derived tables after each run.

Prerequisites:
- A Snowflake account with access to `~/.snowflake/connections.toml`
  `[connections.default]` (database `sturukin` will be created if missing).
- A pre-existing IAM role `SnowflakeS3TablesAccessRole` with permissions
  to the S3 Tables bucket, trusted by Snowflake. If you don't have one,
  `snowflake_setup.py` will print setup details from `DESC EXTERNAL VOLUME`.
- `profiles.yml` contains a `snowflake` output under `embucket_demo.outputs`
  (see `profiles.yml.example`).

Run flow:

```bash
# 1. One-time Snowflake setup (external volume, catalog integration, iceberg table)
uv run python scripts/snowflake_setup.py

# 2. Reset the shared Athena-managed source table
uv run python scripts/load_from_glue.py init

# 3. Load batch 1
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:00:00' --end '2026-04-22 15:30:00'

# 4. Run dbt on both engines (Snowflake needs a metadata refresh first)
uv run dbt run --profiles-dir . --target dev
uv run python scripts/snowflake_refresh.py
uv run dbt run --profiles-dir . --target snowflake

# 5. Parity check
uv run python scripts/parity.py   # exits 0 on zero diffs

# 6. Load batch 2 (append, do not re-init)
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:30:00' --end '2026-04-22 16:00:00'

# 7. Second dbt run exercises the incremental path
uv run dbt run --profiles-dir . --target dev
uv run python scripts/snowflake_refresh.py
uv run dbt run --profiles-dir . --target snowflake

# 8. Parity check again
uv run python scripts/parity.py
```

Success: `parity.py` exits 0 after both batches.

A non-zero exit means the engines produced different output on the same
input — that's the interesting signal the harness exists to surface.
Rowcount-only mismatches hint at incremental-window or JOIN semantics
divergence; hash mismatches with matching rowcounts hint at cast, NULL, or
ordering divergence.
````

- [ ] **Step 3: Verify the section renders**

Run: `grep -A2 "## Comparing Embucket vs Snowflake" README.md`
Expected: the header plus the first lines of the new section.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: README section for Embucket vs Snowflake parity flow"
```

---

## Task 10: End-to-end smoke run

Nothing to edit — this task just executes the documented flow and verifies the harness works end-to-end on both batches.

- [ ] **Step 1: Reset source**

Run: `uv run python scripts/load_from_glue.py init`
Expected: prints row count 0. Takes ~60s.

- [ ] **Step 2: Snowflake setup (idempotent)**

Run: `uv run python scripts/snowflake_setup.py`
Expected: prints `sturukin.atomic.events_0416 row count after setup: 0`.

- [ ] **Step 3: Parity sanity-check on empty state**

Run: `uv run python scripts/parity.py --source-only`
Expected: prints `source events_0416: embucket=0 snowflake=0`, exits 0.

- [ ] **Step 4: Load batch 1**

Run:
```bash
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:00:00' --end '2026-04-22 15:30:00'
```
Expected: prints a row count around 3.1M. Takes 2-5 min.

- [ ] **Step 5: Refresh Snowflake source**

Run: `uv run python scripts/snowflake_refresh.py`
Expected: prints the same row count as step 4.

- [ ] **Step 6: dbt run on Embucket (batch 1)**

Run: `uv run dbt run --profiles-dir . --target dev`
Expected: all 18 models succeed. Takes several minutes. If any model fails, stop and diagnose before proceeding.

- [ ] **Step 7: dbt run on Snowflake (batch 1)**

Run: `uv run dbt run --profiles-dir . --target snowflake`
Expected: all 18 models succeed.

- [ ] **Step 8: Parity after batch 1**

Run: `uv run python scripts/parity.py`
Expected: rowcount and hash diffs print; the harness exits 0 if parity is clean, non-zero if there are diffs. A non-zero exit here is a **finding**, not a harness bug — capture the output.

- [ ] **Step 9: Load batch 2**

Run:
```bash
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:30:00' --end '2026-04-22 16:00:00'
```
Expected: prints a row count roughly double the batch 1 total (~6.2M).

- [ ] **Step 10: Refresh Snowflake and dbt run on both**

Run:
```bash
uv run python scripts/snowflake_refresh.py
uv run dbt run --profiles-dir . --target dev
uv run dbt run --profiles-dir . --target snowflake
```
Expected: each command succeeds; dbt run output shows incremental models taking a faster path than on the first run.

- [ ] **Step 11: Parity after batch 2**

Run: `uv run python scripts/parity.py`
Expected: diffs printed; exit code recorded. Same interpretation as step 8.

- [ ] **Step 12: Record results**

No code change. Record in the PR description or a follow-up note:
- Did parity pass on batch 1? On batch 2?
- If not, which tables diverged, how many rows, and a sample of natural keys from each bucket (the parity output already contains this).

---

## Self-review

- **Spec coverage**: batch plan (Task 4, Task 10 steps 4 & 9) ✓; load subcommands (Task 4) ✓; split SQL (Task 3, Task 4) ✓; Snowflake setup (Task 7) ✓; refresh (Task 8) ✓; parity script (Task 6) ✓; profiles (Task 5) ✓; dependencies (Task 2) ✓; start_date bump (Task 1) ✓; README (Task 9) ✓; end-to-end verification (Task 10) ✓; `--source-only` harness self-check (Task 6, Task 10 step 3) ✓.
- **Placeholders**: none. SQL bodies reference the existing `events_0416_select.sql` (Task 3) rather than reproducing 140 columns inline, but Task 3 specifies exactly what lines to take from the pre-existing `ctas_from_glue.sql`.
- **Type consistency**: `TableSpec`, `DiffResult`, `row_hash`, and `diff_sides` are consistent between the test (Task 6 Step 2) and implementation (Task 6 Step 4). The target name in Python (`TARGET_TABLE = "events_0416"`), SQL, and Snowflake setup (`ICEBERG_TABLE = "events_0416"`) all agree. Embucket target schema (`demo.atomic_derived`) matches the embucket dbt profile's `schema: atomic`; Snowflake target schema (`sturukin.atomic_derived`) matches the snowflake profile's `schema: atomic`.
