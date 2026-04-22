# Embucket vs Snowflake parity for dbt-snowplow-web

## Goal

Prove that dbt-snowplow-web produces the same derived tables on Embucket and on
Snowflake when run against the same real Snowplow atomic events. The harness
exercises the incremental path by loading data in two batches and re-running
dbt on both engines after each batch; a parity script diffs the three headline
derived tables and exits non-zero on any mismatch.

## Context

Prior work (`specs/2026-04-17-athena-to-s3tables-source-design.md`) loads the
Glue Iceberg source `analytics_glue.hooli_events_0417_v2` into the Embucket
S3 Tables bucket as `demo.atomic.events_0416` via Athena CTAS, and wires
dbt-snowplow-web against it. That path is working on the Embucket side.

This spec adds the parallel Snowflake path so the two engines can be compared.
Scope is limited to making the comparison work for a small, real data sample;
runtime benchmarking is out of scope.

## Constraints

- Both engines must read the same physical Iceberg table. Re-deriving the
  source per engine would re-introduce the type-reconciliation problem the
  Athena CTAS already solved.
- Snowflake reads from an S3 Tables bucket in a different account context than
  the default Snowflake-managed storage. An external volume + catalog
  integration is required; account-level objects may already exist.
- Athena's `CREATE TABLE AS SELECT` can only create; for a batched load flow
  we need `CREATE TABLE` + parametrized `INSERT INTO` instead.
- Snowflake-managed Iceberg tables do not auto-refresh on external writes; a
  manual `ALTER ICEBERG TABLE ... REFRESH` is needed after each Athena write.
- The existing Embucket flow must keep working unchanged.

## Approach

Single dbt project, two targets (`embucket` default, `snowflake` new) under
the existing `embucket_demo` profile. Both read `atomic.events_0416` — on
Embucket natively; on Snowflake via a Snowflake-managed Iceberg table
(`CATALOG_SOURCE = 'ICEBERG_REST'`, S3 Tables as the catalog source;
Glue-federated catalog is the fallback if S3 Tables integration is not
available on the account).

The Athena CTAS is split into an idempotent `CREATE TABLE` and a parametrized
`INSERT INTO ... WHERE load_tstamp >= <start> AND load_tstamp < <end>` so the
source table can be emptied and filled batch by batch. Parity is verified
after each batch's dbt run by a Python script that rowcount-diffs and
hash-diffs the three headline derived tables.

### Data flow

```
Glue Iceberg source (analytics_glue.hooli_events_0417_v2)
           │  Athena INSERT (WHERE load_tstamp in [start, end))
           ▼
   S3 Tables: demo.atomic.events_0416  ◀── single source of truth
           │                                │
           ▼                                ▼
   Embucket Lambda                   Snowflake (REFRESH then SELECT)
           │                                │
           ▼                                ▼
   dbt run --target embucket        dbt run --target snowflake
           │                                │
           └──────────► scripts/parity.py ◀─┘
```

### Batch plan

Source `load_tstamp` spans `2026-04-17 17:00` to `2026-04-22 18:00`, with
~1.8M–6.4M rows per hour. The harness uses the freshest complete hour split
into two 30-minute batches:

- **Batch 1**: `2026-04-22 15:00:00` → `2026-04-22 15:30:00` (~3.1M rows)
- **Batch 2**: `2026-04-22 15:30:00` → `2026-04-22 16:00:00` (~3.1M rows)

`snowplow__start_date` bumps from `2026-04-13` to `2026-04-22` so both
engines' incremental state starts at the same cursor.

### Snowflake connection

Credentials come from the user's existing `~/.snowflake/connections.toml`
`default` connection: account `aa06228.us-east-2.aws`, user `rampage644`,
role `ACCOUNTADMIN`, warehouse `compute_wh`. Target lands in database
`sturukin`, schema `atomic`.

`profiles.yml.example` carries placeholders; the real `profiles.yml` (already
gitignored) holds the values. Both `embucket` and `snowflake` outputs live
under the single `embucket_demo` profile so `sources.yml` stays unchanged and
`source('atomic','events')` resolves to `atomic.events_0416` on both sides.

### Type and schema reconciliation

Already handled by the Athena CTAS: both engines read the same Iceberg files,
so there is nothing to reconcile here beyond confirming Snowflake's
Iceberg reader surfaces the same column types Embucket does. A
`DESCRIBE TABLE` diff is part of the harness self-check.

## Components

### `scripts/create_events_0416.sql` (new)

`CREATE TABLE events_0416 (...empty with target schema...)` with
`partitioning = ARRAY['day(load_tstamp)', 'event_name']`. Column list and
order mirror `scripts/create_table.sql`.

### `scripts/insert_events_0416.sql` (new)

The projection/cast `SELECT` body from the existing `ctas_from_glue.sql`,
wrapped in:
```
INSERT INTO events_0416
SELECT ...
FROM awsdatacatalog.analytics_glue.hooli_events_0417_v2
WHERE load_tstamp >= TIMESTAMP '{{start}}'
  AND load_tstamp <  TIMESTAMP '{{end}}';
```
`{{start}}` and `{{end}}` are placeholders rendered by `load_from_glue.py`.

### `scripts/ctas_from_glue.sql` (deleted)

Replaced by the pair above. The existing one-shot load is reproducible by
running `init` then one `insert` spanning the full source range.

### `scripts/load_from_glue.py` (refactored)

Two subcommands:
- `init` — drops `s3tablescatalog/snowplow.atomic.events_0416` if it exists;
  runs `create_events_0416.sql`.
- `insert --start <iso> --end <iso>` — renders and runs
  `insert_events_0416.sql`; polls Athena to completion; prints inserted
  row count on success.

Takes the existing `--athena-workgroup` and `--query-output-location` args.

### `scripts/snowflake_setup.py` (new)

Idempotent one-shot Snowflake setup.
1. `SHOW EXTERNAL VOLUMES` — if an existing volume covers the `snowplow`
   S3 Tables bucket, reuse it; else `CREATE EXTERNAL VOLUME snowplow_vol ...`.
2. `SHOW CATALOG INTEGRATIONS` — if an S3 Tables or matching Glue integration
   already exists, reuse it; else `CREATE CATALOG INTEGRATION snowplow_s3t
   CATALOG_SOURCE = 'ICEBERG_REST' TABLE_FORMAT = 'ICEBERG'
   REST_CONFIG = (CATALOG_URI = ..., CATALOG_NAME = 'snowplow')
   REST_AUTHENTICATION = (TYPE = 'SIGV4' ...)`.
3. `CREATE DATABASE IF NOT EXISTS sturukin; CREATE SCHEMA IF NOT EXISTS
   sturukin.atomic;`.
4. `CREATE OR REPLACE ICEBERG TABLE sturukin.atomic.events_0416
   CATALOG = 'snowplow_s3t' CATALOG_TABLE_NAME = 'events_0416'
   EXTERNAL_VOLUME = 'snowplow_vol'`.
5. Grants: `GRANT USAGE ON DATABASE sturukin TO ROLE ACCOUNTADMIN`
   (no-op under ACCOUNTADMIN but kept for least-privilege future tightening).

Connects via `snowflake.connector.connect(connection_name='default')` so it
uses the existing `~/.snowflake/connections.toml` entry.

Fallback (controlled by `--catalog glue`): creates a Glue-federated catalog
integration against `s3tablescatalog/snowplow` instead.

### `scripts/snowflake_refresh.py` (new)

Runs `ALTER ICEBERG TABLE sturukin.atomic.events_0416 REFRESH;` and prints
the resulting row count. Invoked between each Athena load and the
corresponding Snowflake dbt run.

### `scripts/parity.py` (new)

For each of `snowplow_web_page_views`, `snowplow_web_sessions`,
`snowplow_web_users`:
- `COUNT(*)` on both sides; must be equal.
- Natural-key set diff:
  - `page_views.page_view_id`
  - `sessions.session_identifier`
  - `users.user_identifier`
- Row hash: `SELECT <natural_key>, MD5(CONCAT_WS('|',
  COALESCE(CAST(c AS STRING), '∅'), ...))` over the columns declared in the
  model's yaml schema (excludes engine-specific metadata columns).

Prints per-table summary (matched / mismatched / only-in-embucket /
only-in-snowflake counts) and up to 10 example natural keys from each
mismatch bucket. Exits non-zero on any mismatch.

`--source-only` mode skips derived tables and just diffs
`events_0416` rowcount + `DESCRIBE TABLE`; used as the harness self-check
before any dbt run.

Connects to Embucket via `scripts/embucket_client.py`, to Snowflake via
`snowflake.connector.connect(connection_name='default')`.

### `profiles.yml.example` (edit)

Add `snowflake` output under `embucket_demo`:
```yaml
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
Local `profiles.yml` gets the real values from
`~/.snowflake/connections.toml`.

### `pyproject.toml` (edit)

Add `snowflake-connector-python` and `dbt-snowflake` (matching the existing
dbt version range) as dependencies.

### `dbt_project.yml` (edit)

`snowplow__start_date: '2026-04-22'`.

### `packages.yml` / `patch_snowplow.sh`

No change. `patch_snowplow.sh` is Embucket-specific; dbt-snowplow-web is
Snowflake-first upstream, so no patch is needed on the Snowflake target.

### `README.md` (addendum)

New "Comparing Embucket vs Snowflake" section documenting the 7-step loop
(setup, init, batch 1 insert, dbt run × 2 + refresh, parity, batch 2 insert,
dbt run × 2 + refresh, parity).

## End-to-end run flow

All commands from repo root.

1. `uv run python scripts/snowflake_setup.py` — one-time idempotent setup.
2. `uv run python scripts/load_from_glue.py init` — empty source.
3. `uv run python scripts/load_from_glue.py insert --start '2026-04-22 15:00:00' --end '2026-04-22 15:30:00'`
4. `uv run dbt run --profiles-dir . --target embucket`
   `uv run python scripts/snowflake_refresh.py`
   `uv run dbt run --profiles-dir . --target snowflake`
5. `uv run python scripts/parity.py` — must exit 0.
6. `uv run python scripts/load_from_glue.py insert --start '2026-04-22 15:30:00' --end '2026-04-22 16:00:00'`
7. Repeat step 4, then `parity.py` again.

Success: parity.py exits 0 both times.

## Verification

**Harness self-checks (before trusting a run):**

1. `parity.py --source-only` on an empty `events_0416`: exits 0
   (rowcounts both 0, schemas match). Confirms no false positives.
2. `DESCRIBE TABLE events_0416` on both engines: same column names, same
   types. A mismatch here invalidates every downstream diff and is a bug in
   this spec's setup, not a finding.
3. First `load_from_glue.py insert` returns a row count matching the Athena
   query stats.

**Parity assertions (the findings):**

- Zero diffs after batch 1 dbt run on all three headline tables.
- Zero diffs after batch 2 dbt run on all three headline tables.

**What success means:** the two engines produce byte-for-byte equivalent
headline derived tables on the same input, through both the initial-build
and the incremental-update paths.

**What failure means (and how to read it):**
- Rowcount matches, hashes don't → semantic drift (cast, NULL handling,
  window ordering). This is the signal the harness exists to surface.
- Rowcount differs → incremental-window or JOIN-semantics divergence.
- Batch 1 passes, batch 2 fails → incremental merge logic diverges.
- Snowflake empty after an Athena write → `snowflake_refresh.py` was
  skipped.

## Risks / open issues

1. **S3 Tables catalog integration support**: Snowflake's native S3 Tables
   integration is the preferred path; if unavailable on this account, fall
   back to Glue-federated catalog integration (`--catalog glue` on
   `snowflake_setup.py`). Both are implemented.
2. **Iceberg type drift between Embucket and Snowflake readers**: both read
   the same files but may surface columns differently (e.g. `TIMESTAMP_NTZ`
   vs `TIMESTAMP_LTZ`). The `--source-only` parity check catches this
   before dbt runs are wasted.
3. **`MD5(CONCAT_WS(...))` column ordering**: must match column-by-column
   between engines. Parity script derives the column list from the model's
   yaml schema, not `SELECT *`, so ordering is deterministic.
4. **Snowflake-managed Iceberg refresh lag**: `ALTER ICEBERG TABLE ...
   REFRESH` is synchronous but metadata refresh can still miss very recent
   Athena commits if the Glue commit propagation hasn't landed. If
   rowcounts disagree between Embucket and Snowflake on the source,
   re-running `snowflake_refresh.py` is the first thing to try.
5. **dbt-snowflake version compatibility**: must satisfy the existing
   `[">=1.6.0", "<2.0.0"]` constraint in `dbt_project.yml`. Verified at
   dependency-add time.

## Out of scope

- Runtime / performance benchmarking between engines.
- Parity on non-headline derived tables or intermediate scratch models.
- Tables gated off by disabled context flags (iab, ua, yauaa, consent, cwv).
- CI integration; the harness runs locally for now.
- `dbt test` suite runs.
- Cross-account Snowflake setup; everything lives in the account already
  configured in `~/.snowflake/connections.toml`.
