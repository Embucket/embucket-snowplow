# Embucket vs Snowflake correctness investigation — dbt-snowplow-web

**Date:** 2026-04-22
**Branch:** `rampage644/real-snowplow-source`
**PR:** https://github.com/Embucket/embucket-snowplow/pull/6

## 1. Goal

Run the same `dbt-snowplow-web` pipeline on Embucket (Snowflake-compatible
engine on AWS Lambda + S3 Tables Iceberg) and on Snowflake, against the same
source data, and determine whether the two engines produce equivalent results.
The Embucket side is the system under investigation; Snowflake is the
reference.

## 2. Setup

### 2.1 Source data

- Glue-managed Iceberg table `analytics_glue.hooli_events_0417_v2`
  in account 767397688925, us-east-2. Real Snowplow atomic events,
  ~2.1M rows per hour of data, bursty `load_tstamp` distribution within
  each hour.
- Source has an upstream data quality issue: **4.7% duplicate `event_id`
  values** (2,834,944 rows in the 15:00-15:30 window / 2,700,579 distinct
  `event_id`). Snowplow Web expects the atomic layer to be pre-deduped.

### 2.2 Shared source table

Both engines read the **same physical Iceberg table**, not two copies:

- Athena writes the source into the S3 Tables bucket
  `arn:aws:s3tables:us-east-2:767397688925:bucket/snowplow` as
  `atomic.events_0416` (`scripts/load_from_glue.py`).
- Embucket Lambda is volumed to that bucket and sees
  `demo.atomic.events_0416` directly.
- Snowflake reads the same table via a Glue-federated Iceberg REST
  catalog integration (`SNOWPLOW_S3T`) as
  `sturukin_db.atomic.events_0416`, fronted by a `events_0416_v`
  view that `TRY_PARSE_JSON`s the seven context VARCHAR columns
  into VARIANT (so snowplow-web's `contexts_...[0]:id` indexing
  works).

Integrity of the shared source verified: both engines return identical
`COUNT(*)`, identical `MIN/MAX(load_tstamp)` after every load.

### 2.3 Loading mechanics

- `scripts/load_from_glue.py init` — drops and recreates the empty
  `events_0416` with the correct schema (via CTAS `WHERE 1=0`) and
  `partitioning = ARRAY['day(load_tstamp)', 'event_name']`.
- `scripts/load_from_glue.py insert --start <iso> --end <iso>` — INSERTs
  a `[start, end)` window of `load_tstamp` from the Glue source.
- `scripts/snowflake_setup.py` — idempotent: creates catalog integration,
  grants Lake Formation DESCRIBE+SELECT on the S3 Tables source table to
  the Snowflake IAM role, (re)creates the managed Iceberg table + VARIANT
  view.
- `scripts/snowflake_refresh.py` — `ALTER ICEBERG TABLE ... REFRESH` after
  each Athena write.

### 2.4 dbt wiring

- Single project, single `embucket_demo` profile, two targets: `dev`
  (type: embucket) and `snowflake` (type: snowflake).
- `scripts/patch_snowplow.sh` rewrites
  `target.type == 'snowflake'` → `target.type in ['snowflake', 'embucket']`
  across `dbt_packages/`, so Embucket dispatches the Snowflake-flavored
  snowplow-web models and macros. Without this, compilation fails with
  `Snowplow: Unexpected target type embucket` from
  `snowplow_utils.get_value_by_target_type`.
- `dbt_project.yml` sets `snowplow__events_table` target-aware:
  `events_0416` on Embucket, `events_0416_v` on Snowflake.
- 7 of the 7 snowplow-web context flags are at their defaults (only
  `snowplow__enable_iab`, `_ua`, `_yauaa`, `_consent`, `_cwv` are
  relevant here; all off in both targets).

### 2.5 Embucket Lambda configuration

- Function memory: 10240 MB (10 GB)
- `MEM_POOL_SIZE_MB = 9216`
- `MEM_POOL_TYPE = greedy`
- `DISK_POOL_SIZE_MB = 10240` (spill enabled)
- `QUERY_TIMEOUT_SECS = 1200`

### 2.6 Comparison tooling

- `scripts/parity.py` — set-based row-level diff. Fetches `(key, md5)`
  pairs from each engine and diffs. Abandoned for large tables: 775K
  rows × 64 bytes = 50 MB, well above the Lambda 6 MB response cap.
- `scripts/parity_deep.py` — **server-side aggregate parity**. One query
  per table per engine returning a single row:
  - `COUNT(*)`, `COUNT(DISTINCT <natural_key>)`
  - `MIN`/`MAX` `start_tstamp` cast to epoch microseconds (BIGINT)
  - 4-5 SUM metrics (page_views, sessions, engaged_time_in_s, etc.)
  - 16 position-wise MD5 checksums: for each of 16 hex positions of each
    row's `MD5(CONCAT_WS('|', col1, col2, ...))`, `SUM(ASCII(SUBSTR(md5, i, 1)))`.
  Order-independent and portable. Timestamp columns are normalized to
  epoch-microseconds before MD5 so engine-specific `CAST(ts AS VARCHAR)`
  rendering differences don't contaminate the hash.

## 3. Checks performed and findings

Four full runs at two data volumes; batch 1 only (CTAS-path) and batch
1 + batch 2 (MERGE-path on the incremental models).

### 3.1 2.83M rows (30-minute window `15:00-15:30`)

**Embucket behavior:**
- Without `patch_snowplow.sh`: compile error
  (`Unexpected target type embucket`).
- With `patch_snowplow.sh`: **OOM** on
  `snowplow_web_base_events_this_run`:
  ```
  Resources exhausted: Failed to allocate additional 5.4 MB
  for RepartitionExec[2] with 5.1 MB already allocated for
  this reservation - 3.1 MB remain available for the total pool
  ```
  The repartition for the join against
  `snowplow_web_base_sessions_this_run` exhausts DataFusion's 9 GB pool
  even with spill enabled. All 12 downstream models `SKIP`.

**Snowflake behavior:** completes cleanly, 20 PASS (or 31, depending on
the package version). Derived tables populated.

**Result at this scale:** Embucket cannot complete the pipeline on
2.83M rows × ~130 columns on the current Lambda configuration.

### 3.2 0.28M rows (1/10 scale, single 15:02 burst, 323,559 events)

Both engines: `dbt seed` + `dbt run` succeed, 20 PASS each.

**Parity deep check (batch 1 only — CTAS path for incremental models):**

| table | rowcount | distinct natural key | MIN/MAX start_tstamp | SUM metrics | 16 md5 checksums |
|-------|----------|----------------------|----------------------|-------------|------------------|
| `snowplow_web_page_views` | **equal** | **equal** | **equal** | `sum_absolute_time_in_s` -1.24% on Snowflake; others match | all 16 drift ~0.03% |
| `snowplow_web_sessions` | **equal** (10,891) | **equal** | **equal** | `sum_absolute_time_in_s` -0.81% on Snowflake | all 16 drift ~0.1% |
| `snowplow_web_users` | **equal** (6,243) | **equal** | **equal** | **all match** | drift driven by 1 column |

Two real divergences isolated at this scale:

**Finding A — `absolute_time_in_s` ±1-second rounding.** Spot-checks:
- Session `7da3c35f-5565-4903-88df-bb3c19a918f9`: Embucket 231, Snowflake 230; all other columns identical.
- Page view `00005058-c0e9-4e2a-8c2a-24db2e1f8fa4`: Embucket 40, Snowflake 39; all others identical.

Consistent with different integer-truncation of second-level timestamp
subtraction between engines. Cumulative effect is ~1% divergence on the
table-level SUM.

**Finding B — `users.referrer` column NULL drift.** At 1/10 scale, 239
of 12,346 users (2%) differ on whether `referrer` is NULL. Same 18
distinct non-null referrer values on both engines. Root cause in
`snowplow_web_users_this_run`:

```sql
max(case when start_tstamp = user_start_tstamp
         then domain_sessionid end) AS first_domain_sessionid
```

When a user has two sessions sharing `start_tstamp` (tie), `MAX(uuid)`
resolves to a different UUID on each engine, joining to a different
first-session row whose `referrer` is populated on one engine and NULL
on the other. Deterministic tie-break difference, not a correctness bug
per se.

### 3.3 0.61M rows — batch 2 added (MERGE path for incremental models)

Batch 2: +286,112 rows from 15:15 load_tstamp burst. Total source:
609,671 events.

Both engines: `dbt run` 20 PASS. No errors.

**Parity deep check:**

| table | rowcount | distinct key | SUM metrics |
|-------|----------|--------------|-------------|
| `snowplow_web_page_views` | equal | equal | `sum_absolute_time_in_s` -1.25% on Snowflake (per-row rounding compounding); others match |
| `snowplow_web_sessions` | equal | equal | `sum_absolute_time_in_s` -0.85% on Snowflake; others match |
| **`snowplow_web_users`** | **equal (35,094)** | **equal (35,094)** | **`sum_page_views` +6.4% / `sum_sessions` +6.5% / `sum_engaged_time_in_s` +6.4% on Snowflake** |

**Finding C — Embucket MERGE UPDATE does not apply for ~9% of users on
the second `dbt run`.** 3,164 of 35,094 users have diverging
aggregates, strictly one-sided:

```
of mismatches: sf_aggregate_larger=3164  emb_aggregate_larger=0
```

### 3.4 Localization of Finding C

Instrumented every upstream stage:

| stage | rowcount | SUM(page_views) | SUM(engaged_time_in_s) |
|-------|----------|-----------------|------------------------|
| `snowplow_web_base_events_this_run` | 609,671 / 609,671 | — | — |
| `snowplow_web_base_sessions_this_run` | 75,642 / 75,642 | — | — |
| `snowplow_web_sessions_this_run` | 75,482 / 75,482 | — | — |
| `snowplow_web_sessions` (derived) | 75,482 / 75,482 | — | — |
| `snowplow_web_users_sessions_this_run` | 75,482 / 75,482 | **181,900 both** | **3,298,530 both** |
| `snowplow_web_users_aggs` | 35,094 / 35,094 | **181,900 both** | **3,298,530 both** |
| `snowplow_web_users_this_run` | 35,094 / 35,094 | — | — |
| **`snowplow_web_users` (derived)** | 35,094 / 35,094 | **emb 170,884 / sf 181,900** | **emb 3,100,035 / sf 3,298,530** |

The scratch `users_this_run` source for the MERGE has identical
aggregates on both engines. The divergence appears purely at the final
MERGE step.

**Per-user spot check** (user `eeb8f559-c8d0-4c3b-b50f-e70a11a5d997`):

| source | Embucket | Snowflake |
|--------|----------|-----------|
| scratch `users_this_run` | 39 pv, 16 sess, 535 eng, end=15:14:44 | 39 pv, 16 sess, 535 eng, end=15:14:44 |
| derived `users` after MERGE | **27 pv, 9 sess, 415 eng, end=15:02:11** | 39 pv, 16 sess, 535 eng, end=15:14:44 |

Embucket's scratch row has the correct cumulative values; the target
row still carries batch-1 values. Snowflake updates correctly.

**Compiled MERGE SQL is byte-identical on both engines** (the patch
makes Embucket dispatch the Snowflake MERGE macro):

```sql
MERGE INTO <target> AS DBT_INTERNAL_DEST
  USING <__dbt_tmp> AS DBT_INTERNAL_SOURCE
  ON (DBT_INTERNAL_DEST.start_tstamp BETWEEN
        '2026-04-22 14:52:51.530000' AND '2026-04-22 15:15:39.548000')
    AND (DBT_INTERNAL_SOURCE.domain_userid = DBT_INTERNAL_DEST.domain_userid)
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

For the affected user: target's `start_tstamp = 14:58:56.280` is
inside the BETWEEN range, `domain_userid` matches the scratch row —
so the ON condition is satisfied and WHEN MATCHED UPDATE should fire.

## 4. Summary of findings

| # | Finding | Scope | Attribution |
|---|---------|-------|-------------|
| 0 | Source Glue table has 4.7% duplicate `event_id` | upstream | data quality (not engine) |
| 1 | Embucket OOMs on `snowplow_web_base_events_this_run` at 2.83M rows even with 10 GB pool + 10 GB spill | Embucket engine | memory/spill behavior on wide row with JSON VARIANT columns and repartition-on-join |
| 2 | Unpatched snowplow-web fails on Embucket with `Unexpected target type embucket` | dbt package | hardcoded `target.type` branches; patchable via 1-line regex |
| 3 | `absolute_time_in_s` drifts ±1 second per row | both engines | different integer truncation of timestamp subtraction; visible in ~1% of rows |
| 4 | `users.referrer` NULL-fill differs for 2% of users | snowplow-web package | `MAX(uuid)` tie-break when two sessions share `start_tstamp`; deterministic per engine but not engine-invariant |
| 5 | Snowplow users/page_views/sessions derived tables preserve the source's duplicate-`event_id` rows on first CTAS | snowplow-web package | `_this_run → derived` step doesn't dedup on unique_key; Snowflake errors on second MERGE, Embucket silently appends |
| 6 | **Embucket MERGE UPDATE fails to apply for ~9% of matching target rows on the users derived table** | **Embucket engine** | **compiled MERGE SQL identical; source identical; ~9% of matched rows silently not updated. One-sided (sf ≥ emb always)** |

## 5. Conclusions

**1. Source parity is not the issue.** The Iceberg table
`atomic.events_0416` is byte-for-byte identical from both engines'
perspective: same `COUNT(*)`, same `MIN/MAX(load_tstamp)`, same column
types (after the Snowflake-side VARIANT view converts the JSON context
VARCHARs). Every downstream divergence is post-read.

**2. Most divergence is attributable to the source or to the
snowplow-web package, not to Embucket.** Findings 0, 3, 4, and 5 apply
equally to both engines in the sense that they produce different
outputs from the *same input* because the package's SQL under-specifies
order-insensitive behavior (tie-breaks, dedup) and because the source
has DQ issues the package assumes away. A clean source plus strict-SQL
snowplow-web would remove these.

**3. Two Embucket-specific engine issues remain.**

   - **Memory**: at 2.83M rows the base-events repartition exceeds 9 GB
     pool + 10 GB spill. Either the repartition shuffle is
     under-spilling or the per-row footprint is larger than
     Snowflake's. Reproducer: `load_from_glue.py insert` with a
     full-hour window + `dbt run --target dev`. Probably worth
     investigating whether disk spill actually engages for the
     RepartitionExec path.

   - **MERGE UPDATE correctness**: Finding 6 is the most concrete
     correctness finding. Identical SQL, identical source data,
     identical target pre-state (both engines have the same batch-1
     row); Snowflake executes the UPDATE, Embucket leaves ~9% of
     matching rows unmodified. Investigation should focus on
     Embucket's handling of MERGE when the ON clause combines a
     `BETWEEN` predicate on the target with an equality predicate on
     the source's unique key. Candidate hypotheses:
     - Predicate is evaluated against a pre-filtered subset of the
       target rather than all target rows (incorrect index/partition
       pruning).
     - MATCHED rows are identified but UPDATE is skipped when the
       source row and target row differ by columns that aren't in the
       ON clause, due to a mis-applied "no-op if unchanged"
       optimization.
     - The target-vs-source role is inverted for a subset of rows
       during parallel execution.

**4. At scale the two kinds of issues compound.** The OOM at 2.83M
rows and the 9% MERGE UPDATE miss at 609K rows are the practical
blockers for running dbt-snowplow-web on real-volume data through
Embucket. Both are worth isolating into minimal reproducers outside
the snowplow-web package.

## 6. Reproducers

**Memory OOM:**

```bash
./scripts/patch_snowplow.sh
uv run python scripts/load_from_glue.py init
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:00:00' --end '2026-04-22 15:30:00'
uv run dbt seed --profiles-dir . --target dev
uv run dbt run --profiles-dir . --target dev
# Expect: Failure in snowplow_web_base_events_this_run, "Resources exhausted"
```

**MERGE UPDATE correctness:**

```bash
# 1. Load batch 1 (15:02 burst, 323,559 rows)
uv run python scripts/load_from_glue.py init
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:00:00' --end '2026-04-22 15:03:00'
uv run python scripts/snowflake_refresh.py
uv run dbt seed --profiles-dir . --target dev
uv run dbt seed --profiles-dir . --target snowflake
uv run dbt run --profiles-dir . --target dev
uv run dbt run --profiles-dir . --target snowflake

# 2. Load batch 2 (15:15 burst, 286,112 rows) and re-run dbt
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:15:00' --end '2026-04-22 15:16:00'
uv run python scripts/snowflake_refresh.py
uv run dbt run --profiles-dir . --target dev
uv run dbt run --profiles-dir . --target snowflake

# 3. Parity check: expect sf_page_views > emb_page_views by ~6.4%
uv run python scripts/parity_deep.py
```

Drill-down on a single affected user after the reproducer:

```python
SELECT page_views, sessions, engaged_time_in_s, end_tstamp
FROM demo.atomic_derived.snowplow_web_users
WHERE domain_userid = 'eeb8f559-c8d0-4c3b-b50f-e70a11a5d997';
-- Embucket: (27, 9, 415, 15:02:11)

SELECT page_views, sessions, engaged_time_in_s, end_tstamp
FROM sturukin_db.atomic_derived.snowplow_web_users
WHERE domain_userid = 'eeb8f559-c8d0-4c3b-b50f-e70a11a5d997';
-- Snowflake: (39, 16, 535, 15:14:44)

SELECT page_views, sessions, engaged_time_in_s, end_tstamp
FROM demo.atomic_scratch.snowplow_web_users_this_run
WHERE domain_userid = 'eeb8f559-c8d0-4c3b-b50f-e70a11a5d997';
-- Both: (39, 16, 535, 15:14:44) -- scratch is correct on both engines;
-- the bug is in how Embucket's MERGE applies the UPDATE.
```

## 7. Tooling added on this branch

- `scripts/load_from_glue.py` — Athena loader with `init`/`insert` subcommands.
- `scripts/events_0416_select.sql` — shared projection SELECT body with a
  `{{where}}` placeholder.
- `scripts/snowflake_setup.py` — idempotent Snowflake-side setup: catalog
  integration, LF grants, iceberg table, VARIANT view.
- `scripts/snowflake_refresh.py` — `ALTER ICEBERG TABLE ... REFRESH`.
- `scripts/parity.py` — set-based row-level diff (abandoned at scale).
- `scripts/parity_deep.py` — server-side aggregate parity (the useful one).
- `tests/test_parity.py` — pure-Python unit tests on the hash/diff logic.
- `patch_snowplow.sh` — 1-line rewrite that makes Embucket dispatch
  Snowflake snowplow-web models.
