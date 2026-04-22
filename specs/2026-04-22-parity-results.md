# Parity harness run results (2026-04-22)

## First full run (batch 1 + batch 2, stale state between)

Initial run on the first loaded state. Both engines processed 2.83M rows
after batch 1 and 6.16M after batch 2. Rowcount divergence on all three
headline tables. Snowflake batch-2 incremental did not advance headline
models (incremental manifest guard did not trigger). Embucket users table
grew far beyond input (15.4M vs 6.16M events). See git history.

## Clean batch-1-only investigation (the useful one)

After this run all downstream state (derived/scratch/manifest schemas on
both engines; dbt seeds re-run on both) was dropped, `load_from_glue.py
init` fresh, only batch 1 loaded, `dbt run` once per target.

### Source

| table | rows | distinct event_id | dup ratio |
|-------|------|-------------------|-----------|
| Glue source (hooli_events_0417_v2, 15:00-15:30 window) | 2,834,944 | 2,700,579 | 1.050 |
| demo.atomic.events_0416 (Embucket) | 2,834,944 | 2,700,579 | 1.050 |
| sturukin_db.atomic.events_0416 (Snowflake) | 2,834,944 | 2,700,579 | 1.050 |

Source parity is exact. Source contains **4.7% duplicate event_ids** --
an upstream data quality issue inherited from the Glue-managed source.

### Scratch tables after batch 1 (essentially identical on both engines)

| table | Embucket rows / distinct key | Snowflake rows / distinct key | dup ratio |
|-------|------------------------------|-------------------------------|-----------|
| snowplow_web_base_events_this_run | 2,834,944 / 2,700,579 event_id | 2,834,944 / 2,700,579 | 1.050 |
| snowplow_web_page_views_this_run  | 814,555 / 775,543 page_view_id | 814,570 / 775,543 | 1.050 |
| snowplow_web_sessions_this_run    | 360,999 / 317,162 domain_sessionid | 360,999 / 317,162 | 1.138 |

Pre-merge state is byte-equivalent across engines.

### Derived tables: first `dbt run` CTAS, second+ run MERGE

On the first `dbt run` with no pre-existing target, dbt's incremental
materialization issues `CREATE TABLE AS` (Snowflake) / equivalent
(Embucket), bypassing MERGE entirely. Both engines produce identical
counts, and **both preserve the source duplicates** -- the snowplow-web
`_this_run → derived` step does not deduplicate on unique_key:

| table | Embucket rows / distinct | Snowflake rows / distinct | dup ratio |
|-------|--------------------------|---------------------------|-----------|
| snowplow_web_page_views | 814,555 / 775,543 page_view_id | 814,569 / 775,543 | 1.050 |
| snowplow_web_sessions   | 360,999 / 317,162 domain_sessionid | 360,999 / 317,162 | 1.138 |
| snowplow_web_users      | ? | 122,041 / 71,352 domain_userid | 1.710 |

On a **second** `dbt run` with the scratch still holding the same
duplicates and the target now populated:

- **Snowflake** fails loudly: `100090 (42P18): Duplicate row detected
  during DML action`. ANSI MERGE refuses to match multiple source rows
  to one target row when they share the unique_key.
- **Embucket** succeeds silently and adds more duplicate rows to the
  derived tables (this is how the first full harness run ended up with
  Embucket page_views = 1.34M vs Snowflake 814K -- two dbt runs amplified
  Embucket's duplicate count).

## Fresh-`dbt deps` re-run (no patch, then with patch)

After running `dbt deps` from scratch (prior modified copy preserved as
`dbt_packages.bak.1776896708/`) on the same batch-1 source:

**Without `patch_snowplow.sh`** the Embucket target fails immediately at
compile time:
```
Compilation Error in model snowplow_web_page_views
  Snowplow: Unexpected target type embucket
  > in macro get_value_by_target_type
```
Confirms why the patch exists: `snowplow_utils` has explicit
`target.type` branches (snowflake/bigquery/databricks/postgres/redshift/
spark) and errors out on anything else.

**With `patch_snowplow.sh`** applied (rewrites `target.type == 'snowflake'`
to `target.type in ['snowflake','embucket']` so Embucket dispatches to
the Snowflake-flavored models), Embucket now hits a **memory OOM** on
the biggest scratch model:
```
Failure in model snowplow_web_base_events_this_run
  Database Error
  Resources exhausted: Failed to allocate additional 5.4 MB for
  RepartitionExec[2] with 5.1 MB already allocated for this
  reservation - 3.1 MB remain available for the total pool
```
All 12 downstream models SKIP; only the 7 pre-scratch models succeed.
Lambda config at the time: 10 GB function memory, `MEM_POOL_SIZE_MB=9216`,
`DISK_POOL_SIZE_MB=10240` (spill enabled), `MEM_POOL_TYPE=greedy`. 2.83M
rows × ~130 columns (including the 7 JSON-context VARIANT columns)
still exhaust DataFusion's pool during the repartition for the JOIN
against `snowplow_web_base_sessions_this_run`.

Snowflake ran to completion on the same source: `Done. PASS=20`.

## Deep column-level parity (parity_deep.py)

With seeds loaded on both engines and a single clean `dbt run` each
(31 PASS both), `scripts/parity_deep.py` computes server-side
aggregates per headline table:
row count, distinct natural-key count, MIN/MAX `start_tstamp` (cast to
epoch microseconds so engine timestamp-serialization differences don't
masquerade as content divergence), 4-5 numeric SUMs, and 16 order-
independent row-MD5 checksums (one per hex position). Results:

| table | rowcount | distinct natural key | min/max start | md5 checksums | selected SUMs |
|-------|----------|----------------------|---------------|---------------|---------------|
| snowplow_web_page_views | **+12 on Snowflake** (814,547 vs 814,559) | **equal** (775,543 both) | equal | all 16 drift ~0.03% | sum_engaged: +180 sec (0.001%); sum_absolute_time: +1.27% on Embucket; sum_doc_height/width: drift < 0.003% |
| snowplow_web_sessions   | equal (360,999 both) | equal (317,162 both) | equal | all 16 drift ~0.1% | sum_absolute_time: -0.86% on Snowflake |
| snowplow_web_users      | +12,056 on Embucket (134,097 vs 122,041) | **equal** (71,352 both) | equal | drift ~9% | sum_engaged/page_views/sessions scale with the +9.9% row excess on Embucket |

**Interpretation**:

1. **Same content, different duplicate multiplicity.** Distinct
   natural-key counts match exactly on every table. Distinct
   `(key, absolute_time, engaged_time, page_views_in_session)` tuple
   count also matches (775,543 on both for page_views). So the two
   engines identified the same set of page views / sessions / users,
   produced the same attribute values for each, but copied some of
   those rows into the derived tables a different number of times
   during the scratch→derived CTAS step. This is an artifact of the
   non-deduplicating `_this_run → derived` path running over a source
   that has duplicate event_ids.

2. **±1 second rounding on `absolute_time_in_s`** for individual rows.
   Spot-checked session `7da3c35f-5565-4903-88df-bb3c19a918f9`:
   Embucket 231, Snowflake 230, all other columns identical.
   Spot-checked page_view `00005058-c0e9-4e2a-8c2a-24db2e1f8fa4`:
   Embucket 40, Snowflake 39, other columns identical.
   Consistent with different integer-truncation of second-level
   timestamp subtraction between the two engines.

3. **Tiny md5-position drift with matching distinct-key counts** is the
   hash signature of (1)+(2) combined; it is not independent evidence
   of more divergence.

## Findings

1. **Glue source has ~4.7% duplicate event_ids**. Snowplow assumes the
   atomic layer is deduplicated upstream; this source violates that
   contract. Root cause of everything below.
2. **snowplow-web does not deduplicate on unique_key in the
   `_this_run → derived` step**, on either engine. Both Snowflake and
   Embucket's first `dbt run` CTAS the scratch content verbatim, so
   both derived tables end up with duplicate page_view_id /
   domain_sessionid / domain_userid rows. Not an engine bug -- a
   package-level assumption that the atomic layer is already clean.
3. **MERGE semantics diverge on re-run.** With duplicates in the
   scratch `_this_run` tables and rows in the target, Snowflake's
   MERGE errors with `100090 (42P18): Duplicate row detected` while
   Embucket's MERGE succeeds and keeps inserting more duplicate rows.
   This explains the earlier "Embucket 1.34M page_views vs Snowflake
   814K" numbers -- two dbt runs on Embucket against duplicated
   source, each run appending.

## Reproducing

From a clean state (both engines' derived/scratch/manifest schemas
dropped, iceberg source table dropped):

```bash
uv run python scripts/load_from_glue.py init
uv run python scripts/snowflake_setup.py
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:00:00' --end '2026-04-22 15:30:00'
uv run python scripts/snowflake_refresh.py
uv run dbt seed --profiles-dir . --target dev
uv run dbt seed --profiles-dir . --target snowflake
uv run dbt run --profiles-dir . --target dev         # 31 PASS on first run (CTAS)
uv run dbt run --profiles-dir . --target snowflake   # 31 PASS on first run (CTAS)
# Re-running either without resetting scratch/derived state:
uv run dbt run --profiles-dir . --target snowflake   # 2 errors on MERGE (dup source)
uv run dbt run --profiles-dir . --target dev         # succeeds silently, appending dups
```

Scratch counts can be read from either engine:
```sql
SELECT COUNT(*), COUNT(DISTINCT page_view_id)
FROM <engine>.atomic_scratch.snowplow_web_page_views_this_run;
```

Source dup rate from Athena:
```sql
SELECT COUNT(*), COUNT(DISTINCT event_id)
FROM analytics_glue.hooli_events_0417_v2
WHERE load_tstamp >= TIMESTAMP '2026-04-22 15:00:00 UTC'
  AND load_tstamp <  TIMESTAMP '2026-04-22 15:30:00 UTC';
```
