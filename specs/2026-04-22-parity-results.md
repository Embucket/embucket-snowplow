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

## 1/10 source scale (323,559 events, 3-min window)

Both engines complete cleanly (20 PASS each) at this size -- Embucket
no longer OOMs. Parity picture at this scale is much tighter:

- **Rowcounts match exactly** on all three derived tables.
- **Distinct natural-key counts match exactly.**
- **SUMs of all tracked metrics match on users** (page_views, sessions,
  engaged_time_in_s identical).
- **Only remaining numeric divergence**: `sum_absolute_time_in_s` off
  by 1.24% on page_views and 0.81% on sessions -- the same per-row
  ±1-second rounding seen earlier, scaled down.
- **Users table referrer column diverges on NULL-fill**: 12,585
  non-null referrers on Snowflake vs 12,346 on Embucket (18 distinct
  values on both). Root-caused in the compiled
  `snowplow_web_users_this_run` -- `first_domain_sessionid` is
  computed via `MAX(case when start_tstamp = user_start_tstamp then
  domain_sessionid end)`. When two sessions for the same user share
  `start_tstamp` (tie), `MAX(domain_sessionid)` resolves to a different
  UUID ordering on each engine, picking a different first session whose
  referrer value may be NULL or populated. 239 users (~2%) affected.

### After batch 2 at 1/10 scale (MERGE path engaged)

Second load: 286,112 rows at 15:15 load_tstamp burst, total source
now 609,671 rows. Both engines: `dbt run` 20 PASS.

- Rowcounts + distinct keys still match exactly on all three
  derived tables (35,094 users / 35,094 distinct domain_userid on
  both; page_views and sessions also equal).
- `sum_absolute_time_in_s` drift on page_views / sessions compounds
  with the same ±1-sec-per-row pattern.
- **Users table: 3,164 of 35,094 users (9%) have diverging
  aggregates, all in the same direction -- Snowflake accumulates
  more.** `sum_engaged_time_in_s` +6.4% on Snowflake,
  `sum_page_views` +6.4%, `sum_sessions` +6.5%. Direction check:
  `sf_aggregate_larger=3164 emb_aggregate_larger=0`.

#### Drill-down: where does the divergence enter?

Every stage in the lineage before the final MERGE produces
byte-identical aggregates on both engines:

| stage | rowcount | SUM(page_views) | SUM(engaged_time_in_s) |
|-------|----------|-----------------|------------------------|
| `snowplow_web_base_events_this_run` | 609,671 / 609,671 | -- | -- |
| `snowplow_web_base_sessions_this_run` | 75,642 / 75,642 | -- | -- |
| `snowplow_web_sessions_this_run` | 75,482 / 75,482 | -- | -- |
| `snowplow_web_sessions` (derived) | 75,482 / 75,482 | -- | -- |
| `snowplow_web_users_sessions_this_run` | 75,482 / 75,482 | **181,900 both** | **3,298,530 both** |
| `snowplow_web_users_aggs` | 35,094 / 35,094 | **181,900 both** | **3,298,530 both** |
| `snowplow_web_users_this_run` | 35,094 / 35,094 | -- | -- |
| **`snowplow_web_users` (derived)** | 35,094 / 35,094 | **emb 170,884 / sf 181,900** | **emb 3,100,035 / sf 3,298,530** |

The scratch source for the MERGE has identical data on both engines;
the divergence enters purely at the MERGE step.

Per-user spot check for a high-diff user
`eeb8f559-c8d0-4c3b-b50f-e70a11a5d997`:

| source | Embucket | Snowflake |
|--------|----------|-----------|
| scratch `users_this_run` | (39 pv, 16 sess, 535 eng, end=15:14:44) | (39 pv, 16 sess, 535 eng, end=15:14:44) |
| derived `users` after MERGE | **(27 pv, 9 sess, 415 eng, end=15:02:11)** | (39 pv, 16 sess, 535 eng, end=15:14:44) |

The scratch `users_this_run` row on Embucket has the correct
cumulative batch-1+batch-2 values, but after the MERGE INTO the
derived target, Embucket's row still carries the batch-1 values
unchanged. Snowflake's MERGE updates correctly.

The compiled MERGE SQL is **identical** on both engines (the
`patch_snowplow.sh` rewrite makes Embucket dispatch the Snowflake
MERGE macro):

```sql
MERGE INTO <target> AS DBT_INTERNAL_DEST
  USING <__dbt_tmp> AS DBT_INTERNAL_SOURCE
  ON (DBT_INTERNAL_DEST.start_tstamp BETWEEN
        '2026-04-22 14:52:51.530000' AND '2026-04-22 15:15:39.548000')
    AND (DBT_INTERNAL_SOURCE.domain_userid = DBT_INTERNAL_DEST.domain_userid)
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

For the affected user, the target row's `start_tstamp = 14:58:56.280`
is well inside the BETWEEN range and the `domain_userid` is present
in both sides, so Snowflake fires UPDATE and Embucket does not.
Embucket's server-side MERGE UPDATE is either mis-evaluating the ON
clause (fewer matches than expected) or silently no-op'ing the
WHEN MATCHED UPDATE branch for some matching rows -- ~9% of rows
affected on the users MERGE at 609K source events. Surfaces as
Snowflake aggregates strictly >= Embucket aggregates (never the
reverse), consistent with "batch-1 row left unmodified while it
should have been replaced by cumulative batch-1+batch-2 scratch row."

Example users where batch 1 activity is visible on Snowflake but not
Embucket:
```
2696ac4b-...: emb=(1 pv, 1 sess, 15 eng)   sf=(2 pv, 2 sess, 15 eng)
eeb8f559-...: emb=(27 pv, 9 sess, 415 eng) sf=(39 pv, 16 sess, 535 eng)
```

Snowplow's users incremental model MERGEs via `update set col = source.col`
(row-replace), so the source `snowplow_web_users_this_run` must itself
contain cumulative batch-1+batch-2 aggregates per user for cross-batch
users. Snowflake's scratch pipeline is producing that cumulative row;
Embucket's is producing a batch-2-only row, losing the prior batch's
counts on the target update. Root cause is most likely in how
`snowplow_web_base_sessions_this_run` computes the lookback window
(which sessions from the manifest get re-aggregated against new
events) -- worth drilling into on its own, but outside this
investigation's scope.

### 2.83M scale comparison

At 2.83M rows the same effects are magnified (rowcount drifts by
thousands on users, SUMs drift ~10%) and Embucket OOMs the
`snowplow_web_base_events_this_run` repartition. So the 2.83M
divergence is best understood as: same root-cause pipelines, but at
scale the Embucket engine runs out of memory before completion,
and the duplicate-row drift is amplified.

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
