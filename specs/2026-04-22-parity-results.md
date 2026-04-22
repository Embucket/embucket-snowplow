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

### Derived tables diverge in MERGE behavior

- **Snowflake** fails loudly: `100090 (42P18): Duplicate row detected
  during DML action` on `snowplow_web_page_views` and
  `snowplow_web_sessions` merges. ANSI MERGE cannot match multiple
  source rows to one target row when two source rows share the
  unique_key.
- **Embucket** succeeds silently. Final derived tables retain the
  duplicates from the scratch:

| table | rows | distinct unique_key | dup ratio |
|-------|------|---------------------|-----------|
| demo.atomic_derived.snowplow_web_page_views | 814,555 | 775,543 page_view_id | 1.050 |
| demo.atomic_derived.snowplow_web_sessions   | 360,999 | 317,162 domain_sessionid | 1.138 |

This contradicts the model config `unique_key='page_view_id'`
(`domain_sessionid` for sessions) -- Embucket's incremental MERGE is
not deduplicating by unique_key.

## Findings

1. **Glue source has ~4.7% duplicate event_ids**. Snowplow assumes the
   atomic layer is deduplicated upstream; this source violates that
   contract.
2. **Embucket adapter bug: incremental MERGE ignores unique_key when
   the source has duplicates**. Snowflake errors correctly; Embucket
   silently lets duplicate rows through to the derived layer. Worth
   filing upstream.
3. **Snowflake strict-MERGE behavior** is surfacing what is really a
   source data quality problem. Snowplow Web doesn't have a global
   pre-merge `distinct on unique_key` -- the scratch pipelines assume
   the atomic-level dedup already happened.

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
uv run dbt run --profiles-dir . --target dev         # 31 PASS
uv run dbt run --profiles-dir . --target snowflake   # 2 errors on MERGE
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
