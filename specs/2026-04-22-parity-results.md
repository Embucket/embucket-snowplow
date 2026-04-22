# Parity harness run results (2026-04-22)

Commands: `load_from_glue.py init` → `snowflake_setup.py` → batch 1 + dbt + parity → batch 2 + dbt + parity.

## Source (Iceberg events_0416)

Both engines read the same S3 Tables Iceberg snapshot. Source parity holds
after each Athena insert + Snowflake refresh.

| batch | embucket | snowflake |
|-------|----------|-----------|
| 1 (15:00–15:30) | 2,834,944 | 2,834,944 |
| 2 (15:30–16:00) | 6,162,099 | 6,162,099 |

## Derived tables — findings

Rowcount-only comparison (hash-diff skipped when rowcounts differ).

### After batch 1

| table | embucket | snowflake | ratio |
|-------|----------|-----------|-------|
| snowplow_web_page_views | 1,338,846 | 814,564 | 1.64× |
| snowplow_web_sessions | 553,795 | 360,999 | 1.53× |
| snowplow_web_users | 233,818 | 122,041 | 1.92× |

### After batch 2

| table | embucket | snowflake | notes |
|-------|----------|-----------|-------|
| snowplow_web_page_views | 2,370,157 | 814,564 | **Snowflake unchanged** — batch 2 incremental did not advance |
| snowplow_web_sessions | 1,099,618 | 360,999 | **Snowflake unchanged** |
| snowplow_web_users | 15,437,123 | 122,041 | Embucket 15.4M >> 6.16M input events — suspicious |

## Interpretation

1. Snowflake batch-2 run rebuilt `_this_run` scratch tables but did not
   advance `snowplow_web_{page_views,sessions,users}`. The
   `snowplow_web_incremental_manifest` rows for those models show
   `last_success = 2026-04-22 15:26:32` both before and after the batch-2
   run, while all scratch models advanced to 15:56:33. Likely cause: the
   `snowplow_utils.is_run_with_new_events('snowplow_web')` guard evaluated
   false despite new data being present. Worth isolating; not a harness bug.

2. Embucket batch-2 produced 15.4M user rows against 6.16M input events —
   more rows than input. Likely unique-key collision or duplicated merge
   in the users incremental. Needs engine-side investigation.

3. Source parity is byte-for-byte between Embucket and Snowflake, so the
   divergence is purely in the dbt model execution, not in how the two
   engines read the Iceberg data.

## Harness status

Working as intended. The comparison flow (init → setup → insert → refresh
→ dbt × 2 → parity) runs end-to-end on fresh infrastructure and surfaces
the divergences cleanly. No blockers.
