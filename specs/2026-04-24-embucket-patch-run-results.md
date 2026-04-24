# OOM-mitigation patch: runs on both Embucket and Snowflake, Snowflake-equivalent

## Verdict

After a third iteration of the patch set the pipeline runs to
completion on Embucket Lambda (2 GB memory pool, DataFusion engine)
AND remains byte-equivalent to upstream on Snowflake.

### Embucket
Both full-refresh and incremental dbt runs complete successfully:

| phase                 | result                     | duration |
|-----------------------|----------------------------|----------|
| full-refresh, batch 1 | PASS 33/33, ERROR 0        |  3m 5s   |
| incremental, batch 2  | PASS 33/33, ERROR 0        |  8m 52s  |

Rowcounts (Embucket patched):

| table                    | rows (fr)  | rows (inc) |
|--------------------------|-----------:|-----------:|
| snowplow_web_page_views  |   775,543  |  1,728,842 |
| snowplow_web_sessions    |   317,162  |    704,037 |
| snowplow_web_users       |    71,352  |    148,357 |

### Snowflake self-parity (baseline vs patched, new patch set)

Both `parity_self.py` invocations exit 0.

| table                    | rows (fr)  | rows (inc) | fr diffs | inc diffs |
|--------------------------|-----------:|-----------:|---------:|----------:|
| snowplow_web_page_views  |   775,543  |  1,731,722 |        0 |         0 |
| snowplow_web_sessions    |   317,162  |    704,107 |        0 |         0 |
| snowplow_web_users       |    71,352  |    148,357 |        0 |         0 |

### Cross-engine rowcount comparison (Embucket patched vs Snowflake patched)

Full-refresh: exact match on all three tables.
Incremental: small drift on page_views (-2,880, 0.17%) and sessions
(-70, 0.01%); users exact. This is a cross-engine incremental-window
timing artefact (the Snowplow package's `current_timestamp()`-bounded
upper_limit differed between the two invocations run minutes apart),
not a patch semantics issue — the Snowflake self-parity remains 0 diffs.

## What changed from iteration 2

Iteration 2's patch set kept upstream's event_id QUALIFY in the
base-events macro (to preserve Snowflake semantics) and failed with
`RepartitionExec` resource exhaustion on Embucket. Iteration 3 replaces
that single wide-column QUALIFY with a three-model narrow-scratch chain:

1. `snowplow_web_base_events_raw_this_run` - materialized=table, wide
   projection from the macro with the QUALIFY removed. May contain
   duplicate event_ids.
2. `snowplow_web_base_events_winners_this_run` - materialized=table,
   narrow (2 columns): `(event_id, winner_hash)`. One row per event_id.
   `winner_hash = MD5(concat_ws('|', event_id, collector_tstamp,
   dvce_created_tstamp, load_tstamp))`.
3. `snowplow_web_base_events_this_run` - materialized=table, the final
   INNER JOIN of `_raw` against `_winners` on `(event_id, winner_hash)`,
   producing exactly one wide row per event_id.

Each hop materialises, so DataFusion's memory pool resets between
steps; the memory-bounded QUALIFY runs on a 3-column projection rather
than 137.

Why MD5 and not `HASH()`: Embucket (DataFusion) rejects Snowflake's
`HASH()` function with "Function 'hash' is not implemented yet".
`MD5(CONCAT_WS('|', ...))` is portable across both engines.

Why the inner join on `(event_id, winner_hash)` and not on `event_id`
alone: if `_raw` has two rows with the same event_id, a join on
`event_id` alone matches both (filter, not dedup). Matching on the
hash of the full ORDER BY tuple uniquely identifies the chosen
"winner" row.

## Harness state (all committed on this branch)

- `patches/` - 18 patch files (14 from iteration 2 + 4 base-events
  files added in iteration 3).
- `scripts/apply_oom_patch.sh` - idempotent rsync, survives `dbt deps`.
- Snowflake verification: `scripts/verify_oom_patch.sh`,
  `scripts/parity_self.py`, `scripts/snowflake_clone_schema.py`,
  `scripts/snowflake_reset_manifest.py`.
- Embucket verification: `scripts/parity_self_embucket.py`,
  `scripts/embucket_snapshot_derived.py` (CTAS; Iceberg has no CLONE),
  `scripts/embucket_reset_manifest.py`.

## Live evidence

Snapshots live in both engines; zero-copy on Snowflake, CTAS copies
on Embucket:

```
sturukin_db.atomic_derived_{baseline,patched}_{fr,inc}   -- Snowflake
demo.atomic_derived_patched_{fr,inc}                     -- Embucket
```

The baseline snapshots on Snowflake cover the upstream (un-patched)
side; Embucket does not have a baseline snapshot because upstream
does not run there.
