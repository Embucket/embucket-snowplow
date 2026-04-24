# OOM-mitigation dbt patch is NOT semantically equivalent to upstream

## Verdict

The decomposed `snowplow_web` / `snowplow_utils` models in
`dbt_packages.bak.1776896708/` (snapshot Apr 10) do NOT produce the
same golden tables as the upstream models when run on the same input.
The patched pipeline produces more rows in all three golden tables on
Snowflake where memory is not a constraint.

Full-refresh parity on identical input (6,162,099 source events):

| table                    | baseline  | patched   | Δ rows  | Δ %    |
|--------------------------|-----------|-----------|---------|--------|
| snowplow_web_page_views  |  775,543  |  814,570  | +39,027 |  +5.0% |
| snowplow_web_sessions    |  317,162  |  360,999  | +43,837 | +13.8% |
| snowplow_web_users       |   71,352  |  122,041  | +50,689 | +71.0% |

Incremental parity is not informative in this experiment (see
"Incremental run caveat" below); the full-refresh numbers carry the
full signal.

## Root cause

`patches/snowplow_utils/macros/base/base_create_snowplow_events_this_run.sql`
removes the upstream event_id dedup:

```
# upstream (baseline)
qualify row_number() over (partition by a.event_id
                           order by a.{{ session_timestamp }},
                                    a.dvce_created_tstamp) = 1

# patched
(removed; comment: "Dedup is handled by downstream models which
 each dedup by their own key (page_view_id, domain_sessionid,
 domain_userid) on the much smaller batch-sized
 base_events_this_run table.")
```

The source data has duplicate event_ids. On our 6.16M-row batch:

```
events_0416 total rows        : 6,162,099
events_0416 distinct event_ids: 6,027,734
duplicate event_ids           :   134,365   (2.18% of input)
```

These duplicates are Snowplow collector retries or client replays —
two rows with the same `event_id` but potentially different
`collector_tstamp`, `derived_tstamp`, or enrichment columns.

The patch's rationale ("downstream models dedup by their own key") is
only partially true. It holds for `snowplow_web_pv_dedup_this_run`
(deduplicates on `page_view_id`) and for the session `_firsts`/`_lasts`
narrow QUALIFY variants (deduplicate on `domain_sessionid`). It does
NOT hold for everything downstream of the base model — in particular:

- `snowplow_web_session_aggs_this_run` does `COUNT(*)` and
  `COUNT(DISTINCT page_view_id || bucket)` grouped by
  `domain_sessionid`. Duplicate event rows inflate `total_events`
  and, crucially, add spurious `(page_view, heartbeat_bucket)`
  combinations that inflate `engaged_time_in_s`.
- `snowplow_web_users_this_run` is driven from
  `snowplow_web_users_sessions_this_run`, which aggregates over
  `snowplow_web_sessions_this_run`. Each duplicate session row
  propagates into extra users rows (users table has 1.71 rows per
  distinct domain_userid in the patched output).
- `snowplow_web_users_lasts` uses `qualify row_number() partition by
  domain_userid` but with ties on `collector_tstamp` caused by
  duplicate events, the tie-break picks one row per (user, tstamp),
  not per user — leaving ~25k extra rows.

## Not a root cause

The other three rewrites (page_views narrow-QUALIFY, session
`_firsts`/`_lasts` narrow-QUALIFY, user_mapping `last_value` →
`row_number`) are semantically equivalent to upstream. The whole
row-count divergence traces back to the single decision to drop
event_id dedup in `base_create_snowplow_events_this_run.sql`.

## What the patch should have done

Preserve event_id dedup, but do it in a memory-safe way. The same
"extract a narrow projection, inner-join back" pattern the patch
already uses for page_view dedup and session firsts/lasts applies
cleanly here:

```sql
-- snowplow_web_base_event_ids_this_run (new, narrow, materialized=table):
select a.event_id
from {{ source('atomic', 'events') }} a
inner join {{ ref('snowplow_web_base_sessions_this_run') }} b
  on a.domain_sessionid = b.session_identifier
where a.{{ session_timestamp }} <= b.end_tstamp
  ...other upstream filters...
qualify row_number() over (
  partition by a.event_id
  order by a.{{ session_timestamp }}, a.dvce_created_tstamp
) = 1
```

Then the rewritten `base_create_snowplow_events_this_run.sql` adds an
`INNER JOIN snowplow_web_base_event_ids_this_run USING (event_id)`.
The QUALIFY runs on 4 narrow columns (event_id, session_timestamp,
dvce_created_tstamp, domain_sessionid) — same idea as
`snowplow_web_pv_dedup_this_run`.

This is a two-line change once the scratch model is added, and
restores row-count parity without bringing back the OOM.

## Incremental run caveat

The full-refresh numbers above are directly comparable. The
incremental numbers collected in this experiment are NOT, because
the Snowplow dbt package stores its own state in
`sturukin_db.atomic_snowplow_manifest.snowplow_web_incremental_manifest`
(a cross-schema singleton), and `dbt run --full-refresh` does not
reset it for every model — in practice the manifest carries state
across pipeline variants. The patched full-refresh run therefore
inherited the baseline's "last-processed timestamp" and processed
almost nothing on the second (incremental) dbt invocation:

| table                    | baseline_inc | patched_inc |
|--------------------------|-------------:|------------:|
| snowplow_web_page_views  |    1,731,722 |     814,570 |
| snowplow_web_sessions    |      704,107 |     360,999 |
| snowplow_web_users       |      148,357 |     122,041 |

`patched_inc` equals `patched_fr` exactly — the patched incremental
dbt run did not process batch 2. This is an artifact of the
experimental harness, not a property of the patch. For a clean
incremental comparison the manifest table would need to be truncated
between variants.

## Evidence artifacts

All four snapshots remain in Snowflake as zero-copy clones, available
for any further investigation:

```
sturukin_db.atomic_derived_baseline_fr    -- upstream, full-refresh on batch 1
sturukin_db.atomic_derived_baseline_inc   -- upstream, incremental after batch 2
sturukin_db.atomic_derived_patched_fr     -- patched,  full-refresh on batch 1
sturukin_db.atomic_derived_patched_inc    -- patched,  incremental after batch 2
                                             (see caveat above — near-identical to _fr)
```

The patch mechanism itself works cleanly:

- `patches/` (git-tracked) holds the 15 patched files.
- `scripts/apply_oom_patch.sh` overlays them via rsync onto a fresh
  `dbt_packages/` and is idempotent.
- `scripts/parity_self.py` performs Snowflake-to-Snowflake diffs with
  the same rowcount + row-hash methodology as the existing
  Embucket-vs-Snowflake `parity.py`, with `model_tstamp` excluded.
- `scripts/verify_oom_patch.sh` orchestrates the four-run flow
  end-to-end.

## Recommendation

Do NOT merge the Apr 10 stash as-is. Before landing the decomposed
models, add a narrow-projection `snowplow_web_base_event_ids_this_run`
scratch model to restore event_id dedup, and re-run this same parity
harness. Expected outcome: all three golden tables match byte-for-byte
with model_tstamp excluded.
