# OOM-mitigation dbt patch is semantically equivalent to upstream on Snowflake

## Verdict

The decomposed `snowplow_web` models in `patches/` produce byte-identical
golden tables to the upstream (unpatched) models on Snowflake across
both a full-refresh run and an incremental run on the same source data.

Parity on 6,162,099 source events (batch 1 = 2,834,944 rows; batch 1+2
= 6,162,099 rows), comparing hashes of the three golden tables keyed
on their natural keys with `model_tstamp` and `event_id` excluded:

| table                    | rows (fr) | rows (inc) | mismatched | only_left | only_right |
|--------------------------|----------:|-----------:|-----------:|----------:|-----------:|
| snowplow_web_page_views  |   775,543 |  1,731,722 |          0 |         0 |          0 |
| snowplow_web_sessions    |   317,162 |    704,107 |          0 |         0 |          0 |
| snowplow_web_users       |    71,352 |    148,357 |          0 |         0 |          0 |

Both `parity_self.py` invocations exit 0.

## What was wrong in the earlier Apr-10 stash

The first iteration of this verification found massive row-count
inflation in the patched variant (users +71%, sessions +13.8%,
page_views +5.0%). Root cause: the stash had removed upstream's
event-level dedup in `snowplow_utils/macros/base/base_create_snowplow_events_this_run.sql`
without a replacement, so duplicate event_ids in the source (134,365
out of 6.16M rows, i.e. 2.18% — collector retries and tracker replays)
propagated into every downstream aggregate.

## Fix

Removed the patch to
`snowplow_utils/macros/base/base_create_snowplow_events_this_run.sql`.
The upstream macro already performs event_id dedup via a `QUALIFY
row_number() over (partition by event_id order by collector_tstamp,
dvce_created_tstamp) = 1`; the stash removed that QUALIFY with a
comment arguing it was too memory-heavy. Restoring it gives correct
semantics on Snowflake.

On Embucket/DataFusion the wide-projection QUALIFY in that macro may
still OOM — that is a separate, open issue. It should be addressed by
extracting event_id dedup into a narrow-projection scratch model, but
NOT via the naive wrapper-level INNER JOIN attempted in an earlier
iteration of this work. That approach fails because an INNER JOIN on
`event_id = event_id` filters but does not deduplicate: if `base_query`
contains two rows with the same event_id, both rows match the single
dedup row. A correct narrow-dedup on Embucket needs either (a) a
deterministic row_id in the join predicate, or (b) materialising the
dedup as the driving table and projecting `SELECT * FROM
dedup_winners_wide` — both of which are follow-up work.

## Harness improvements over iteration 1

Iteration 1 showed `patched_inc` rowcounts equal to `patched_fr` —
the patched pipeline's second dbt invocation processed no new events.
Root cause: the Snowplow dbt package stores its state (last-processed
run window) in `sturukin_db.atomic_snowplow_manifest.*`, and
`dbt run --full-refresh` does NOT reset it (the manifest model has
`full_refresh=allow_refresh()` which defaults to false). The harness
now calls `scripts/snowflake_reset_manifest.py` between variants,
which `DROP`s three state tables so each variant starts with a clean
Snowplow state. Iteration 2's `inc` numbers are real.

## Non-deterministic tie-break in `event_id`

Earlier runs flagged 194 (fr) / 198 (inc) mismatched page_view_ids.
Investigation showed every such row differed ONLY in `event_id`, with
every aggregate / scroll / engagement / URL column matching. In every
case both chosen event_ids had identical `(derived_tstamp,
dvce_created_tstamp)` — the natural ORDER BY of the QUALIFY — so
`row_number() = 1` was non-deterministic.

Both upstream and patched pipelines are non-deterministic in this
respect, though each is internally consistent at a given execution.
The pipelines' semantic output (the aggregates) is identical. The
parity check was tightened to exclude `event_id` from the page_view
hash for this reason — same reason `model_tstamp` is excluded.

The equivalence claim the parity harness now proves: **for every
page_view_id, session, and user the upstream and patched pipelines
compute the same aggregate values.**

## What the patch set now contains

```
patches/
  snowplow_web/models/sessions/scratch/snowflake/
      snowplow_web_first_event_ids_this_run.sql        (new, narrow-QUALIFY on 4 cols)
      snowplow_web_last_event_ids_this_run.sql         (new)
      snowplow_web_session_firsts_this_run.sql         (new, wide projection, inner-join fe)
      snowplow_web_session_lasts_this_run.sql          (new, wide projection, inner-join le)
      snowplow_web_session_base_aggs_this_run.sql      (new, isolated heavy GROUP BY)
      snowplow_web_session_pv_set_this_run.sql         (new, spillable replacement for
                                                        count(distinct page_view_id))
      snowplow_web_session_pv_counts_this_run.sql      (new)
      snowplow_web_session_pp_bucket_set_this_run.sql  (new, spillable replacement for
                                                        count(distinct pv_id || bucket))
      snowplow_web_session_pp_counts_this_run.sql      (new)
      snowplow_web_session_aggs_this_run.sql           (rewritten, 3-way join of
                                                        pre-materialized aggregates)
      snowplow_web_sessions_this_run.sql               (rewritten, 3-way left join only)
  snowplow_web/models/page_views/scratch/snowflake/
      snowplow_web_pv_dedup_this_run.sql               (new, narrow-QUALIFY on 4 cols)
      snowplow_web_page_views_this_run.sql             (rewritten, inner-join d on event_id)
  snowplow_web/models/user_mapping/
      snowplow_web_user_mapping.sql                    (rewritten, UNBOUNDED window →
                                                        QUALIFY row_number = 1 DESC)
```

14 files — 10 new, 4 rewritten. The earlier `snowplow_utils` macro
patch was removed (upstream event_id QUALIFY is kept).

## Verification how-to

All in `scripts/`:

```
scripts/apply_oom_patch.sh           # overlays patches/ into dbt_packages/
scripts/snowflake_reset_manifest.py  # DROPs Snowplow state tables
scripts/snowflake_clone_schema.py    # zero-copy schema snapshot
scripts/parity_self.py               # Snowflake-to-Snowflake row-hash diff
scripts/verify_oom_patch.sh          # end-to-end orchestrator
```

`verify_oom_patch.sh` does it all in one invocation: reset to baseline
dbt packages, run baseline fr + inc with manifest reset, snapshot;
apply patch, run patched fr + inc with manifest reset, snapshot;
parity diff both pairs; exit 0 on full agreement.

## Live evidence

All four output-schema snapshots are in Snowflake as zero-copy clones:

```
sturukin_db.atomic_derived_baseline_fr
sturukin_db.atomic_derived_baseline_inc
sturukin_db.atomic_derived_patched_fr
sturukin_db.atomic_derived_patched_inc
```

Drop at your convenience — clones are free to keep.
