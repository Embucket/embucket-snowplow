# OOM-mitigation patch on Embucket: neither upstream nor current patch runs

## Verdict

Both variants fail on Embucket at the same model,
`snowplow_web_base_events_this_run`, with the same DataFusion resource
exhaustion. The Snowflake-verified patch set (iteration 2) is
**semantically correct but not Embucket-viable** — making it
Embucket-viable requires additional work on the event_id dedup step
that iteration 2 deliberately avoided.

| variant                   | result                                   |
|---------------------------|------------------------------------------|
| upstream (no patch)       | FAIL — OOM in `snowplow_web_base_events_this_run` |
| iteration-2 patched       | FAIL — OOM in the SAME model, same cause |

Error on both (abbreviated):

```
Resources exhausted: Failed to allocate additional 5.0 MB for RepartitionExec[4]
with 13.5 MB already allocated for this reservation
- 1348.8 KB remain available for the total pool
```

Source: `demo.atomic.events_0416` with 2,834,944 rows (batch 1 only).
Embucket Lambda config: `MEM_POOL_SIZE_MB=2048`, greedy pool.

## Why both failed

The upstream macro
`snowplow_utils/macros/base/base_create_snowplow_events_this_run.sql`
ends with:

```
qualify row_number() over (partition by a.event_id
                           order by a.collector_tstamp,
                                    a.dvce_created_tstamp) = 1
```

`a.*` expands to the full 137+ column row inside the QUALIFY scope, so
the row_number() window buffers 137 columns per partition. On a 2.8M
input it exceeds DataFusion's 2 GB memory pool via `RepartitionExec`
during the qualify's shuffle.

Iteration 1 of the Apr-10 stash removed this QUALIFY entirely, which
avoided the OOM but broke semantics (the 2.18% duplicate event_ids in
the source started double-counting downstream, inflating users by
+71%). Iteration 2 reverted that macro patch — restoring upstream
semantics at the cost of restoring the Embucket OOM.

The iteration-2 patch set relieves memory pressure in the
sessions/page_views/user_mapping layers (decomposed into
narrow-projection scratch models + inner-joins), but it leaves the
base-events macro untouched. The OOM site is therefore reached before
any of those improvements matter.

## What's needed to make the patch Embucket-viable

The narrow-scratch pattern the stash already uses for page-view and
session dedup needs to be extended to event_id dedup. The shape that
WOULD work (a three-model decomposition):

1. `snowplow_web_base_events_raw_this_run` - wide, materialized=table,
   emitted by the macro with the QUALIFY removed. May contain
   duplicate event_ids.
2. `snowplow_web_base_events_winners_this_run` - narrow,
   materialized=table. `SELECT event_id, collector_tstamp,
   dvce_created_tstamp FROM raw QUALIFY row_number() OVER (...) = 1`.
   Spillable because projection is 3 columns.
3. `snowplow_web_base_events_this_run` - wide, materialized=table.
   Inner-joins `_raw` to `_winners`. **Caveat**: a naive inner join on
   `event_id` alone is NOT a dedup - if two raw rows share an event_id
   they both still match. A correct join needs a row-uniquely-
   determining key. Options:

   - **Hash-based**: add `HASH(event_id, collector_tstamp,
     dvce_created_tstamp, load_tstamp, ...)` to both sides; join on
     `(event_id, row_hash)`. Cheap, narrow, deterministic.
   - **Aggregated winner**: in `_winners` pick one full set of
     metadata per event_id via `MIN(hash) GROUP BY event_id`; join on
     `(event_id, hash)`.

   Materialisation between each step gives DataFusion a fresh memory
   pool and keeps the QUALIFY on a narrow projection.

An earlier attempt in this branch tried a simpler wrapper-level
`INNER JOIN event_id_dedup ON event_id = event_id`; that was wrong for
the reason above (it filters but doesn't dedup) and was reverted. The
correct solution is the three-model decomposition described here.

## What to report back

- The verification infrastructure for Embucket now exists:
  `scripts/embucket_reset_manifest.py`, `scripts/embucket_snapshot_derived.py`,
  `scripts/parity_self_embucket.py`. These are useful as soon as the
  base-events dedup is re-engineered.
- Iteration 2's patches are correct and merged for the Snowflake
  story. Landing Embucket requires one more iteration specifically on
  the base-events model, following the design sketch above.
- The existing `scripts/parity.py` is still the right tool for cross-
  engine comparison (Embucket patched vs Snowflake upstream) once the
  Embucket side actually runs.
