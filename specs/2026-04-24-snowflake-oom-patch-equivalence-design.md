# Verifying semantic equivalence of the OOM-mitigation dbt patch on Snowflake

## Problem

`dbt_packages.bak.1776896708/` contains a hand-patched copy of the
Snowplow dbt models (`snowplow_web`, `snowplow_utils`) that rewrites
four memory-heavy queries into eleven narrow, materialized steps so
they run on Embucket/DataFusion without OOM. The rewrite preserves
upstream semantics only if the chained scratch models compute the same
final aggregates as the original monolithic queries.

The patch is ephemeral: it lives only in `.bak`, and `dbt deps`
restores the unmodified upstream `dbt_packages/`.

Goal: prove, on Snowflake, that the patched pipeline and the upstream
pipeline produce byte-identical golden tables
(`snowplow_web_page_views`, `snowplow_web_sessions`,
`snowplow_web_users`) across both a full-refresh run and an
incremental run, using the same source data.

## Strategy

Snowflake is the oracle: it runs both variants successfully and has
no memory pressure, so any diff comes from SQL semantics, not engine
behaviour. Run the full pipeline four times in a single ordered flow.

```
init source + batch 1
    baseline --full-refresh    → clone atomic_derived to atomic_derived_baseline_fr
batch 2
    baseline (incremental)     → clone atomic_derived to atomic_derived_baseline_inc

init source + batch 1          (reset to identical starting state)
apply OOM patch
    patched  --full-refresh    → clone atomic_derived to atomic_derived_patched_fr
batch 2
    patched  (incremental)     → clone atomic_derived to atomic_derived_patched_inc

diff (baseline_fr,  patched_fr)
diff (baseline_inc, patched_inc)
```

The source Iceberg table is Athena-managed and safe to recreate; data
comes from `awsdatacatalog.analytics_glue.hooli_events_0417_v2` via
`scripts/events_0416_select.sql`. `load_from_glue.py init` is
deterministic against that Glue table for a fixed `[start, end)`
window.

Batches reuse the canonical README windows:

- batch 1: `2026-04-22 15:00:00` to `2026-04-22 15:30:00`
- batch 2: `2026-04-22 15:30:00` to `2026-04-22 16:00:00`

Snapshotting via `CREATE SCHEMA … CLONE` is a Snowflake zero-copy
metadata op; it costs nothing and lets all four output sets coexist
for diffing.

## Components

### 1. `patches/` — git-tracked patched model tree

Mirror structure of `dbt_packages/`. Holds the exact files from
`dbt_packages.bak.1776896708/` that differ from upstream:

```
patches/
  snowplow_web/models/sessions/scratch/snowflake/
      snowplow_web_first_event_ids_this_run.sql       (new)
      snowplow_web_last_event_ids_this_run.sql        (new)
      snowplow_web_session_firsts_this_run.sql        (new)
      snowplow_web_session_lasts_this_run.sql         (new)
      snowplow_web_session_base_aggs_this_run.sql     (new)
      snowplow_web_session_pv_set_this_run.sql        (new)
      snowplow_web_session_pv_counts_this_run.sql     (new)
      snowplow_web_session_pp_bucket_set_this_run.sql (new)
      snowplow_web_session_pp_counts_this_run.sql     (new)
      snowplow_web_session_aggs_this_run.sql          (rewritten)
      snowplow_web_sessions_this_run.sql              (rewritten)
  snowplow_web/models/page_views/scratch/snowflake/
      snowplow_web_pv_dedup_this_run.sql              (new)
      snowplow_web_page_views_this_run.sql            (rewritten)
  snowplow_web/models/user_mapping/
      snowplow_web_user_mapping.sql                   (rewritten)
  snowplow_utils/macros/base/
      base_create_snowplow_events_this_run.sql        (rewritten)
```

Files are copied verbatim from `.bak`. The dirty diffs noted in the
earlier analysis (the `get_value_by_target_type.sql` and optional-
module `target.type` edits) are NOT in this patch — they belong to a
different concern (embucket adapter recognition) and are handled by
the existing `patch_snowplow.sh`.

### 2. `scripts/apply_oom_patch.sh` — patch applier

```
#!/usr/bin/env bash
set -euo pipefail
# 1. Apply the pre-existing embucket-target-type sed patch.
./scripts/patch_snowplow.sh
# 2. Overlay the OOM-mitigation decomposed models.
rsync -a --checksum patches/ dbt_packages/
echo "Applied OOM patch to dbt_packages/"
```

Idempotent. `rsync -a --checksum` compares content not mtime, so
repeat runs are no-ops. Survives `dbt deps` because `patches/` is in
the repo; re-run after any `dbt deps`.

### 3. `scripts/parity_self.py` — Snowflake-only two-schema diff

Fork of `scripts/parity.py` with the following changes:

- Drop the Embucket path entirely; only uses
  `snowflake.connector.connect(connection_name="default")`.
- Takes `--left` and `--right` arguments naming two schemas in
  `sturukin_db` (e.g. `atomic_derived_baseline_fr` and
  `atomic_derived_patched_fr`).
- Keeps the three `TableSpec` entries from `parity.py` unchanged
  except that `model_tstamp` is removed from each `columns` list.
  `model_tstamp` is `current_timestamp()` at dbt run time and will
  always differ between the two pipelines; its presence would produce
  100% row mismatches and drown the signal.
- Same methodology: per-table rowcount check, then per-row MD5 hash
  keyed on the natural key, with a summary of matched / mismatched /
  only-left / only-right rows.
- Exits non-zero on any mismatch.

### 4. `scripts/verify_oom_patch.sh` — orchestrator

```
#!/usr/bin/env bash
set -euo pipefail

BATCH1_START='2026-04-22 15:00:00'
BATCH1_END='2026-04-22 15:30:00'
BATCH2_START='2026-04-22 15:30:00'
BATCH2_END='2026-04-22 16:00:00'

run_pipeline_stage() {
  local label=$1 refresh_flag=$2 suffix=$3
  if [[ -n $refresh_flag ]]; then
    uv run dbt run --profiles-dir . --target snowflake --full-refresh
  else
    uv run dbt run --profiles-dir . --target snowflake
  fi
  uv run python scripts/snowflake_clone_schema.py \
      --src sturukin_db.atomic_derived \
      --dst sturukin_db.atomic_derived_${label}_${suffix}
}

# --- BASELINE ---------------------------------------------------------
rm -rf dbt_packages && uv run dbt deps --profiles-dir .
./scripts/patch_snowplow.sh   # embucket target-type only; no OOM patch

uv run python scripts/load_from_glue.py init
uv run python scripts/load_from_glue.py insert \
    --start "$BATCH1_START" --end "$BATCH1_END"
uv run python scripts/snowflake_refresh.py
run_pipeline_stage baseline --full-refresh fr

uv run python scripts/load_from_glue.py insert \
    --start "$BATCH2_START" --end "$BATCH2_END"
uv run python scripts/snowflake_refresh.py
run_pipeline_stage baseline "" inc

# --- PATCHED ----------------------------------------------------------
./scripts/apply_oom_patch.sh

uv run python scripts/load_from_glue.py init
uv run python scripts/load_from_glue.py insert \
    --start "$BATCH1_START" --end "$BATCH1_END"
uv run python scripts/snowflake_refresh.py
run_pipeline_stage patched --full-refresh fr

uv run python scripts/load_from_glue.py insert \
    --start "$BATCH2_START" --end "$BATCH2_END"
uv run python scripts/snowflake_refresh.py
run_pipeline_stage patched "" inc

# --- DIFF -------------------------------------------------------------
uv run python scripts/parity_self.py \
    --left  sturukin_db.atomic_derived_baseline_fr \
    --right sturukin_db.atomic_derived_patched_fr
uv run python scripts/parity_self.py \
    --left  sturukin_db.atomic_derived_baseline_inc \
    --right sturukin_db.atomic_derived_patched_inc
```

Before baseline, we force-delete `dbt_packages/` and re-run `dbt deps`
to guarantee upstream state (a previous `apply_oom_patch.sh` run would
otherwise persist).

### 5. `scripts/snowflake_clone_schema.py` — zero-copy snapshot helper

```
CREATE OR REPLACE SCHEMA <dst> CLONE <src>;
```

One `snowflake.connector` call, parameters `--src` and `--dst` as
fully-qualified `database.schema`. Zero-copy, metadata-only.

## Data flow

```
Glue: hooli_events_0417_v2           (read-only; NEVER written)
  ↓ Athena projection
S3 Tables Iceberg: atomic.events_0416 (shared, recreated per variant)
  ↓ Snowflake iceberg external ref
sturukin_db.atomic.events_0416
  ↓ dbt (baseline or patched)
sturukin_db.atomic_derived.{snowplow_web_page_views,sessions,users}
  ↓ CLONE
sturukin_db.atomic_derived_<variant>_<fr|inc>.*
  ↓ parity_self.py
diff report
```

## Error handling

- `apply_oom_patch.sh` aborts if `dbt_packages/` is missing (caller
  must `dbt deps` first).
- `snowflake_clone_schema.py` uses `CREATE OR REPLACE`; reruns are
  idempotent. Caller responsible for cleanup if they want to reclaim
  catalog entries.
- `parity_self.py` exits 1 on any diff. `verify_oom_patch.sh` runs
  under `set -e`, so the first diff halts the run; this is correct —
  once baseline_fr vs patched_fr differs, we want to investigate
  before the incremental run is judged on top of divergent state.

## Explicit non-goals

- No automated cleanup of `atomic_derived_*` schemas. Zero-copy clones
  are cheap; leaving them aids post-mortem investigation.
- No column-level diff tooling beyond natural-key row hashes.
  If diffs appear, follow-up is manual SQL.
- No cross-variant interleaving (e.g. running both pipelines on batch
  1, comparing, then both on batch 2). The sequential approach above
  is simpler and produces the same equivalence signal.
- No change to `parity.py`. The existing Embucket-vs-Snowflake harness
  stays untouched.
- The existing `.bak.1776896708/` is left in place as a reference
  snapshot and will not be moved or deleted.

## Success criterion

Both `parity_self.py` invocations exit 0:

- `atomic_derived_baseline_fr` matches `atomic_derived_patched_fr` on
  rowcount and per-row hash for all three golden tables, ignoring
  `model_tstamp`.
- Same for `_inc`.

If either fails, the mismatch summary (per-table matched /
mismatched / only-left / only-right counts plus up to ten example
natural keys per bucket) identifies where semantic divergence exists.
