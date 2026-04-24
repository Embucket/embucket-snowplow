#!/usr/bin/env bash
# End-to-end verification that the OOM-mitigation patch to snowplow-web dbt
# models produces semantically identical golden tables on Snowflake.
#
#   baseline   = upstream snowplow-web models (as installed by dbt deps)
#   patched    = baseline + patches/ overlay via scripts/apply_oom_patch.sh
#
# Flow:
#   1. dbt deps + embucket-target-type patch (baseline state).
#   2. init source, load batch 1, snowflake refresh.
#   3. dbt run --full-refresh, clone output -> atomic_derived_baseline_fr.
#   4. load batch 2, snowflake refresh.
#   5. dbt run (incremental), clone output -> atomic_derived_baseline_inc.
#   6. Apply OOM patch.
#   7. Repeat 2-5 with clones -> atomic_derived_patched_{fr,inc}.
#   8. parity_self.py on both (fr, inc) pairs.
#
# Source data: hooli_events_0417_v2 in Glue (READ-ONLY, never written).
# Athena-managed atomic.events_0416 on S3 Tables is recreated between variants.
set -euo pipefail

BATCH1_START='2026-04-22 15:00:00'
BATCH1_END='2026-04-22 15:30:00'
BATCH2_START='2026-04-22 15:30:00'
BATCH2_END='2026-04-22 16:00:00'

DERIVED_SRC='sturukin_db.atomic_derived'

dbt_run_stage() {
  local full_refresh=$1
  if [[ $full_refresh == 1 ]]; then
    uv run dbt run --profiles-dir . --target snowflake --full-refresh
  else
    uv run dbt run --profiles-dir . --target snowflake
  fi
}

clone_to() {
  local dst=$1
  uv run python scripts/snowflake_clone_schema.py \
      --src "$DERIVED_SRC" --dst "$dst"
}

load_batch() {
  local start=$1 end=$2
  uv run python scripts/load_from_glue.py insert --start "$start" --end "$end"
  uv run python scripts/snowflake_refresh.py
}

reset_source() {
  uv run python scripts/load_from_glue.py init
  # After Athena drops + recreates events_0416 its S3 Tables location changes,
  # so we must re-register the Snowflake iceberg table and re-grant LF perms.
  uv run python scripts/snowflake_setup.py
  load_batch "$BATCH1_START" "$BATCH1_END"
}

# --- BASELINE ---------------------------------------------------------
echo "=== baseline: fresh dbt_packages ==="
rm -rf dbt_packages
uv run dbt deps --profiles-dir .
./scripts/patch_snowplow.sh   # embucket target-type only

echo "=== baseline: init source + batch 1 + full-refresh ==="
reset_source
dbt_run_stage 1
clone_to "sturukin_db.atomic_derived_baseline_fr"

echo "=== baseline: batch 2 + incremental ==="
load_batch "$BATCH2_START" "$BATCH2_END"
dbt_run_stage 0
clone_to "sturukin_db.atomic_derived_baseline_inc"

# --- PATCHED ----------------------------------------------------------
echo "=== patched: apply OOM patch ==="
./scripts/apply_oom_patch.sh

echo "=== patched: reset source + full-refresh ==="
reset_source
dbt_run_stage 1
clone_to "sturukin_db.atomic_derived_patched_fr"

echo "=== patched: batch 2 + incremental ==="
load_batch "$BATCH2_START" "$BATCH2_END"
dbt_run_stage 0
clone_to "sturukin_db.atomic_derived_patched_inc"

# --- DIFF -------------------------------------------------------------
echo "=== parity: full-refresh ==="
uv run python scripts/parity_self.py \
    --left  sturukin_db.atomic_derived_baseline_fr \
    --right sturukin_db.atomic_derived_patched_fr

echo "=== parity: incremental ==="
uv run python scripts/parity_self.py \
    --left  sturukin_db.atomic_derived_baseline_inc \
    --right sturukin_db.atomic_derived_patched_inc

echo "=== ALL CHECKS PASSED ==="
