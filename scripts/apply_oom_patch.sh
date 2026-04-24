#!/usr/bin/env bash
# Apply the OOM-mitigation decomposed models on top of a fresh dbt_packages/.
# Survives `dbt deps` (patches/ is in the repo; this script is re-runnable).
set -euo pipefail

if [[ ! -d dbt_packages ]]; then
  echo "ERROR: dbt_packages/ missing. Run 'uv run dbt deps --profiles-dir .' first." >&2
  exit 1
fi

# 1) Existing embucket-target-type sed patch (safe to layer on snowflake target too).
# It uses `grep | while read` under pipefail; grep exits 1 when already-patched,
# which is not an error for us. Tolerate exit 1 only.
set +e
./scripts/patch_snowplow.sh
rc=$?
set -e
if (( rc != 0 && rc != 1 )); then
  echo "patch_snowplow.sh failed with rc=$rc" >&2
  exit $rc
fi

# 2) Overlay decomposed models. --checksum compares content, not mtime -> idempotent.
rsync -a --checksum patches/ dbt_packages/

echo "Applied OOM patch to dbt_packages/"
