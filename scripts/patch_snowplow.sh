#!/usr/bin/env bash
# Patch dbt-snowplow packages to recognize 'embucket' as a Snowflake-compatible target type.
# Run this after `dbt deps`.
set -euo pipefail

grep -rl "target\.type == 'snowflake'" dbt_packages/ 2>/dev/null | while read -r file; do
  sed -i '' "s/target\.type == 'snowflake'/target.type in ['snowflake', 'embucket']/g" "$file"
done

echo "Patched dbt_packages for embucket compatibility."
