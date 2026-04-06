#!/usr/bin/env bash
# Patch dbt-snowplow packages to recognize 'embucket' as a Snowflake-compatible target type.
# Run this after `dbt deps`.
set -euo pipefail

find dbt_packages -name '*.sql' -o -name '*.yml' | \
  xargs grep -rl "target\.type == 'snowflake'" 2>/dev/null | \
  xargs sed -i '' "s/target\.type == 'snowflake'/target.type in ['snowflake', 'embucket']/g"

echo "Patched dbt_packages for embucket compatibility."
