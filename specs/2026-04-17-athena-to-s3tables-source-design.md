# Load real Snowplow source into S3 Tables via Athena CTAS

## Goal

Run the existing dbt-snowplow-web pipeline on real Snowplow atomic events (from
the Glue-managed Iceberg table `analytics_glue.hooli_events_0417_v2`) and
verify all derived tables are non-empty.

## Constraints

- `dbt-embucket` only reads tables that live in the S3 Tables bucket Embucket is
  volumed to (`arn:aws:s3tables:us-east-2:767397688925:bucket/snowplow`,
  mapped as database `demo`).
- Embucket's `COPY INTO` is path-based and cannot traverse Iceberg metadata, so
  it cannot be pointed at the source warehouse prefix directly.
- The source table is Iceberg v2 in Glue, partitioned by
  `day(load_tstamp), event_name`, ~2.12M rows / ~2.44 GB across 634 files.

## Approach

Have Athena do both sides: read the Glue Iceberg source, write a new Iceberg
table directly into the Embucket S3 Tables bucket via the existing Lake
Formation federated catalog. No staging parquet, no Embucket `COPY INTO`. dbt
then reads the new table through Embucket.

### Environment prerequisites (already in place)

- Glue federated catalog `767397688925:s3tablescatalog` registered against
  `arn:aws:s3tables:us-east-2:767397688925:bucket/*`. Addresses the `snowplow`
  bucket's namespaces from Athena as `"s3tablescatalog/snowplow"."atomic"`.
- Lake Formation resource `arn:aws:s3tables:us-east-2:767397688925:bucket/*`
  registered with `WithFederation=true`.
- Source table visible to Athena as
  `awsdatacatalog.analytics_glue.hooli_events_0417_v2`.

### Target schema policy

Keep the target identical to today's `demo.atomic.events`:
- **117 atomic columns** plus **7 snowplow-context/unstruct columns** used by
  dbt-snowplow-web (`web_page_1`, `ua_parser_context_1`, `yauaa_context_1`,
  `consent_preferences_1`, `cmp_visible_1`, `iab_spiders_and_robots_1`,
  `web_vitals_1`).
- 3 of those 7 context columns **exist** in the source and are serialised to
  JSON strings; 4 **are absent** in the source and are written as `NULL` to
  preserve the package's schema contract.
- All 100+ other typed context/unstruct columns in the source are dropped.

### Type reconciliation (done in the CTAS SELECT)

| Source (Iceberg) | Target (Embucket `create_table.sql`) | Cast |
|---|---|---|
| `timestamptz` (all tstamps) | `TIMESTAMP_NTZ` | `CAST(... AS TIMESTAMP)` — strips tz |
| `decimal(18,2)` (tr_*, ti_*) | `DOUBLE` | `CAST(... AS DOUBLE)` |
| `double` `se_value` | `STRING` | `CAST(... AS VARCHAR)` |
| `string` `br_colordepth` | `INTEGER` | `TRY_CAST(... AS INTEGER)` |
| struct/list contexts | `VARIANT` | `CAST(... AS JSON)` (serialises to JSON string) |

`CAST(struct AS JSON)` in Athena produces a VARCHAR JSON payload that Embucket
stores as VARIANT, matching the existing synthetic fixture's shape.

### Target table name

`demo.atomic.events_0416` (new name, does not touch the existing
`demo.atomic.events` produced by the synthetic loader). Source configuration
in dbt is redirected to it.

### Partitioning

Preserve the source's partition spec on the target:
`WITH (partitioning = ARRAY['day(load_tstamp)', 'event_name'])`. This matches
the Snowplow incremental tuning already in `dbt_project.yml`
(`snowplow__session_timestamp: load_tstamp`) and the source's own layout.

### dbt wiring

Update `models/sources.yml` with an `identifier: events_0416` override on the
existing `atomic.events` source. This leaves `snowplow__events:
"{{ source('atomic','events') }}"` in `dbt_project.yml` untouched — all
references resolve to the new physical table with no model edits.

### Verification steps

1. Count rows visible to Embucket: run `SELECT COUNT(*) FROM demo.atomic.events_0416`
   via `scripts/embucket_client.py run_sql` and expect ~2.12M.
2. Sanity-read one row: confirm `load_tstamp`, `event_id`, and a VARIANT column
   deserialise.
3. `uv run dbt seed --profiles-dir .`
4. `uv run dbt run --profiles-dir .` — expect 18 models to build.
5. `dbt show` the three headline derived tables; each should be non-empty:
   - `demo.atomic_derived.snowplow_web_page_views`
   - `demo.atomic_derived.snowplow_web_sessions`
   - `demo.atomic_derived.snowplow_web_users`

## Components

### `scripts/ctas_from_glue.sql`

New file. The Athena CTAS statement with the full column projection and casts
described above. Kept in-repo so the load is reproducible.

### `scripts/load_from_glue.py`

New file. Orchestrates:
1. Drop `s3tablescatalog/snowplow.atomic.events_0416` if it exists (idempotent
   reruns).
2. Start Athena query from `ctas_from_glue.sql` using `boto3` Athena client.
3. Poll until completion; print row count on success, throw on failure.

Takes `--athena-workgroup`, `--query-output-location`
(`s3://.../athena-results/`) as args, with sensible defaults resolved from
environment. Uses the current AWS credentials (no Embucket Lambda involved).

### `models/sources.yml` edit

Add `identifier: events_0416` under the `events` table entry so dbt's
`source('atomic','events')` resolves to the Athena-written table.

### `README.md` addendum

New "Loading real Snowplow data" section pointing at `load_from_glue.py` as the
alternative to step 6. Keeps the synthetic quickstart path as the default.

## Risks / open issues

1. **Embucket reading Athena-written Iceberg**: unverified end-to-end today. If
   read fails on a type (most likely candidate: nested JSON VARIANT round-trip),
   mitigation is to adjust the cast in `ctas_from_glue.sql` — the SQL is the
   single point of control.
2. **Partition transform support on read**: Embucket should ignore partition
   spec on reads (it's metadata), but if pruning misbehaves we can re-run CTAS
   unpartitioned as a fallback.
3. **Glue federated catalog write permissions**: CTAS from Athena requires the
   query's Lake Formation principal to hold `CREATE_TABLE` on
   `s3tablescatalog/snowplow.atomic`. First run may surface a perms gap; noted
   so we don't treat it as a code bug.
4. **Absent context columns**: 4 of the 7 snowplow contexts referenced by the
   current dbt_project.yml are not present in the source. Models gated on
   those flags (iab, cwv, consent) will produce empty/null-only derived rows.
   That's acceptable for a "does the pipeline run" smoke test; flag values in
   `dbt_project.yml` can be disabled later if the test is re-scoped to
   "populates every derived table".

## Out of scope

- Changing `create_table.sql` / the synthetic load path.
- Adding `dbt-snowplow-web` config flags for context coverage.
- Benchmarking runtime; this spec only targets coverage verification.
- Cross-account plumbing (source and target live in the same account).
