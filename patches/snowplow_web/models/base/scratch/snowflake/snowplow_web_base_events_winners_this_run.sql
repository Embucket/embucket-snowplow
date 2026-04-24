{#
Narrow-projection event_id dedup. Two columns buffered per partition
(event_id + winner_hash); the QUALIFY is spillable.

winner_hash is computed as HASH(event_id, collector_tstamp,
dvce_created_tstamp, load_tstamp) so the raw table's inner join back on
(event_id, winner_hash) picks exactly one source row per event_id, even
when two events share the same (collector_tstamp, dvce_created_tstamp).
#}

{{
  config(
    materialized='table',
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select event_id, winner_hash
from (
  select
    event_id,
    md5(concat_ws('|', event_id, cast(collector_tstamp as varchar), cast(dvce_created_tstamp as varchar), cast(load_tstamp as varchar))) as winner_hash,
    collector_tstamp,
    dvce_created_tstamp
  from {{ ref('snowplow_web_base_events_raw_this_run') }}
)
qualify row_number() over (
  partition by event_id
  order by collector_tstamp, dvce_created_tstamp, winner_hash
) = 1
