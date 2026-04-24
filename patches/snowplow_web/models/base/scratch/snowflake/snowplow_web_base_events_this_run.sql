{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

Patched: replaces the upstream monolithic base_events_this_run with a
3-model narrow-scratch dedup chain. This model is now just the final
INNER JOIN of the wide raw table against the winners table, producing
one wide row per event_id.
#}

{{
  config(
    materialized='table',
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select r.*
from {{ ref('snowplow_web_base_events_raw_this_run') }} r
inner join {{ ref('snowplow_web_base_events_winners_this_run') }} w
  on r.event_id = w.event_id
  and md5(concat_ws('|', r.event_id, cast(r.collector_tstamp as varchar), cast(r.dvce_created_tstamp as varchar), cast(r.load_tstamp as varchar))) = w.winner_hash
