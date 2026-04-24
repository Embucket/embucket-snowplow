{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    materialized='incremental',
    unique_key='domain_userid',
    sort='end_tstamp',
    dist='domain_userid',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "end_tstamp",
      "data_type": "timestamp"
    }),
    tags=["derived"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}


{# Narrow projection + spillable QUALIFY instead of non-spillable UNBOUNDED window.
   row_number() uses a sort (spillable), while last_value() with UNBOUNDED buffers
   entire partitions in memory (non-spillable) — OOMs on large datasets.
   Semantically equivalent: the user_id and end_tstamp of the last event per user. #}
select domain_userid, user_id, end_tstamp
from (
    select domain_userid, user_id, collector_tstamp as end_tstamp
    from (
        select domain_userid, {{ var('snowplow__user_stitching_id', 'user_id') }} as user_id, collector_tstamp
        from {{ ref('snowplow_web_base_events_this_run') }}
        where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }}
        and {{ var('snowplow__user_stitching_id', 'user_id') }} is not null
        and domain_userid is not null
    )
    qualify row_number() over (partition by domain_userid order by collector_tstamp desc) = 1
)
