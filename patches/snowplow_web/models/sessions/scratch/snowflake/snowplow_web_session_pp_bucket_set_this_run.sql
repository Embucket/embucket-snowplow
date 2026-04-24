{#
Spillable GROUP BY replacement for count(distinct page_view_id || ping_bucket)
which is non-spillable in DataFusion's hash aggregate.
Each row = one unique (session, page_view, heartbeat-bucket) combination.
#}

{{
    config(
        materialized='table',
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

select
    domain_sessionid,
    page_view_id,
    cast(floor({{ snowplow_utils.to_unixtstamp('dvce_created_tstamp') }} / {{ var('snowplow__heartbeat', 10) }}) as {{ dbt.type_string() }}) as ping_bucket
from {{ ref('snowplow_web_base_events_this_run') }}
where event_name = 'page_ping'
  and page_view_id is not null
  {% if var("snowplow__ua_bot_filter", true) %}
      {{ filter_bots() }}
  {% endif %}
group by domain_sessionid, page_view_id, ping_bucket
