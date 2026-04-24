{#
Spillable GROUP BY replacement for count(distinct page_view_id) which is
non-spillable in DataFusion's hash aggregate.
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
    max(case when event_name = 'page_ping' then 1 else 0 end) as has_ping
from {{ ref('snowplow_web_base_events_this_run') }}
where event_name in ('page_ping', 'page_view')
  and page_view_id is not null
  {% if var("snowplow__ua_bot_filter", true) %}
      {{ filter_bots() }}
  {% endif %}
group by domain_sessionid, page_view_id
