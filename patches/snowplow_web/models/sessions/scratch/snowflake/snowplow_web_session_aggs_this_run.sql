{#
Final session aggregates: 3-way join of pre-materialized small tables.
All GROUP BYs run in isolated models to keep hash join memory low.
#}

{{
    config(
        materialized='table',
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

select
    a.domain_sessionid
    , a.start_tstamp
    , a.end_tstamp
    {%- if var('snowplow__list_event_counts', false) %}
    , a.event_counts_string
    {%- endif %}
    , a.total_events
    , coalesce(pv.page_views, 0) as page_views
    , ({{ var("snowplow__heartbeat", 10) }} * (coalesce(pp.total_ping_buckets, 0) - coalesce(pv.pv_with_ping, 0)))
      + (coalesce(pv.pv_with_ping, 0) * {{ var("snowplow__min_visit_length", 5) }}) as engaged_time_in_s
    , a.absolute_time_in_s
{%- if var('snowplow__conversion_events', none) %}
    {%- for conv_def in var('snowplow__conversion_events') %}
    {{ snowplow_web.get_conversion_columns(conv_def, names_only = true)}}
    {%- endfor %}
{%- endif %}
from {{ ref('snowplow_web_session_base_aggs_this_run') }} a
left join {{ ref('snowplow_web_session_pv_counts_this_run') }} pv on a.domain_sessionid = pv.domain_sessionid
left join {{ ref('snowplow_web_session_pp_counts_this_run') }} pp on a.domain_sessionid = pp.domain_sessionid
