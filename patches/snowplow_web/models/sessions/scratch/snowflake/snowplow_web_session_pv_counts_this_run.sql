{# Pre-materialized page_view counts per session to keep session_aggs hash joins small. #}

{{
    config(
        materialized='table',
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

select domain_sessionid,
       count(*) as page_views,
       count(case when has_ping = 1 then 1 end) as pv_with_ping
from {{ ref('snowplow_web_session_pv_set_this_run') }}
group by domain_sessionid
