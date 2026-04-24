{# Narrow-projection QUALIFY: last event per session on 4 columns. #}

{{
    config(
        materialized='table',
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

select event_id
from (
    select event_id, domain_sessionid, derived_tstamp, dvce_created_tstamp
    from {{ ref('snowplow_web_base_events_this_run') }}
    where event_name = 'page_view'
    and page_view_id is not null
)
qualify row_number() over (partition by domain_sessionid order by derived_tstamp desc, dvce_created_tstamp desc, event_id) = 1
