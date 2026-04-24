{#
Narrow-projection QUALIFY isolated in its own model for memory safety at scale.
row_number() runs over 4 columns instead of 80+ in page_views_this_run.
#}

{{
    config(
        materialized='table',
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

select event_id
from (
    select event_id, page_view_id, derived_tstamp, dvce_created_tstamp
    from {{ ref('snowplow_web_base_events_this_run') }}
    where event_name = 'page_view'
    and page_view_id is not null
)
qualify row_number() over (partition by page_view_id order by derived_tstamp, dvce_created_tstamp) = 1
