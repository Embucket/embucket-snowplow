{# Pre-materialized ping bucket counts per session to keep session_aggs hash joins small. #}

{{
    config(
        materialized='table',
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

select domain_sessionid,
       count(*) as total_ping_buckets
from {{ ref('snowplow_web_session_pp_bucket_set_this_run') }}
group by domain_sessionid
