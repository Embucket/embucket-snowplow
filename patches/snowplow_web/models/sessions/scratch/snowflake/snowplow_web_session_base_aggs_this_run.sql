{#
Heavy GROUP BY over base_events_this_run isolated in its own model so it doesn't
share a memory pool with the downstream hash joins in session_aggs_this_run.
#}

{{
    config(
        materialized='table',
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

select
    domain_sessionid
    , min(derived_tstamp) as start_tstamp
    , max(derived_tstamp) as end_tstamp
    {%- if var('snowplow__list_event_counts', false) %}
        {% set event_names =  dbt_utils.get_column_values(ref('snowplow_web_base_events_this_run'), 'event_name', order_by = 'event_name') %}
        , '{' || rtrim(
        {%- for event_name in event_names %}
            case when sum(case when event_name = '{{event_name}}' then 1 else 0 end) > 0 then '"{{event_name}}" :' || sum(case when event_name = '{{event_name}}' then 1 else 0 end) || ', ' else '' end ||
        {%- endfor -%}
        '', ', ') || '}' as event_counts_string
    {%- endif %}
    , count(*) as total_events
    , {{ snowplow_utils.timestamp_diff('min(derived_tstamp)', 'max(derived_tstamp)', 'second') }} as absolute_time_in_s
{%- if var('snowplow__conversion_events', none) %}
    {%- for conv_def in var('snowplow__conversion_events') %}
        {{ snowplow_web.get_conversion_columns(conv_def)}}
    {%- endfor %}
{%- endif %}
from {{ ref('snowplow_web_base_events_this_run') }}
where
    1 = 1
    {% if var("snowplow__ua_bot_filter", true) %}
        {{ filter_bots() }}
    {% endif %}
group by domain_sessionid
