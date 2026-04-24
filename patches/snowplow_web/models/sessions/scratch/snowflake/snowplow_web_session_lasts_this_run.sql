{# Wide projection for last event per session, isolated for memory safety. #}

{{
    config(
        materialized='table',
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

select
    ev.domain_sessionid,
    ev.page_title as last_page_title,
    ev.page_url as last_page_url,
    ev.page_urlscheme as last_page_urlscheme,
    ev.page_urlhost as last_page_urlhost,
    ev.page_urlpath as last_page_urlpath,
    ev.page_urlquery as last_page_urlquery,
    ev.page_urlfragment as last_page_urlfragment,
    ev.geo_country as last_geo_country,
    ev.geo_city as last_geo_city,
    ev.geo_region_name as last_geo_region_name,
    g.name as last_geo_country_name,
    g.region as last_geo_continent,
    ev.br_lang as last_br_lang,
    l.name as last_br_lang_name
from {{ ref('snowplow_web_base_events_this_run') }} ev
inner join {{ ref('snowplow_web_last_event_ids_this_run') }} le on ev.event_id = le.event_id
left join
    {{ ref(var('snowplow__rfc_5646_seed')) }} l on lower(ev.br_lang) = lower(l.lang_tag)
left join
    {{ ref(var('snowplow__geo_mapping_seed')) }} g on lower(ev.geo_country) = lower(g.alpha_2)
where
    ev.event_name = 'page_view'
    and ev.page_view_id is not null
    {% if var("snowplow__ua_bot_filter", true) %}
        {{ filter_bots() }}
    {% endif %}
