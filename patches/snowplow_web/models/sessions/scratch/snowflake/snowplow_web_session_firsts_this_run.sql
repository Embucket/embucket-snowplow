{# Wide projection for first event per session, isolated for memory safety. #}

{{
    config(
        materialized='table',
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

select
    ev.app_id as app_id,
    ev.platform,
    ev.domain_sessionid,
    ev.original_domain_sessionid,
    ev.domain_sessionidx,
    {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp,
    ev.user_id,
    ev.domain_userid,
    ev.original_domain_userid,
    {% if var('snowplow__session_stitching') %}
        cast(ev.domain_userid as {{ type_string() }}) as stitched_user_id,
    {% else %}
        cast(null as {{ type_string() }}) as stitched_user_id,
    {% endif %}
    ev.network_userid as network_userid,
    ev.page_title as first_page_title,
    ev.page_url as first_page_url,
    ev.page_urlscheme as first_page_urlscheme,
    ev.page_urlhost as first_page_urlhost,
    ev.page_urlpath as first_page_urlpath,
    ev.page_urlquery as first_page_urlquery,
    ev.page_urlfragment as first_page_urlfragment,
    ev.page_referrer as referrer,
    ev.refr_urlscheme as refr_urlscheme,
    ev.refr_urlhost as refr_urlhost,
    ev.refr_urlpath as refr_urlpath,
    ev.refr_urlquery as refr_urlquery,
    ev.refr_urlfragment as refr_urlfragment,
    ev.refr_medium as refr_medium,
    ev.refr_source as refr_source,
    ev.refr_term as refr_term,
    ev.mkt_medium as mkt_medium,
    ev.mkt_source as mkt_source,
    ev.mkt_term as mkt_term,
    ev.mkt_content as mkt_content,
    ev.mkt_campaign as mkt_campaign,
    ev.mkt_clickid as mkt_clickid,
    ev.mkt_network as mkt_network,
    regexp_substr(ev.page_urlquery, 'utm_source_platform=([^?&#]*)', 1, 1, 'e') as mkt_source_platform,
    {{ channel_group_query() }} as default_channel_group,
    ev.geo_country as geo_country,
    ev.geo_region as geo_region,
    ev.geo_region_name as geo_region_name,
    ev.geo_city as geo_city,
    ev.geo_zipcode as geo_zipcode,
    ev.geo_latitude as geo_latitude,
    ev.geo_longitude as geo_longitude,
    ev.geo_timezone as geo_timezone,
    g.name as geo_country_name,
    g.region as geo_continent,
    ev.user_ipaddress as user_ipaddress,
    ev.useragent as useragent,
    ev.dvce_screenwidth || 'x' || ev.dvce_screenheight as screen_resolution,
    ev.br_renderengine as br_renderengine,
    ev.br_lang as br_lang,
    l.name as br_lang_name,
    ev.os_timezone as os_timezone,
    {{snowplow_web.get_iab_context_fields()}},
    {{snowplow_web.get_ua_context_fields()}},
    {{snowplow_web.get_yauaa_context_fields()}},
    ev.event_name
    {%- if var('snowplow__session_passthroughs', []) -%}
        {%- for identifier in var('snowplow__session_passthroughs', []) %}
        {%- if identifier is mapping -%}
            ,{{identifier['sql']}} as {{identifier['alias']}}
        {%- else -%}
            ,ev.{{identifier}}
        {%- endif -%}
        {% endfor -%}
    {%- endif %}
from {{ ref('snowplow_web_base_events_this_run') }} ev
inner join {{ ref('snowplow_web_first_event_ids_this_run') }} fe on ev.event_id = fe.event_id
left join
    {{ ref(var('snowplow__ga4_categories_seed')) }} c on lower(trim(ev.mkt_source)) = lower(c.source)
left join
    {{ ref(var('snowplow__rfc_5646_seed')) }} l on lower(ev.br_lang) = lower(l.lang_tag)
left join
    {{ ref(var('snowplow__geo_mapping_seed')) }} g on lower(ev.geo_country) = lower(g.alpha_2)
where
    ev.event_name in ('page_ping', 'page_view')
    and ev.page_view_id is not null
    {% if var("snowplow__ua_bot_filter", true) %}
        {{ filter_bots() }}
    {% endif %}
