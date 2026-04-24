{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

NOTE (Embucket / DataFusion):
QUALIFY dedup + wide projections moved to materialized models for memory isolation:
  - snowplow_web_first_event_ids_this_run (QUALIFY on 4 narrow cols)
  - snowplow_web_last_event_ids_this_run (QUALIFY on 4 narrow cols)
  - snowplow_web_session_firsts_this_run (wide projection + lookup joins)
  - snowplow_web_session_lasts_this_run (wide projection + lookup joins)
This model is now just the final 3-way LEFT JOIN.
#}

{{
    config(
        tags=["this_run"],
        sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}

{%- if var('snowplow__session_passthroughs', []) -%}
    {%- set passthrough_names = [] -%}
    {%- for identifier in var('snowplow__session_passthroughs', []) -%}
        {%- if identifier is mapping -%}
            {%- do passthrough_names.append(identifier['alias']) -%}
        {%- else -%}
            {%- do passthrough_names.append(identifier) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif %}

select
    -- app id
    a.app_id,

    a.platform,

    -- session fields
    a.domain_sessionid,
    a.original_domain_sessionid,
    a.domain_sessionidx,

    -- when the session starts with a ping we need to add the min visit length to get when the session actually started
    case when a.event_name = 'page_ping' then
        {{ snowplow_utils.timestamp_add(datepart="second", interval=-var("snowplow__min_visit_length", 5), tstamp="c.start_tstamp") }}
    else c.start_tstamp end as start_tstamp,
    c.end_tstamp,
    a.model_tstamp,

    -- user fields
    a.user_id,
    a.domain_userid,
    a.original_domain_userid,
    a.stitched_user_id,
    a.network_userid,

    -- engagement fields
    c.page_views,
    c.engaged_time_in_s,
    {%- if var('snowplow__list_event_counts', false) %}
    try_parse_json(c.event_counts_string) as event_counts,
    {%- endif %}
    c.total_events,
    {{ engaged_session() }} as is_engaged,
    -- when the session starts with a ping we need to add the min visit length to get when the session actually started
    c.absolute_time_in_s + case when a.event_name = 'page_ping' then {{ var("snowplow__min_visit_length", 5) }} else 0 end as absolute_time_in_s,

    -- first page fields
    a.first_page_title,
    a.first_page_url,
    a.first_page_urlscheme,
    a.first_page_urlhost,
    a.first_page_urlpath,
    a.first_page_urlquery,
    a.first_page_urlfragment,

    -- only take the first value when the last is genuinely missing (base on url as has to always be populated)
    case when b.last_page_url is null then coalesce(b.last_page_title, a.first_page_title) else b.last_page_title end as last_page_title,
    case when b.last_page_url is null then coalesce(b.last_page_url, a.first_page_url) else b.last_page_url end as last_page_url,
    case when b.last_page_url is null then coalesce(b.last_page_urlscheme, a.first_page_urlscheme) else b.last_page_urlscheme end as last_page_urlscheme,
    case when b.last_page_url is null then coalesce(b.last_page_urlhost, a.first_page_urlhost) else b.last_page_urlhost end as last_page_urlhost,
    case when b.last_page_url is null then coalesce(b.last_page_urlpath, a.first_page_urlpath) else b.last_page_urlpath end as last_page_urlpath,
    case when b.last_page_url is null then coalesce(b.last_page_urlquery, a.first_page_urlquery) else b.last_page_urlquery end as last_page_urlquery,
    case when b.last_page_url is null then coalesce(b.last_page_urlfragment, a.first_page_urlfragment) else b.last_page_urlfragment end as last_page_urlfragment,

    -- referrer fields
    a.referrer,
    a.refr_urlscheme,
    a.refr_urlhost,
    a.refr_urlpath,
    a.refr_urlquery,
    a.refr_urlfragment,
    a.refr_medium,
    a.refr_source,
    a.refr_term,

    -- marketing fields
    a.mkt_medium,
    a.mkt_source,
    a.mkt_term,
    a.mkt_content,
    a.mkt_campaign,
    a.mkt_clickid,
    a.mkt_network,
    a.mkt_source_platform,
    a.default_channel_group,

    -- geo fields
    a.geo_country,
    a.geo_region,
    a.geo_region_name,
    a.geo_city,
    a.geo_zipcode,
    a.geo_latitude,
    a.geo_longitude,
    a.geo_timezone,
    a.geo_country_name,
    a.geo_continent,
    case when b.last_geo_country is null then coalesce(b.last_geo_country, a.geo_country) else b.last_geo_country end as last_geo_country,
    case when b.last_geo_country is null then coalesce(b.last_geo_region_name, a.geo_region_name) else b.last_geo_region_name end as last_geo_region_name,
    case when b.last_geo_country is null then coalesce(b.last_geo_city, a.geo_city) else b.last_geo_city end as last_geo_city,
    case when b.last_geo_country is null then coalesce(b.last_geo_country_name, a.geo_country_name) else b.last_geo_country_name end as last_geo_country_name,
    case when b.last_geo_country is null then coalesce(b.last_geo_continent, a.geo_continent) else b.last_geo_continent end as last_geo_continent,

    -- ip address
    a.user_ipaddress,

    -- user agent
    a.useragent,

    a.br_renderengine,
    a.br_lang,
    a.br_lang_name,
    case when b.last_br_lang is null then coalesce(b.last_br_lang, a.br_lang) else b.last_br_lang end as last_br_lang,
    case when b.last_br_lang is null then coalesce(b.last_br_lang_name, a.br_lang_name) else b.last_br_lang_name end as last_br_lang_name,

    a.os_timezone,

    -- iab enrichment fields
    a.category,
    a.primary_impact,
    a.reason,
    a.spider_or_robot,

    -- ua parser enrichment fields
    a.useragent_family,
    a.useragent_major,
    a.useragent_minor,
    a.useragent_patch,
    a.useragent_version,
    a.os_family,
    a.os_major,
    a.os_minor,
    a.os_patch,
    a.os_patch_minor,
    a.os_version,
    a.device_family,

    -- yauaa enrichment fields
    a.device_class,
    case when a.device_class = 'Desktop' THEN 'Desktop'
        when a.device_class = 'Phone' then 'Mobile'
        when a.device_class = 'Tablet' then 'Tablet'
        else 'Other' end as device_category,
    a.screen_resolution,
    a.agent_class,
    a.agent_name,
    a.agent_name_version,
    a.agent_name_version_major,
    a.agent_version,
    a.agent_version_major,
    a.device_brand,
    a.device_name,
    a.device_version,
    a.layout_engine_class,
    a.layout_engine_name,
    a.layout_engine_name_version,
    a.layout_engine_name_version_major,
    a.layout_engine_version,
    a.layout_engine_version_major,
    a.operating_system_class,
    a.operating_system_name,
    a.operating_system_name_version,
    a.operating_system_version

    -- conversion fields
    {%- if var('snowplow__conversion_events', none) %}
        {%- for conv_def in var('snowplow__conversion_events') %}
    {{ snowplow_web.get_conversion_columns(conv_def, names_only = true)}}
        {%- endfor %}
    {% if var('snowplow__total_all_conversions', false) %}
    ,{%- for conv_def in var('snowplow__conversion_events') %}{{'cv_' ~ conv_def['name'] ~ '_volume'}}{%- if not loop.last %} + {% endif -%}{%- endfor %} as cv__all_volume
    ,0 {%- for conv_def in var('snowplow__conversion_events') %}{%- if conv_def.get('value') %} + {{'cv_' ~ conv_def['name'] ~ '_total'}}{% endif -%}{%- endfor %} as cv__all_total
    {% endif %}
    {%- endif %}

    -- passthrough fields
    {%- if var('snowplow__session_passthroughs', []) -%}
        {%- for col in passthrough_names %}
            , a.{{col}}
        {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_web_session_firsts_this_run') }} a
left join
    {{ ref('snowplow_web_session_lasts_this_run') }} b on a.domain_sessionid = b.domain_sessionid
left join
    {{ ref('snowplow_web_session_aggs_this_run') }} c on a.domain_sessionid = c.domain_sessionid
