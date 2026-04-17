-- Athena CTAS: copy Glue Iceberg source into the Embucket S3 Tables bucket.
--
-- Source: awsdatacatalog.analytics_glue.hooli_events_0417_v2 (Iceberg v2 in Glue).
-- Target: "s3tablescatalog/snowplow"."atomic"."events_0416" (new Iceberg table
--         in the bucket Embucket Lambda has volumed as database `demo`).
--
-- Column list and ordering mirror scripts/create_table.sql so the existing
-- dbt-snowplow-web wiring works unchanged. Type casts reconcile differences
-- between the source's Iceberg types and the target schema:
--   timestamptz              -> timestamp      (strip tz)
--   decimal(18,2)            -> double         (tr_*, ti_* monetary cols)
--   int    txn_id            -> varchar
--   double se_value          -> varchar
--   string br_colordepth     -> integer        (via TRY_CAST)
--   list/struct context cols -> varchar (JSON) (via JSON_FORMAT)
--
-- Only the 3 context columns that exist in the source are populated; the 4
-- absent ones (consent_preferences, cmp_visible, iab_spiders_and_robots,
-- web_vitals) are emitted as NULL so the column set matches the synthetic
-- schema.

CREATE TABLE events_0416
WITH (
    table_type = 'ICEBERG',
    is_external = false,
    format = 'PARQUET',
    write_compression = 'ZSTD',
    partitioning = ARRAY['day(load_tstamp)', 'event_name']
) AS
SELECT
    app_id,
    platform,
    CAST(etl_tstamp AS TIMESTAMP)          AS etl_tstamp,
    CAST(collector_tstamp AS TIMESTAMP)    AS collector_tstamp,
    CAST(dvce_created_tstamp AS TIMESTAMP) AS dvce_created_tstamp,
    event,
    event_id,
    CAST(txn_id AS VARCHAR)                AS txn_id,
    name_tracker,
    v_tracker,
    v_collector,
    v_etl,
    user_id,
    user_ipaddress,
    user_fingerprint,
    domain_userid,
    domain_sessionidx,
    network_userid,
    geo_country,
    geo_region,
    geo_city,
    geo_zipcode,
    geo_latitude,
    geo_longitude,
    geo_region_name,
    ip_isp,
    ip_organization,
    ip_domain,
    ip_netspeed,
    page_url,
    page_title,
    page_referrer,
    page_urlscheme,
    page_urlhost,
    page_urlport,
    page_urlpath,
    page_urlquery,
    page_urlfragment,
    refr_urlscheme,
    refr_urlhost,
    refr_urlport,
    refr_urlpath,
    refr_urlquery,
    refr_urlfragment,
    refr_medium,
    refr_source,
    refr_term,
    mkt_medium,
    mkt_source,
    mkt_term,
    mkt_content,
    mkt_campaign,
    se_category,
    se_action,
    se_label,
    se_property,
    CAST(se_value AS VARCHAR)              AS se_value,
    tr_orderid,
    tr_affiliation,
    CAST(tr_total AS DOUBLE)               AS tr_total,
    CAST(tr_tax AS DOUBLE)                 AS tr_tax,
    CAST(tr_shipping AS DOUBLE)            AS tr_shipping,
    tr_city,
    tr_state,
    tr_country,
    ti_orderid,
    ti_sku,
    ti_name,
    ti_category,
    CAST(ti_price AS DOUBLE)               AS ti_price,
    ti_quantity,
    pp_xoffset_min,
    pp_xoffset_max,
    pp_yoffset_min,
    pp_yoffset_max,
    useragent,
    br_name,
    br_family,
    br_version,
    br_type,
    br_renderengine,
    br_lang,
    br_features_pdf,
    br_features_flash,
    br_features_java,
    br_features_director,
    br_features_quicktime,
    br_features_realplayer,
    br_features_windowsmedia,
    br_features_gears,
    br_features_silverlight,
    br_cookies,
    TRY_CAST(br_colordepth AS INTEGER)     AS br_colordepth,
    br_viewwidth,
    br_viewheight,
    os_name,
    os_family,
    os_manufacturer,
    os_timezone,
    dvce_type,
    dvce_ismobile,
    dvce_screenwidth,
    dvce_screenheight,
    doc_charset,
    doc_width,
    doc_height,
    tr_currency,
    CAST(tr_total_base AS DOUBLE)          AS tr_total_base,
    CAST(tr_tax_base AS DOUBLE)            AS tr_tax_base,
    CAST(tr_shipping_base AS DOUBLE)       AS tr_shipping_base,
    ti_currency,
    CAST(ti_price_base AS DOUBLE)          AS ti_price_base,
    base_currency,
    geo_timezone,
    mkt_clickid,
    mkt_network,
    etl_tags,
    CAST(dvce_sent_tstamp AS TIMESTAMP)    AS dvce_sent_tstamp,
    refr_domain_userid,
    CAST(refr_dvce_tstamp AS TIMESTAMP)    AS refr_dvce_tstamp,
    domain_sessionid,
    CAST(derived_tstamp AS TIMESTAMP)      AS derived_tstamp,
    event_vendor,
    event_name,
    event_format,
    event_version,
    event_fingerprint,
    CAST(true_tstamp AS TIMESTAMP)         AS true_tstamp,
    CAST(load_tstamp AS TIMESTAMP)         AS load_tstamp,
    JSON_FORMAT(CAST(contexts_com_snowplowanalytics_snowplow_web_page_1 AS JSON))
        AS contexts_com_snowplowanalytics_snowplow_web_page_1,
    CAST(NULL AS VARCHAR)
        AS unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1,
    CAST(NULL AS VARCHAR)
        AS unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1,
    CAST(NULL AS VARCHAR)
        AS contexts_com_iab_snowplow_spiders_and_robots_1,
    JSON_FORMAT(CAST(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1 AS JSON))
        AS contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
    JSON_FORMAT(CAST(contexts_nl_basjes_yauaa_context_1 AS JSON))
        AS contexts_nl_basjes_yauaa_context_1,
    CAST(NULL AS VARCHAR)
        AS unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1
-- Filter: only post-cutover data (generator realism fix landed 19:47 UTC)
FROM "awsdatacatalog"."analytics_glue"."hooli_events_0417_v2"
WHERE load_tstamp >= TIMESTAMP '2026-04-17 19:47:00 UTC'
