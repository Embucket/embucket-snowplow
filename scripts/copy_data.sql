COPY INTO demo.atomic.events
FROM 's3://embucket-testdata/dbt_snowplow_data/synthetic_web_analytics_1g_fixed.parquet'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
