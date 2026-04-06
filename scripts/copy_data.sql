COPY INTO demo.atomic.events
FROM 's3://embucket-testdata/dbt_snowplow_data/events_today.csv'
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1);
