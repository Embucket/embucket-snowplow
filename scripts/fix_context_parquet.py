#!/usr/bin/env python3
"""Convert Snowplow context/unstruct columns from Python-repr strings to valid JSON.

The synthetic parquet files at s3://embucket-testdata/dbt_snowplow_data/ store
context columns like '[{\'id\':\'47e3cac1-...\'}]' (Python dict repr, single
quotes). Snowflake/Embucket VARIANT path access (`col[0]:id::varchar`) requires
valid JSON, so these rows come out NULL.

Reads the source parquet in row-group batches, rewrites the offending columns
as json.dumps(ast.literal_eval(value)), and streams a new parquet to disk.
"""
import ast
import json
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

CONTEXT_COLS = {
    "CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_WEB_PAGE_1",
    "UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_CONSENT_PREFERENCES_1",
    "UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_CMP_VISIBLE_1",
    "CONTEXTS_COM_IAB_SNOWPLOW_SPIDERS_AND_ROBOTS_1",
    "CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1",
    "CONTEXTS_NL_BASJES_YAUAA_CONTEXT_1",
    "UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_WEB_VITALS_1",
}


def pyrepr_to_json(s):
    if s is None:
        return None
    try:
        return json.dumps(ast.literal_eval(s))
    except (ValueError, SyntaxError):
        return None


def main():
    if len(sys.argv) != 3:
        print("usage: fix_context_parquet.py <s3_input_uri> <local_output_path>")
        sys.exit(1)
    in_uri, out_path = sys.argv[1], sys.argv[2]
    assert in_uri.startswith("s3://")
    in_s3 = in_uri[len("s3://") :]

    fs = s3fs.S3FileSystem()
    reader = pq.ParquetFile(in_s3, filesystem=fs)
    schema = reader.schema_arrow
    print(f"input: {in_uri} ({reader.metadata.num_rows:,} rows, {reader.num_row_groups} row groups)")

    cols_to_fix = [c for c in CONTEXT_COLS if c in schema.names]
    print(f"columns to fix: {cols_to_fix}")

    writer = pq.ParquetWriter(out_path, schema, compression="snappy")
    total = 0
    for i, batch in enumerate(reader.iter_batches(batch_size=200_000)):
        table = pa.Table.from_batches([batch])
        for col in cols_to_fix:
            idx = table.column_names.index(col)
            fixed = [pyrepr_to_json(v) for v in table.column(col).to_pylist()]
            table = table.set_column(idx, col, pa.array(fixed, type=pa.string()))
        writer.write_table(table)
        total += table.num_rows
        print(f"  batch {i + 1}: +{table.num_rows:,} rows (total {total:,})")
    writer.close()
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
