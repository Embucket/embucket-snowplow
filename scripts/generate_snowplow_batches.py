#!/usr/bin/env python3
"""Clone the public 1 GB Snowplow parquet into monotonic benchmark batches."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import s3fs

DEFAULT_TEMPLATE_URI = "s3://embucket-testdata/dbt_snowplow_data/synthetic_web_analytics_1g_fixed.parquet"
DEFAULT_MANIFEST_PATH = "tmp/snowplow_batches_manifest.json"
TIMESTAMP_COLUMNS = {
    "ETL_TSTAMP",
    "COLLECTOR_TSTAMP",
    "DVCE_CREATED_TSTAMP",
    "DVCE_SENT_TSTAMP",
    "REFR_DVCE_TSTAMP",
    "DERIVED_TSTAMP",
    "TRUE_TSTAMP",
    "LOAD_TSTAMP",
}
STRING_ID_COLUMNS = {
    "EVENT_ID",
    "TXN_ID",
    "USER_ID",
    "USER_FINGERPRINT",
    "DOMAIN_USERID",
    "NETWORK_USERID",
    "REFR_DOMAIN_USERID",
    "DOMAIN_SESSIONID",
    "TR_ORDERID",
    "TI_ORDERID",
}
JSON_ID_COLUMNS = {
    "CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_WEB_PAGE_1",
    "UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_CONSENT_PREFERENCES_1",
    "UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_CMP_VISIBLE_1",
    "CONTEXTS_COM_IAB_SNOWPLOW_SPIDERS_AND_ROBOTS_1",
    "CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1",
    "CONTEXTS_NL_BASJES_YAUAA_CONTEXT_1",
    "UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_WEB_VITALS_1",
}
JSON_ID_PATTERN = r'("id"\s*:\s*")([^"]+)(")'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template-uri", default=DEFAULT_TEMPLATE_URI, help="Template parquet URI")
    parser.add_argument("--output-prefix", required=True, help="Destination dir or s3:// prefix")
    parser.add_argument("--batches", type=int, default=6, help="Number of parquet batches to generate")
    parser.add_argument(
        "--window-hours",
        type=float,
        help="Compress each batch's event timestamps into this many hours while preserving order",
    )
    parser.add_argument(
        "--gap-hours",
        type=float,
        default=1.0,
        help="Extra gap between batches when batch spacing is auto-computed",
    )
    parser.add_argument(
        "--batch-spacing-hours",
        type=float,
        help="Override the distance between consecutive batches in event time",
    )
    parser.add_argument(
        "--manifest-path",
        default=DEFAULT_MANIFEST_PATH,
        help="Local path for the generated batch manifest JSON",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200_000,
        help="Arrow record batch size while rewriting parquet rows",
    )
    return parser.parse_args()


def s3_key(uri: str) -> str:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got {uri}")
    return uri[len("s3://") :]


def batch_uri(prefix: str, index: int, batch_start: datetime) -> str:
    suffix = f"batch_{index:03d}_{batch_start.strftime('%Y%m%dT%H%M%S')}.parquet"
    if prefix.startswith("s3://"):
        return f"{prefix.rstrip('/')}/{suffix}"
    return str(Path(prefix).expanduser() / suffix)


def parquet_bounds(parquet_file: pq.ParquetFile) -> tuple[datetime, datetime]:
    schema = parquet_file.schema_arrow
    idx = schema.names.index("COLLECTOR_TSTAMP")
    mins = []
    maxes = []
    for row_group_idx in range(parquet_file.metadata.num_row_groups):
        stats = parquet_file.metadata.row_group(row_group_idx).column(idx).statistics
        mins.append(datetime.fromisoformat(stats.min))
        maxes.append(datetime.fromisoformat(stats.max))
    return min(mins), max(maxes)


def with_suffix(array, suffix: str):
    return pc.binary_join_element_wise(array, pa.scalar(suffix), "")


def rewrite_json_ids(array, suffix: str):
    return pc.replace_substring_regex(
        array,
        pattern=JSON_ID_PATTERN,
        replacement=rf"\1\2{suffix}\3",
    )


def shift_timestamp_strings(array, batch_start: datetime, template_min: datetime, scale: float):
    parsed = pc.cast(array, pa.timestamp("us"))
    offset = pc.cast(
        pc.subtract(parsed, pa.scalar(template_min, type=pa.timestamp("us"))),
        pa.int64(),
    )
    if scale != 1.0:
        offset = pc.cast(pc.floor(pc.multiply(offset, pa.scalar(scale))), pa.int64())
    shifted = pc.add(pc.cast(offset, pa.duration("us")), pa.scalar(batch_start, type=pa.timestamp("us")))
    return pc.cast(shifted, pa.string())


def rewrite_batch(table, batch_start: datetime, template_min: datetime, scale: float, suffix: str):
    for idx, column_name in enumerate(table.column_names):
        field = table.schema.field(idx)
        column = table.column(idx)
        if column_name in TIMESTAMP_COLUMNS:
            new_column = shift_timestamp_strings(column, batch_start, template_min, scale)
            new_field = pa.field(field.name, new_column.type, nullable=field.nullable, metadata=field.metadata)
            table = table.set_column(idx, new_field, new_column)
        elif column_name in STRING_ID_COLUMNS:
            new_column = with_suffix(column, suffix)
            new_field = pa.field(field.name, new_column.type, nullable=field.nullable, metadata=field.metadata)
            table = table.set_column(idx, new_field, new_column)
        elif column_name in JSON_ID_COLUMNS:
            new_column = rewrite_json_ids(column, suffix)
            new_field = pa.field(field.name, new_column.type, nullable=field.nullable, metadata=field.metadata)
            table = table.set_column(idx, new_field, new_column)
    return table


def open_output_handle(uri: str, fs: s3fs.S3FileSystem):
    if uri.startswith("s3://"):
        return fs.open(s3_key(uri), "wb")

    path = Path(uri).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("wb")


def main() -> int:
    args = parse_args()
    fs = s3fs.S3FileSystem()
    source_path = s3_key(args.template_uri) if args.template_uri.startswith("s3://") else str(Path(args.template_uri).expanduser())
    source_filesystem = fs if args.template_uri.startswith("s3://") else None
    source = pq.ParquetFile(source_path, filesystem=source_filesystem)
    template_min, template_max = parquet_bounds(source)
    template_span = template_max - template_min
    target_span = timedelta(hours=args.window_hours) if args.window_hours is not None else template_span
    scale = target_span / template_span if template_span.total_seconds() else 1.0
    spacing = (
        timedelta(hours=args.batch_spacing_hours)
        if args.batch_spacing_hours is not None
        else target_span if args.window_hours is not None else template_span + timedelta(hours=args.gap_hours)
    )

    print(
        f"Template: {args.template_uri} "
        f"({source.metadata.num_rows:,} rows, collector_tstamp {template_min} -> {template_max})"
    )
    if args.window_hours is not None:
        print(f"Compressed event window: {target_span} (scale factor {scale:.8f})")
    print(f"Batch spacing: {spacing}")

    manifest = {
        "template_uri": args.template_uri,
        "template_rows": source.metadata.num_rows,
        "template_collector_tstamp_min": template_min.isoformat(sep=" "),
        "template_collector_tstamp_max": template_max.isoformat(sep=" "),
        "window_hours": args.window_hours,
        "batch_spacing_seconds": spacing.total_seconds(),
        "batches": [],
    }

    for batch_index in range(args.batches):
        batch_start = template_min + (spacing * batch_index)
        suffix = f"__b{batch_index:03d}"
        destination = batch_uri(args.output_prefix, batch_index, batch_start)

        print(f"[{batch_index + 1}/{args.batches}] writing {destination}")
        with open_output_handle(destination, fs) as sink:
            writer = None
            rows_written = 0
            for record_batch in source.iter_batches(batch_size=args.batch_size):
                table = rewrite_batch(pa.Table.from_batches([record_batch]), batch_start, template_min, scale, suffix)
                if writer is None:
                    writer = pq.ParquetWriter(sink, table.schema, compression="snappy")
                writer.write_table(table)
                rows_written += table.num_rows
            if writer is not None:
                writer.close()

        collector_min = batch_start
        collector_max = batch_start + timedelta(microseconds=int(template_span.total_seconds() * 1_000_000 * scale))
        manifest["batches"].append(
            {
                "batch_index": batch_index,
                "uri": destination,
                "rows": rows_written,
                "collector_tstamp_min": collector_min.isoformat(sep=" "),
                "collector_tstamp_max": collector_max.isoformat(sep=" "),
                "suffix": suffix,
            }
        )

    manifest_path = Path(args.manifest_path).expanduser()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Wrote manifest to {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
