#!/usr/bin/env python3
"""Probe Embucket partition pruning on MERGE INTO events_hooli.

Reads Iceberg metadata straight from S3 Tables to find the smallest partition
in demo.atomic.events_hooli (partitioned by day(collector_tstamp),
identity(event_name)). Samples existing rows from that partition via
Embucket, then drives a MERGE INTO whose ON clause includes the partition
columns so the optimizer can push them into the target scan. Captures
EXPLAIN and (optionally) EXPLAIN ANALYZE. Finally re-reads Iceberg metadata
and verifies that every data file added or removed by the new snapshot
belongs to the target partition.

Default run is safe: only EXPLAIN is issued, no rows are mutated. Pass
--execute to run the actual MERGE (as EXPLAIN ANALYZE when supported).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
import fastavro

from embucket_client import lambda_client, login, run_sql

TABLE_BUCKET_ARN = "arn:aws:s3tables:us-east-2:767397688925:bucket/snowplow"
NAMESPACE = "atomic"
TABLE_NAME = "events_hooli"
REGION = "us-east-2"
EMBUCKET_FQN = f"demo.{NAMESPACE}.{TABLE_NAME}"


@dataclass
class FileEntry:
    partition: tuple
    path: str
    record_count: int
    status: int  # 0=existing, 1=added, 2=deleted
    content: int  # 0=data, 1=position-delete, 2=equality-delete


@dataclass
class SnapshotInfo:
    snapshot_id: int
    metadata_location: str
    manifest_list_path: str
    partition_spec: list  # [(name, transform, source_field_name)]
    summary: dict
    schema_field_by_id: dict


def aws_cp(uri: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["aws", "s3", "cp", uri, str(dest), "--region", REGION, "--quiet"],
        check=True,
    )


def s3tables_get_table() -> dict:
    client = boto3.client("s3tables", region_name=REGION)
    return client.get_table(
        tableBucketARN=TABLE_BUCKET_ARN, namespace=NAMESPACE, name=TABLE_NAME
    )


def load_snapshot(metadata_location: str, workdir: Path) -> SnapshotInfo:
    meta_path = workdir / "metadata.json"
    aws_cp(metadata_location, meta_path)
    meta = json.loads(meta_path.read_text())
    current_id = meta["current-snapshot-id"]
    current = next(s for s in meta["snapshots"] if s["snapshot-id"] == current_id)
    spec = next(s for s in meta["partition-specs"] if s["spec-id"] == meta["default-spec-id"])
    schema = meta["schemas"][-1]
    fields_by_id = {f["id"]: f["name"] for f in schema["fields"]}
    return SnapshotInfo(
        snapshot_id=current_id,
        metadata_location=metadata_location,
        manifest_list_path=current["manifest-list"],
        partition_spec=[
            (f["name"], f["transform"], fields_by_id[f["source-id"]])
            for f in spec["fields"]
        ],
        summary=current.get("summary", {}),
        schema_field_by_id=fields_by_id,
    )


def read_manifests(snap: SnapshotInfo, workdir: Path) -> list[FileEntry]:
    snaplist_path = workdir / "snaplist.avro"
    aws_cp(snap.manifest_list_path, snaplist_path)
    with snaplist_path.open("rb") as fh:
        snaplist = list(fastavro.reader(fh))

    entries: list[FileEntry] = []
    partition_names = [name for name, _, _ in snap.partition_spec]
    for idx, manifest in enumerate(snaplist):
        mpath = workdir / f"manifest_{idx}.avro"
        aws_cp(manifest["manifest_path"], mpath)
        with mpath.open("rb") as fh:
            for rec in fastavro.reader(fh):
                df = rec["data_file"]
                partition = df.get("partition") or {}
                key = tuple(partition.get(name) for name in partition_names)
                entries.append(
                    FileEntry(
                        partition=key,
                        path=df["file_path"],
                        record_count=df.get("record_count", 0),
                        status=rec.get("status", 1),
                        content=df.get("content", 0),
                    )
                )
    return entries


def live_files(entries: list[FileEntry]) -> dict[str, FileEntry]:
    # Manifest entries with status=2 mean "deleted in the snapshot that wrote
    # this manifest"; status 0 (existing) and 1 (added) are live in the
    # current snapshot.
    return {e.path: e for e in entries if e.status != 2}


def smallest_partition(entries: list[FileEntry]) -> tuple[tuple, int, int]:
    per_partition: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
    for entry in entries:
        if entry.status == 2 or entry.content != 0:
            continue
        per_partition[entry.partition][0] += entry.record_count
        per_partition[entry.partition][1] += 1
    key = min(per_partition, key=lambda k: per_partition[k][0])
    return key, per_partition[key][0], per_partition[key][1]


def day_value_to_iso(value) -> str:
    # Iceberg's day() partition transform stores days since 1970-01-01.
    # fastavro decodes with logical types enabled, so we normally get a
    # datetime.date here, but accept an int as a defensive fallback.
    if isinstance(value, date):
        return value.isoformat()
    return (date(1970, 1, 1) + timedelta(days=int(value))).isoformat()


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sample_rows(
    client,
    function_name: str,
    token: str,
    target_day_iso: str,
    target_event: str,
    n: int,
) -> list[list]:
    sql = f"""
    SELECT event_id, collector_tstamp, event_name
    FROM {EMBUCKET_FQN}
    WHERE CAST(collector_tstamp AS DATE) = '{target_day_iso}'
      AND event_name = {sql_string_literal(target_event)}
    LIMIT {n}
    """.strip()
    result = run_sql(client, function_name, token, sql)
    return result["data"]["rowset"]


def fetch_etl_tags(
    client, function_name: str, token: str, event_ids: list[str]
) -> list[list]:
    id_list = ", ".join(sql_string_literal(i) for i in event_ids)
    sql = f"SELECT event_id, etl_tags FROM {EMBUCKET_FQN} WHERE event_id IN ({id_list})"
    return run_sql(client, function_name, token, sql)["data"]["rowset"]


def build_merge_sql(rows: list[list], probe_tag: str) -> str:
    selects = []
    for r in rows:
        event_id, collector_tstamp, event_name = r[0], r[1], r[2]
        selects.append(
            "SELECT "
            f"{sql_string_literal(str(event_id))} AS event_id, "
            f"TO_TIMESTAMP_TZ({sql_string_literal(str(collector_tstamp))}) AS collector_tstamp, "
            f"{sql_string_literal(str(event_name))} AS event_name"
        )
    source = "\n    UNION ALL ".join(selects)
    return f"""
MERGE INTO {EMBUCKET_FQN} AS t
USING (
    {source}
) AS s
ON  t.event_id = s.event_id
AND t.collector_tstamp = s.collector_tstamp
AND t.event_name = s.event_name
WHEN MATCHED THEN UPDATE SET etl_tags = {sql_string_literal(probe_tag)}
""".strip()


def run_statement(client, function_name: str, token: str, sql: str) -> tuple[str | None, str | None]:
    try:
        result = run_sql(client, function_name, token, sql)
    except Exception as exc:  # noqa: BLE001 - we want to capture any SQL error
        return None, str(exc)
    rowset = result.get("data", {}).get("rowset", [])
    if not rowset:
        return "", None
    lines = []
    for row in rowset:
        if len(row) == 1:
            lines.append("" if row[0] is None else str(row[0]))
        else:
            lines.append(" | ".join("" if c is None else str(c) for c in row))
    return "\n".join(lines), None


def validate_pruning(
    baseline_entries: list[FileEntry],
    new_entries: list[FileEntry],
    target: tuple,
) -> dict:
    before = live_files(baseline_entries)
    after = live_files(new_entries)
    added_paths = set(after) - set(before)
    removed_paths = set(before) - set(after)

    added = [after[p] for p in added_paths]
    removed = [before[p] for p in removed_paths]
    touched = added + removed
    wrong = [e for e in touched if e.partition != target]

    return {
        "added_files": len(added),
        "removed_files": len(removed),
        "touched_in_target": sum(1 for e in touched if e.partition == target),
        "touched_outside_target": len(wrong),
        "added_detail": [
            {
                "partition": list(e.partition),
                "path": e.path,
                "record_count": e.record_count,
                "content": e.content,
            }
            for e in added[:20]
        ],
        "removed_detail": [
            {
                "partition": list(e.partition),
                "path": e.path,
                "record_count": e.record_count,
                "content": e.content,
            }
            for e in removed[:20]
        ],
        "outside_target": [
            {"partition": list(e.partition), "path": e.path, "content": e.content}
            for e in wrong[:20]
        ],
        "pass": len(wrong) == 0 and len(touched) > 0,
    }


def wait_for_new_snapshot(baseline_id: int, attempts: int = 6, delay: float = 2.0) -> dict:
    for _ in range(attempts):
        table = s3tables_get_table()
        meta_path = Path("/tmp/hooli_wait_meta.json")
        aws_cp(table["metadataLocation"], meta_path)
        meta = json.loads(meta_path.read_text())
        if meta["current-snapshot-id"] != baseline_id:
            return table
        time.sleep(delay)
    return table  # return the latest we saw even if unchanged


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lambda_function", help="Embucket Lambda ARN or function name")
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run the real MERGE (via EXPLAIN ANALYZE, with plain MERGE as fallback). "
        "Without this flag, only EXPLAIN is issued and no rows are mutated.",
    )
    parser.add_argument("--out-dir", default="tmp/merge_partition_probe")
    args = parser.parse_args()

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = Path(args.out_dir) / ts
    out.mkdir(parents=True, exist_ok=True)
    plans_path = out / "plans.txt"

    print(f"[1/6] Reading baseline Iceberg metadata for {EMBUCKET_FQN}...")
    table = s3tables_get_table()
    baseline = load_snapshot(table["metadataLocation"], out / "baseline")
    print(
        f"      snapshot_id={baseline.snapshot_id} "
        f"total_records={baseline.summary.get('total-records')} "
        f"total_data_files={baseline.summary.get('total-data-files')}"
    )
    print(f"      partition spec: {baseline.partition_spec}")

    print("[2/6] Reading manifests, picking smallest partition...")
    baseline_entries = read_manifests(baseline, out / "baseline")
    target, target_rows, target_files = smallest_partition(baseline_entries)
    target_day_iso = day_value_to_iso(target[0])
    target_event = target[1]
    print(
        f"      target: day={target_day_iso} event_name={target_event!r} "
        f"rows={target_rows:,} files={target_files}"
    )

    print(f"[3/6] Logging in to Embucket and sampling {args.sample_size} rows...")
    client = lambda_client(args.lambda_function)
    token = login(client, args.lambda_function)
    rows = sample_rows(
        client, args.lambda_function, token, target_day_iso, target_event, args.sample_size
    )
    if not rows:
        print("      ERROR: sample query returned 0 rows — check Embucket snapshot state")
        return 2
    if len(rows) < args.sample_size:
        print(
            f"      WARN: asked for {args.sample_size}, got {len(rows)} "
            f"(Embucket may be on a stale snapshot)"
        )
    event_ids = [str(r[0]) for r in rows]
    (out / "sampled_rows.json").write_text(
        json.dumps(
            [
                {"event_id": str(r[0]), "collector_tstamp": str(r[1]), "event_name": str(r[2])}
                for r in rows
            ],
            indent=2,
        )
    )
    print(f"      sampled event_ids written to {out / 'sampled_rows.json'}")

    original_tags = fetch_etl_tags(client, args.lambda_function, token, event_ids)
    (out / "original_etl_tags.json").write_text(
        json.dumps(
            [{"event_id": str(r[0]), "etl_tags": None if r[1] is None else str(r[1])} for r in original_tags],
            indent=2,
        )
    )

    probe_tag = f"merge_probe_{ts}"
    merge_sql = build_merge_sql(rows, probe_tag)
    (out / "merge.sql").write_text(merge_sql + "\n")

    print("[4/6] EXPLAIN MERGE...")
    explain_output, explain_err = run_statement(
        client, args.lambda_function, token, f"EXPLAIN {merge_sql}"
    )
    with plans_path.open("w") as fh:
        fh.write("===== EXPLAIN MERGE =====\n")
        fh.write(explain_output if explain_output is not None else f"ERROR: {explain_err}")
        fh.write("\n")
    print(f"      wrote {plans_path}")

    if not args.execute:
        print("[5/6] SKIP execution (no --execute flag). EXPLAIN-only probe complete.")
        print(f"[6/6] No validation without execution. Outputs in {out}")
        return 0

    print("[5/6] EXPLAIN ANALYZE MERGE (real write)...")
    analyze_output, analyze_err = run_statement(
        client, args.lambda_function, token, f"EXPLAIN ANALYZE {merge_sql}"
    )
    with plans_path.open("a") as fh:
        fh.write("\n===== EXPLAIN ANALYZE MERGE =====\n")
        fh.write(
            analyze_output if analyze_output is not None else f"ERROR: {analyze_err}"
        )
        fh.write("\n")

    if analyze_err:
        print(f"      EXPLAIN ANALYZE failed: {analyze_err[:200]}")
        print("      Falling back to plain MERGE + EXPLAIN ANALYZE SELECT...")
        run_sql(client, args.lambda_function, token, merge_sql)
        read_probe = (
            f"EXPLAIN ANALYZE SELECT COUNT(*) FROM {EMBUCKET_FQN} "
            f"WHERE CAST(collector_tstamp AS DATE) = '{target_day_iso}' "
            f"AND event_name = {sql_string_literal(target_event)}"
        )
        read_output, read_err = run_statement(client, args.lambda_function, token, read_probe)
        with plans_path.open("a") as fh:
            fh.write("\n===== EXPLAIN ANALYZE SELECT (read-side probe) =====\n")
            fh.write(read_output if read_output is not None else f"ERROR: {read_err}")
            fh.write("\n")
    else:
        print("      EXPLAIN ANALYZE MERGE succeeded")

    print("[6/6] Re-reading Iceberg metadata, validating partition pruning...")
    new_table = wait_for_new_snapshot(baseline.snapshot_id)
    if new_table["metadataLocation"] == baseline.metadata_location:
        print("      WARN: metadata location unchanged — Iceberg commit may not have landed")

    new_snap = load_snapshot(new_table["metadataLocation"], out / "after")
    new_entries = read_manifests(new_snap, out / "after")

    report: dict = {
        "baseline_snapshot_id": baseline.snapshot_id,
        "new_snapshot_id": new_snap.snapshot_id,
        "new_snapshot_summary": new_snap.summary,
        "target_partition": {"day": target_day_iso, "event_name": target_event},
        "probe_tag": probe_tag,
        "sample_event_ids": event_ids,
    }
    report.update(validate_pruning(baseline_entries, new_entries, target))

    (out / "report.json").write_text(json.dumps(report, indent=2, default=str))

    verdict = "PASS" if report["pass"] else "FAIL"
    print()
    print(f"===== {verdict} =====")
    print(f"  baseline snapshot: {baseline.snapshot_id}")
    print(f"  new snapshot:      {new_snap.snapshot_id}")
    print(f"  snapshot summary:  operation={new_snap.summary.get('operation')} "
          f"changed-partition-count={new_snap.summary.get('changed-partition-count')} "
          f"added-data-files={new_snap.summary.get('added-data-files')} "
          f"added-records={new_snap.summary.get('added-records')}")
    print(f"  files added:       {report['added_files']}")
    print(f"  files removed:     {report['removed_files']}")
    print(f"  touched in target: {report['touched_in_target']}")
    print(f"  touched outside:   {report['touched_outside_target']}")
    print(f"  outputs:           {out}")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
