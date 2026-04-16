#!/usr/bin/env python3
"""Load generated Snowplow parquet batches and time dbt incremental runs."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

from embucket_client import ensure_snowplow_schemas, lambda_client, login, run_sql

DEFAULT_RESULTS_PATH = "tmp/incremental_benchmark_results.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lambda_function", help="Lambda ARN or function name")
    parser.add_argument("--manifest-path", required=True, help="Manifest JSON from generate_snowplow_batches.py")
    parser.add_argument("--profiles-dir", default=".", help="dbt profiles directory")
    parser.add_argument("--initial-batches", type=int, default=1, help="Batches to load before the first dbt run")
    parser.add_argument("--batch-limit", type=int, help="Only use the first N batches from the manifest")
    parser.add_argument("--results-path", default=DEFAULT_RESULTS_PATH, help="Output JSON path")
    parser.add_argument("--select", help="Optional dbt select filter")
    parser.add_argument("--skip-seed", action="store_true", help="Skip dbt seed before the first run")
    parser.add_argument("--skip-reset", action="store_true", help="Do not reset source and target objects first")
    return parser.parse_args()


def load_manifest(path: str) -> dict:
    return json.loads(Path(path).expanduser().read_text())


def copy_into_events(client, function_name: str, token: str, parquet_uri: str) -> None:
    sql = f"""COPY INTO demo.atomic.events
FROM '{parquet_uri}'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;"""
    run_sql(client, function_name, token, sql)


def reset_environment(client, function_name: str, token: str, create_table_sql: str) -> None:
    statements = [
        "DROP TABLE IF EXISTS demo.atomic.events",
        "DROP SCHEMA IF EXISTS demo.atomic_scratch CASCADE",
        "DROP SCHEMA IF EXISTS demo.atomic_derived CASCADE",
        "DROP SCHEMA IF EXISTS demo.atomic_snowplow_manifest CASCADE",
    ]
    for sql in statements:
        run_sql(client, function_name, token, sql)
    ensure_snowplow_schemas(client, function_name, token)
    run_sql(client, function_name, token, create_table_sql)


def source_metrics(client, function_name: str, token: str) -> dict:
    result = run_sql(
        client,
        function_name,
        token,
        """
        SELECT
          COUNT(*) AS row_count,
          MIN(collector_tstamp) AS min_collector_tstamp,
          MAX(collector_tstamp) AS max_collector_tstamp
        FROM demo.atomic.events
        """.strip(),
    )
    row_count, min_tstamp, max_tstamp = result["data"]["rowset"][0]
    return {
        "row_count": row_count,
        "min_collector_tstamp": min_tstamp,
        "max_collector_tstamp": max_tstamp,
    }


def run_subprocess(command: list[str], cwd: Path) -> float:
    start = time.perf_counter()
    subprocess.run(command, cwd=cwd, check=True)
    return time.perf_counter() - start


def dbt_run_command(profiles_dir: str, select: str | None) -> list[str]:
    command = ["uv", "run", "dbt", "run", "--profiles-dir", profiles_dir]
    if select:
        command.extend(["--select", select])
    return command


def read_run_results(project_root: Path) -> dict | None:
    run_results = project_root / "target" / "run_results.json"
    if not run_results.exists():
        return None
    return json.loads(run_results.read_text())


def summarize_run_results(payload: dict | None) -> dict | None:
    if payload is None:
        return None
    return {
        "elapsed_time": payload.get("elapsed_time"),
        "results": [
            {
                "model": item.get("unique_id"),
                "status": item.get("status"),
                "execution_time": item.get("execution_time"),
            }
            for item in payload.get("results", [])
        ],
    }


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parent.parent
    manifest = load_manifest(args.manifest_path)
    batches = manifest["batches"][: args.batch_limit] if args.batch_limit else manifest["batches"]
    if not batches:
        raise SystemExit("Manifest does not contain any batches")
    if args.initial_batches < 1 or args.initial_batches > len(batches):
        raise SystemExit("--initial-batches must be between 1 and the number of manifest batches")

    create_table_sql = (project_root / "scripts" / "create_table.sql").read_text().strip()
    client = lambda_client(args.lambda_function)

    print("Logging in...")
    token = login(client, args.lambda_function)
    if args.skip_reset:
        ensure_snowplow_schemas(client, args.lambda_function, token)
        run_sql(client, args.lambda_function, token, create_table_sql)
    else:
        print("Resetting schemas and source table...")
        reset_environment(client, args.lambda_function, token, create_table_sql)

    if not args.skip_seed:
        print("Running dbt seed...")
        run_subprocess(["uv", "run", "dbt", "seed", "--profiles-dir", args.profiles_dir], project_root)

    results = {
        "manifest_path": str(Path(args.manifest_path).expanduser()),
        "initial_batches": args.initial_batches,
        "runs": [],
    }

    initial_group = batches[: args.initial_batches]
    print(f"Loading initial {len(initial_group)} batch(es)...")
    initial_copy_results = []
    for batch in initial_group:
        copy_start = time.perf_counter()
        copy_into_events(client, args.lambda_function, token, batch["uri"])
        copy_seconds = time.perf_counter() - copy_start
        initial_copy_results.append({"batch_uri": batch["uri"], "copy_seconds": copy_seconds})
        print(f"  copied {batch['uri']} in {copy_seconds:.2f}s")

    metrics = source_metrics(client, args.lambda_function, token)
    print("Running initial dbt build...")
    dbt_seconds = run_subprocess(dbt_run_command(args.profiles_dir, args.select), project_root)
    results["runs"].append(
        {
            "phase": "initial",
            "loaded_batches": [batch["uri"] for batch in initial_group],
            "copy_batches": initial_copy_results,
            "copy_seconds_total": sum(item["copy_seconds"] for item in initial_copy_results),
            "source_metrics": metrics,
            "dbt_wall_seconds": dbt_seconds,
            "dbt_run_results": summarize_run_results(read_run_results(project_root)),
        }
    )

    for batch in batches[args.initial_batches :]:
        print(f"Incremental batch {batch['batch_index']}: {batch['uri']}")
        copy_start = time.perf_counter()
        copy_into_events(client, args.lambda_function, token, batch["uri"])
        copy_seconds = time.perf_counter() - copy_start
        metrics = source_metrics(client, args.lambda_function, token)
        dbt_seconds = run_subprocess(dbt_run_command(args.profiles_dir, args.select), project_root)
        results["runs"].append(
            {
                "phase": "incremental",
                "batch_index": batch["batch_index"],
                "batch_uri": batch["uri"],
                "copy_seconds": copy_seconds,
                "source_metrics": metrics,
                "dbt_wall_seconds": dbt_seconds,
                "dbt_run_results": summarize_run_results(read_run_results(project_root)),
            }
        )

    results_path = Path(args.results_path).expanduser()
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.write_text(json.dumps(results, indent=2) + "\n")
    print(f"Wrote benchmark results to {results_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
