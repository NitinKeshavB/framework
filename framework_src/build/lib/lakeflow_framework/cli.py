"""Console entry point. Validation and file writes do not start a cluster."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from lakeflow_framework.config import ConfigValidationError, validate_configs
from lakeflow_framework.metadata import (
    new_object_rows,
    object_rows,
    step_payload,
    write_definition_files,
    write_step_file,
)
from lakeflow_framework.pipeline import MetadataNotFound, assemble


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakeflow-framework")
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate")
    validate.add_argument("paths", nargs="+", type=Path)

    sync = commands.add_parser("sync")
    sync.add_argument("paths", nargs="+", type=Path)
    sync.add_argument("--out", type=Path, required=True)

    step = commands.add_parser("step")
    step.add_argument("--project", required=True)
    step.add_argument("--full-name", required=True)
    step.add_argument("--run-id", required=True)
    step.add_argument("--step-no", required=True, type=int)
    step.add_argument("--step-name", required=True)
    step.add_argument("--status", required=True)
    step.add_argument("--out", type=Path, required=True)
    step.add_argument("--failure-reason")
    step.add_argument("--parent-run-id")
    step.add_argument("--recon-json")

    waiting = commands.add_parser("await-registry")
    waiting.add_argument("--catalog", required=True)
    waiting.add_argument("--pipeline", action="append", required=True, dest="pipelines")
    waiting.add_argument("--timeout-seconds", type=float, default=300)

    batch = commands.add_parser("batch-sql")
    batch.add_argument("paths", nargs="+", type=Path)
    batch.add_argument("--pipeline", required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            configs = validate_configs(_yaml_files(args.paths))
            print(f"validated {len(configs)} pipeline(s)")
            return 0
        if args.command == "sync":
            if args.out.exists() and not args.out.is_dir():
                print("sync --out must be a directory", file=sys.stderr)
                return 1
            configs = validate_configs(_yaml_files(args.paths))
            rows: list[dict] = []
            seen: set[tuple[str, str, str]] = set()
            for config in configs:
                rows.extend(new_object_rows(object_rows(config), seen))
            written = write_definition_files(args.out, rows)
            print(f"wrote {len(written)} definition file(s) to {args.out}")
            return 0
        if args.command == "await-registry":
            from pyspark.sql import SparkSession

            from lakeflow_framework.collector import await_registered

            spark = SparkSession.builder.getOrCreate()
            await_registered(
                spark, args.pipelines, args.catalog, timeout_seconds=args.timeout_seconds
            )
            print(f"registry has {', '.join(args.pipelines)}")
            return 0
        if args.command == "step":
            recon = json.loads(args.recon_json) if args.recon_json else None
            if recon is not None and not isinstance(recon, dict):
                print("recon-json must be a JSON object", file=sys.stderr)
                return 1
            payload = step_payload(
                project=args.project,
                full_name=args.full_name,
                run_id=args.run_id,
                step_no=args.step_no,
                step_name=args.step_name,
                status=args.status,
                failure_reason=args.failure_reason,
                parent_run_id=args.parent_run_id,
                recon=recon,
            )
            path = write_step_file(args.out, payload)
            print(path)
            return 0
        configs = validate_configs(_yaml_files(args.paths))
        selected = next(config for config in configs if config.pipeline_id == args.pipeline)
        statements = assemble(selected).batch_sql
        print("\n;\n".join(statements))
        return 0
    except ConfigValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except MetadataNotFound as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except json.JSONDecodeError:
        print("recon-json must be a JSON object", file=sys.stderr)
        return 1
    except StopIteration:
        print(f"pipeline {args.pipeline} was not in the validated files", file=sys.stderr)
        return 1


def _yaml_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(path.glob("*.yaml")))
            files.extend(sorted(path.glob("*.yml")))
        else:
            files.append(path)
    return files


if __name__ == "__main__":
    raise SystemExit(main())
