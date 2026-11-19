"""Console entry point. Validation and file writes do not start a cluster."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from lakeflow_framework.config import ConfigValidationError, validate_configs
from lakeflow_framework.pipeline import MetadataNotFound, assemble


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakeflow-framework")
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate")
    validate.add_argument("paths", nargs="+", type=Path)
    validate.add_argument("--catalog", help="catalog for specs that do not name one")

    new_source = commands.add_parser("new-source")
    new_source.add_argument("bundle", type=Path)
    new_source.add_argument("domain")
    new_source.add_argument("source_system")
    new_source.add_argument("--owner", required=True, help="owner email address")
    new_source.add_argument("--kind", default="sftp_files")
    new_source.add_argument("--delivery", default="daily")

    new_table = commands.add_parser("new-table")
    new_table.add_argument("bundle", type=Path)
    new_table.add_argument("domain")
    new_table.add_argument("source_system")
    new_table.add_argument("table")
    new_table.add_argument(
        "--requested-by",
        help="project bringing the table in; omit to put the spec next to source.yaml",
    )
    new_table.add_argument("--delivery", help="needed when the source has several deliveries")

    generate = commands.add_parser("generate")
    generate.add_argument("bundle", type=Path, help="ingestion bundle folder holding sources/")
    generate.add_argument(
        "--check", action="store_true", help="change nothing; fail if generated files are stale"
    )

    projects = commands.add_parser("projects")
    projects.add_argument("action", choices=["generate", "new"])
    projects.add_argument("root", type=Path, help="the curation folder, framework_curation")
    projects.add_argument("--sources", type=Path, help="the ingestion bundle folder")
    projects.add_argument("--check", action="store_true", help="change nothing; fail if stale")
    projects.add_argument("--domain")
    projects.add_argument("--project")
    projects.add_argument("--owner", help="owner email address")

    unity = commands.add_parser(
        "apply-metadata", help="set each spec's tags and column comments in Unity Catalog"
    )
    unity.add_argument("paths", nargs="+", type=Path, help="the spec files the job loads")
    unity.add_argument("--catalog", required=True)

    batch = commands.add_parser("batch-sql")
    batch.add_argument("paths", nargs="+", type=Path)
    batch.add_argument("--pipeline", required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "projects":
            from lakeflow_framework import projects as project_files

            if args.action == "new":
                if not (args.domain and args.project and args.owner):
                    print("projects new needs --domain, --project, and --owner", file=sys.stderr)
                    return 1
                created = project_files.new_project(
                    args.root, args.domain, args.project, owner=args.owner
                )
                print(f"created {created}")
                return 0
            if args.sources is None:
                print("projects generate needs --sources", file=sys.stderr)
                return 1
            stale = project_files.generate(args.root, args.sources, check=args.check)
            if args.check and stale:
                print("out of date: " + ", ".join(stale), file=sys.stderr)
                return 1
            print(f"updated {', '.join(stale)}" if stale else "project maps are up to date")
            return 0
        if args.command in {"new-source", "new-table"}:
            from lakeflow_framework import ingestion

            if args.command == "new-source":
                created = ingestion.new_source(
                    args.bundle,
                    args.domain,
                    args.source_system,
                    owner=args.owner,
                    kind=args.kind,
                    delivery=args.delivery,
                )
            else:
                created = ingestion.new_table(
                    args.bundle,
                    args.domain,
                    args.source_system,
                    args.table,
                    requested_by=args.requested_by,
                    delivery=args.delivery,
                )
            print(f"created {created}")
            print("replace every TODO, then run ./run.sh generate")
            return 0
        if args.command == "validate":
            configs = validate_configs(_yaml_files(args.paths), catalog=args.catalog)
            print(f"validated {len(configs)} pipeline(s)")
            return 0
        if args.command == "generate":
            from lakeflow_framework.ingestion import generate as generate_files
            from lakeflow_framework.sources import description_warnings, load_sources

            stale = generate_files(args.bundle, check=args.check)
            for warning in description_warnings(load_sources(args.bundle)):
                print(f"warning: {warning}", file=sys.stderr)
            if args.check and stale:
                print("out of date: " + ", ".join(stale), file=sys.stderr)
                print(f"run `lakeflow-framework generate {args.bundle}`", file=sys.stderr)
                return 1
            print(f"updated {', '.join(stale)}" if stale else "generated files are up to date")
            return 0
        if args.command == "apply-metadata":
            from pyspark.sql import SparkSession

            from lakeflow_framework.metadata import definition_rows
            from lakeflow_framework.unity import apply_metadata

            configs = validate_configs(_yaml_files(args.paths), catalog=args.catalog)
            rows = definition_rows(configs)
            applied = apply_metadata(SparkSession.builder.getOrCreate(), rows, args.catalog)
            print(f"applied {len(applied)} metadata change(s)")
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
    except StopIteration:
        print(f"pipeline {args.pipeline} was not in the validated files", file=sys.stderr)
        return 1


def _yaml_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            found = [*sorted(path.glob("*.yaml")), *sorted(path.glob("*.yml"))]
            files.extend(item for item in found if item.name != "source.yaml")
        else:
            files.append(path)
    return files


if __name__ == "__main__":
    raise SystemExit(main())
