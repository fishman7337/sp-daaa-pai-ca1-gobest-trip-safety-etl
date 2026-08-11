"""Command-line interface for local ETL operations."""

from __future__ import annotations

import argparse

from pai_pipeline.config import (
    REQUIRED_PIPELINE_VARS,
    REQUIRED_STAGING_VARS,
    DatabaseConfig,
    load_env_file,
    missing_vars,
    mysql_jdbc_jar_path,
)
from pai_pipeline.runner import run_selected, selected_specs


def build_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser."""
    parser = argparse.ArgumentParser(prog="pai-pipeline")
    parser.add_argument(
        "--env-file",
        help="Optional env file to load before running commands.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check-config", help="Validate local settings.")
    check.add_argument(
        "--strict",
        action="store_true",
        help="Also require all live DB variables needed for ETL execution.",
    )

    run = subparsers.add_parser("run", help="Run one ETL job or all jobs.")
    run.add_argument("pipeline", choices=["all", "sensor", "driver", "safety"])
    run.add_argument(
        "--skip-counts",
        action="store_true",
        help="Run scripts without first querying source table row counts.",
    )
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the scripts that would run without executing them.",
    )

    subparsers.add_parser("list", help="List available ETL jobs.")
    return parser


def _check_config(strict: bool) -> int:
    print(f"MySQL JDBC jar: {mysql_jdbc_jar_path()}")
    missing = missing_vars(REQUIRED_PIPELINE_VARS)
    if strict:
        missing = sorted(set(missing + missing_vars(REQUIRED_STAGING_VARS)))
    if missing:
        print("Missing environment variables:")
        for name in missing:
            print(f"  - {name}")
        return 1 if strict else 0
    print("Configuration looks ready.")
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)
    loaded_path = load_env_file(args.env_file)
    if loaded_path:
        print(f"Loaded env file: {loaded_path}")

    if args.command == "check-config":
        return _check_config(args.strict)
    if args.command == "list":
        for spec in selected_specs("all"):
            print(f"{spec.name}: {spec.script_path}")
        return 0
    if args.command == "run":
        missing = missing_vars(REQUIRED_STAGING_VARS)
        staging_config = None
        if not missing:
            staging_config = DatabaseConfig.from_env("STAGING", "staging")
        elif not args.skip_counts and not args.dry_run:
            parser.error(
                "live DB count checks require staging DB env vars; "
                "use --skip-counts or --dry-run for offline use"
            )
        return run_selected(
            selected_specs(args.pipeline),
            staging_config=staging_config,
            skip_counts=args.skip_counts,
            dry_run=args.dry_run,
        )
    parser.error("Unknown command")
    return 2
