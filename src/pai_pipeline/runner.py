"""Subprocess runner for the ETL scripts exported from notebooks."""

from __future__ import annotations

# Subprocess execution is required for repository-owned ETL scripts.
import subprocess  # nosec B404
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from pai_pipeline.config import DatabaseConfig, project_root
from pai_pipeline.db import count_uncleaned_rows


@dataclass(frozen=True)
class PipelineSpec:
    """Metadata needed to run one ETL script."""

    name: str
    source_table: str
    script_path: Path


def pipeline_specs() -> dict[str, PipelineSpec]:
    """Return the available ETL jobs keyed by CLI name."""
    root = project_root()
    pipeline_dir = root / "src" / "pai_pipeline" / "pipelines"
    return {
        "sensor": PipelineSpec(
            name="sensor",
            source_table="sensor_data_source",
            script_path=pipeline_dir / "sensor_cleaning.py",
        ),
        "driver": PipelineSpec(
            name="driver",
            source_table="drivers_source",
            script_path=pipeline_dir / "driver_cleaning.py",
        ),
        "safety": PipelineSpec(
            name="safety",
            source_table="safety_labels_source",
            script_path=pipeline_dir / "safety_cleaning.py",
        ),
    }


def selected_specs(selection: str) -> list[PipelineSpec]:
    """Resolve a single pipeline name or ``all`` into ordered specs."""
    specs = pipeline_specs()
    if selection == "all":
        return [specs["sensor"], specs["driver"], specs["safety"]]
    if selection not in specs:
        allowed = ", ".join(["all", *specs])
        raise ValueError(f"Unknown pipeline {selection!r}. Expected one of: {allowed}")
    return [specs[selection]]


def run_pipeline(
    spec: PipelineSpec,
    *,
    python_executable: str = sys.executable,
    dry_run: bool = False,
) -> int:
    """Run one pipeline script and return its process exit code."""
    command = [python_executable, str(spec.script_path)]
    if dry_run:
        print("DRY RUN:", " ".join(command))
        return 0
    # Both the interpreter and script path are supplied by trusted application code.
    completed = subprocess.run(  # nosec B603
        command,
        cwd=project_root(),
        check=False,
        shell=False,
    )
    return completed.returncode


def run_selected(
    specs: Iterable[PipelineSpec],
    *,
    staging_config: DatabaseConfig | None = None,
    skip_counts: bool = False,
    dry_run: bool = False,
) -> int:
    """Run selected pipeline scripts, optionally skipping empty source tables."""
    for spec in specs:
        if staging_config and not skip_counts:
            pending_rows = count_uncleaned_rows(staging_config, spec.source_table)
            print(f"{spec.name}: {pending_rows} rows pending in {spec.source_table}")
            if pending_rows == 0:
                continue
        exit_code = run_pipeline(spec, dry_run=dry_run)
        if exit_code != 0:
            return exit_code
    return 0
