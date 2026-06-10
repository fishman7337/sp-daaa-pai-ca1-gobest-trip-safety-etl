"""Tests for the package CLI."""

from pai_pipeline.cli import main


def test_cli_lists_available_pipelines(capsys) -> None:
    assert main(["list"]) == 0

    output = capsys.readouterr().out
    assert "sensor:" in output
    assert "driver:" in output
    assert "safety:" in output


def test_cli_dry_run_all_without_database(capsys) -> None:
    assert main(["run", "all", "--dry-run"]) == 0

    output = capsys.readouterr().out
    assert "sensor_cleaning.py" in output
    assert "driver_cleaning.py" in output
    assert "safety_cleaning.py" in output
