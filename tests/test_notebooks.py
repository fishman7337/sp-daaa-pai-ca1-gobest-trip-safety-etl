"""Notebook integrity tests for the checked-in coursework artifacts."""

from pathlib import Path

import nbformat
import pytest

NOTEBOOK_DIR = Path(__file__).resolve().parents[1] / "notebooks"
NOTEBOOK_PATHS = tuple(sorted(NOTEBOOK_DIR.glob("*.ipynb")))


@pytest.mark.parametrize("notebook_path", NOTEBOOK_PATHS, ids=lambda path: path.name)
def test_notebook_matches_nbformat_schema(notebook_path: Path) -> None:
    notebook = nbformat.read(notebook_path, as_version=nbformat.NO_CONVERT)

    nbformat.validate(notebook)
