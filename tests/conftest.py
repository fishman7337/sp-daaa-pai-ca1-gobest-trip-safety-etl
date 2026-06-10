"""Shared pytest setup for offline and integration tests."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pai_pipeline.config import load_env_file  # noqa: E402


# Load private env values only when the user explicitly provides a local env
# file. CI and normal unit tests remain offline by default.
_ENV_LOADED = load_env_file()
