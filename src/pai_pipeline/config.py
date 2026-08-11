"""Environment and path configuration for the ETL pipeline.

The original coursework scripts were executed on a prepared EC2 host. This
module keeps that workflow possible while making local and CI runs explicit,
validated, and free of committed credentials.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping

REQUIRED_STAGING_VARS = (
    "STAGING_DB_HOST",
    "STAGING_DB_PORT",
    "STAGING_DB_NAME",
    "STAGING_DB_USER",
    "STAGING_DB_PASSWORD",
)

REQUIRED_PIPELINE_VARS = (
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_STAGING_HOST",
    "STAGING_DB_PORT",
)


def project_root() -> Path:
    """Return the repository root based on the installed source layout."""
    return Path(__file__).resolve().parents[2]


def default_env_paths() -> tuple[Path, ...]:
    """Return env file locations in priority order."""
    root = project_root()
    candidates = []
    explicit = os.getenv("PAI_ENV_FILE")
    if explicit:
        candidates.append(Path(explicit).expanduser())
    candidates.extend(
        [
            root / ".env",
            root / "config" / ".env",
            root / "config" / ".env.local",
            Path.home() / ".pai_env",
        ]
    )
    return tuple(candidates)


def load_env_file(path: str | os.PathLike[str] | None = None) -> Path | None:
    """Load a shell-style env file into ``os.environ`` without overwriting.

    Args:
        path: Optional explicit env file path. When omitted, common project and
            user locations are checked.

    Returns:
        The path loaded, or ``None`` when no env file was found.
    """
    paths = [Path(path).expanduser()] if path else list(default_env_paths())
    for candidate in paths:
        if not candidate.is_file():
            continue
        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key.startswith("export "):
                key = key[len("export ") :].strip()
            value = _strip_inline_comment(value.strip()).strip('"').strip("'")
            os.environ.setdefault(key, os.path.expandvars(value))
        return candidate
    return None


def _strip_inline_comment(value: str) -> str:
    """Remove unquoted inline comments from simple env values."""
    in_single = False
    in_double = False
    for index, char in enumerate(value):
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "#" and not in_single and not in_double:
            return value[:index].rstrip()
    return value


def missing_vars(
    names: Iterable[str], env: Mapping[str, str] | None = None
) -> list[str]:
    """Return required variable names that are absent or blank."""
    source = env if env is not None else os.environ
    return [name for name in names if not source.get(name)]


def int_env(
    name: str, default: int | None = None, env: Mapping[str, str] | None = None
) -> int:
    """Read an integer environment variable with a clear error message."""
    source = env if env is not None else os.environ
    raw_value = source.get(name)
    if raw_value in (None, ""):
        if default is not None:
            return default
        raise ValueError(f"Missing required integer environment variable: {name}")
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw_value!r}") from exc


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection settings for a MySQL-compatible database."""

    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)

    @classmethod
    def from_env(cls, prefix: str, default_database: str) -> "DatabaseConfig":
        """Build settings from ``<PREFIX>_DB_*`` style environment variables."""
        return cls(
            host=os.environ[f"{prefix}_DB_HOST"],
            port=int_env(f"{prefix}_DB_PORT"),
            database=os.getenv(f"{prefix}_DB_NAME", default_database),
            user=os.environ[f"{prefix}_DB_USER"],
            password=os.environ[f"{prefix}_DB_PASSWORD"],
        )

    def jdbc_url(self) -> str:
        """Return a Spark JDBC URL for this database."""
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"


def mysql_jdbc_jar_path() -> Path:
    """Return the MySQL Connector/J path used by Spark JDBC jobs."""
    explicit = os.getenv("PAI_MYSQL_JDBC_JAR")
    if explicit:
        return Path(explicit).expanduser()
    return project_root() / "vendor" / "mysql-connector-j-8.0.33.jar"
