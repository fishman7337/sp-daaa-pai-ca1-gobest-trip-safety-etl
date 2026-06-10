"""Unit tests for environment and path configuration."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pai_pipeline.config import (
    DatabaseConfig,
    int_env,
    load_env_file,
    missing_vars,
    mysql_jdbc_jar_path,
)


def test_missing_vars_reports_blank_and_absent_values() -> None:
    env = {"PRESENT": "value", "BLANK": ""}

    assert missing_vars(["PRESENT", "BLANK", "ABSENT"], env) == ["BLANK", "ABSENT"]


def test_int_env_parses_values_and_raises_helpfully() -> None:
    assert int_env("PORT", env={"PORT": "3306"}) == 3306
    assert int_env("PORT", default=3306, env={}) == 3306

    with pytest.raises(ValueError, match="PORT must be an integer"):
        int_env("PORT", env={"PORT": "not-a-port"})


def test_load_env_file_does_not_override_existing_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "EXISTING=from_file\n"
        "NEW_VALUE='hello world'\n"
        "export EXPORTED=yes\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXISTING", "from_os")

    loaded = load_env_file(env_file)

    assert loaded == env_file
    assert Path(loaded).is_file()
    assert loaded.read_text(encoding="utf-8")
    assert os.environ["EXISTING"] == "from_os"
    assert os.environ["NEW_VALUE"] == "hello world"
    assert os.environ["EXPORTED"] == "yes"


def test_database_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STAGING_DB_HOST", "localhost")
    monkeypatch.setenv("STAGING_DB_PORT", "3306")
    monkeypatch.setenv("STAGING_DB_NAME", "staging")
    monkeypatch.setenv("STAGING_DB_USER", "tester")
    monkeypatch.setenv("STAGING_DB_PASSWORD", "secret")

    config = DatabaseConfig.from_env("STAGING", "staging")

    assert config.host == "localhost"
    assert config.port == 3306
    assert config.database == "staging"
    assert config.user == "tester"
    assert "secret" not in repr(config)
    assert config.jdbc_url() == "jdbc:mysql://localhost:3306/staging"


def test_mysql_jdbc_jar_path_defaults_to_vendor() -> None:
    assert mysql_jdbc_jar_path().as_posix().endswith(
        "vendor/mysql-connector-j-8.0.33.jar"
    )
