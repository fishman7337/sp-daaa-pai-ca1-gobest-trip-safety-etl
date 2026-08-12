"""Tests for SQL helper functions."""

import pytest

from pai_pipeline.sql import (
    nullify_sql,
    quote_identifier,
    quote_qualified_identifier,
    sql_statement,
)


def test_sql_statement_joins_nonempty_fragments() -> None:
    assert sql_statement(" SELECT ", "*", "", "FROM", "`sensor_data`;") == (
        "SELECT * FROM `sensor_data`;"
    )


def test_quote_identifier_escapes_backticks() -> None:
    assert quote_identifier("driver`id") == "`driver``id`"


def test_quote_qualified_identifier_quotes_every_component() -> None:
    assert quote_qualified_identifier("warehouse", "Dim`Sensor") == (
        "`warehouse`.`Dim``Sensor`"
    )


def test_quote_qualified_identifier_requires_a_component() -> None:
    with pytest.raises(ValueError, match="At least one identifier"):
        quote_qualified_identifier()


def test_nullify_sql_wraps_common_missing_tokens() -> None:
    expression = nullify_sql("label")

    assert "LOWER(TRIM(`label`))" in expression
    for token in ("''", "'na'", "'n/a'", "'null'", "'nan'", "'.'"):
        assert token in expression
