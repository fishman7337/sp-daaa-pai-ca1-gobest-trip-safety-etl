"""Tests for SQL helper functions."""

from pai_pipeline.sql import nullify_sql, quote_identifier


def test_quote_identifier_escapes_backticks() -> None:
    assert quote_identifier("driver`id") == "`driver``id`"


def test_nullify_sql_wraps_common_missing_tokens() -> None:
    expression = nullify_sql("label")

    assert "LOWER(TRIM(`label`))" in expression
    for token in ("''", "'na'", "'n/a'", "'null'", "'nan'", "'.'"):
        assert token in expression
