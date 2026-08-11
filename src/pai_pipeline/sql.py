"""Small SQL-building helpers shared by pipeline code and tests."""

from __future__ import annotations

NULL_TOKENS = ("", "na", "n/a", "null", "nan", ".")


def quote_identifier(identifier: str) -> str:
    """Quote a MySQL identifier after escaping embedded backticks."""
    return f"`{identifier.replace('`', '``')}`"


def nullify_sql(column: str) -> str:
    """Return a MySQL expression that coerces common missing tokens to NULL."""
    base = f"LOWER(TRIM({quote_identifier(column)}))"
    expression = base
    for token in NULL_TOKENS:
        expression = f"NULLIF({expression}, '{token}')"
    return expression
