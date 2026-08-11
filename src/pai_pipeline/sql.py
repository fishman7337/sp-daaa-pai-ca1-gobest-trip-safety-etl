"""Small SQL-building helpers shared by pipeline code and tests."""

from __future__ import annotations

NULL_TOKENS = ("", "na", "n/a", "null", "nan", ".")


def sql_statement(*parts: str) -> str:
    """Join trusted SQL fragments into one statement.

    This helper only composes SQL syntax. Callers must quote every dynamic
    identifier with :func:`quote_identifier` and continue binding data values
    through the database driver's parameter mechanism.

    Args:
        *parts: Fixed SQL syntax, quoted identifiers, and placeholder groups.

    Returns:
        Non-empty fragments stripped and joined with a single space.
    """
    return " ".join(part.strip() for part in parts if part.strip())


def quote_identifier(identifier: str) -> str:
    """Quote a MySQL identifier after escaping embedded backticks."""
    return f"`{identifier.replace('`', '``')}`"


def quote_qualified_identifier(*identifiers: str) -> str:
    """Quote every component of a qualified MySQL identifier.

    Args:
        *identifiers: Ordered identifier components, such as database and table.

    Returns:
        A dot-separated identifier with every component quoted independently.

    Raises:
        ValueError: If no identifier components are provided.
    """
    if not identifiers:
        raise ValueError("At least one identifier component is required.")
    return ".".join(quote_identifier(identifier) for identifier in identifiers)


def nullify_sql(column: str) -> str:
    """Return a MySQL expression that coerces common missing tokens to NULL."""
    base = f"LOWER(TRIM({quote_identifier(column)}))"
    expression = base
    for token in NULL_TOKENS:
        expression = f"NULLIF({expression}, '{token}')"
    return expression
